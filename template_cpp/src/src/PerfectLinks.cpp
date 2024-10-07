#include "PerfectLinks.hpp"
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

PerfectLinks::PerfectLinks(int sockfd, const struct sockaddr_in &myAddr, std::ofstream& logFile, int myProcessId)
    : sockfd(sockfd), myAddr(myAddr), logFile(logFile), myProcessId(myProcessId) {}

void PerfectLinks::startSending(const sockaddr_in &destAddr, int messageCount, int destProcessId) {
    // Fill the message queue with the messages to be sent
    for (int i = 1; i <= messageCount; ++i) {
        // Message format: "<myProcessId>:<messageId>:<messageContent>"
        std::string message = std::to_string(myProcessId) + ":" + std::to_string(i) + ":Message " + std::to_string(i);
        std::lock_guard<std::mutex> lock(queueMutex);
        messageQueue.emplace(destAddr, std::make_pair(message, i));

        // Log the first time each message is broadcasted
        logBroadcast(i, destProcessId);
    }

    // Create a pool of 6 threads to handle sending messages
    for (int i = 0; i < 6; ++i) {
        sendThreads.emplace_back(&PerfectLinks::sendWorker, this);
    }

    // Join all sender threads to keep them running
    for (auto &thread : sendThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void PerfectLinks::sendWorker() {
    while (running) {
        std::vector<std::pair<sockaddr_in, std::pair<std::string, int>>> packet;
        
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            // Try to take N messages from the queue
            for (int i = 0; i < packetSize && !messageQueue.empty(); ++i) {
                packet.push_back(messageQueue.pop());
            }
        }

        // If there are no messages to send, return
        if (packet.empty()) {
            return;
        }

        // Combine the messages into a single packet (this is just a conceptual packet for sending)
        std::string combinedPacket;
        for (const auto& msg : packet) {
            combinedPacket += msg.second.first + ";"; // Add a delimiter to separate messages in the packet
        }

        // Send the packet
        auto destAddr = packet[0].first;  // Assuming all messages in the packet are sent to the same destination
        ssize_t sent_bytes = sendto(sockfd, combinedPacket.c_str(), combinedPacket.size(), 0,
                                    reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
        if (sent_bytes < 0) {
            perror("sendto failed (packet)");
        } else {
            std::cout << "Packet sent: " << combinedPacket << std::endl;
        }

        // After sending the packet, check for acknowledgments and requeue unacknowledged messages
        for (const auto& msg : packet) {
            int messageId = msg.second.second;
            {
                std::lock_guard<std::mutex> ackLock(ackMutex);
                if (acknowledgments.find(messageId) == acknowledgments.end() || !acknowledgments[messageId]) {
                    // Requeue the message if not acknowledged
                    std::lock_guard<std::mutex> queueLock(queueMutex);
                    messageQueue.push(msg);  // Put the message back in the queue
                }
            }
        }

        // Short delay to avoid spamming the receiver
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}


void PerfectLinks::deliver() {
    char buffer[1024];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, reinterpret_cast<struct sockaddr*>(&srcAddr), &srcAddrLen);
        if (len < 0) {
            if (running) {
                perror("recvfrom failed");
            }
            continue;
        }

        buffer[len] = '\0';
        std::string receivedPacket(buffer);

        // Split the packet into individual messages using the ';' delimiter (or your preferred delimiter)
        std::istringstream packetStream(receivedPacket);
        std::string message;
        while (std::getline(packetStream, message, ';')) {
            // If it's an acknowledgment, mark it in the acknowledgments map
            if (message.rfind("ACK:", 0) == 0) {
                int ackNumber = std::stoi(message.substr(4));
                {
                    std::lock_guard<std::mutex> lock(ackMutex);
                    acknowledgments[ackNumber] = true;
                }
                ackCv.notify_all();
                std::cout << "Acknowledgment received for Message ID: " << ackNumber << std::endl;
            }
            // Otherwise, it's a new message that we need to acknowledge and deliver
            else {
                size_t firstColon = message.find(':');
                size_t secondColon = message.find(':', firstColon + 1);

                // Extract sender process ID and message ID
                int senderProcessId = std::stoi(message.substr(0, firstColon));
                int messageId = std::stoi(message.substr(firstColon + 1, secondColon - firstColon - 1));

                std::cout << "Message received from Process " << senderProcessId << ": " << message << std::endl;

                // Check if the message has already been delivered (avoid duplication)
                {
                    std::lock_guard<std::mutex> lock(deliveryMutex);
                    std::pair<int, int> msgPair = {senderProcessId, messageId};
                    if (deliveredMessages.find(msgPair) != deliveredMessages.end()) {
                        // If the message is already delivered, skip logging and acknowledgment
                        std::cout << "Duplicate message detected: Message ID " << messageId << " from Process " << senderProcessId << std::endl;
                    } else {
                        // Mark the message as delivered
                        deliveredMessages.insert(msgPair);
                        // Log the delivery of the message to the output file
                        logDelivery(messageId, senderProcessId);
                    }
                }

                // Send acknowledgment back
                std::string ackMessage = "ACK:" + std::to_string(messageId);
                ssize_t sent_bytes = sendto(sockfd, ackMessage.c_str(), ackMessage.size(), 0,
                                            reinterpret_cast<struct sockaddr*>(&srcAddr), sizeof(srcAddr));
                if (sent_bytes < 0) {
                    perror("sendto failed (acknowledgment)");
                } else {
                    std::cout << "Acknowledgment sent for Message ID " << messageId << " to Process " << senderProcessId << std::endl;
                }
            }
        }
    }
}




// Function to log when a message is broadcasted
void PerfectLinks::logBroadcast(int messageId, int processId) {
    logFile << "Broadcast: Message " << messageId << " to Process " << processId << std::endl;
}

// Function to log when a message is delivered
void PerfectLinks::logDelivery(int messageId, int processId) {
    logFile << "Delivered: Message " << messageId << " from Process " << processId << std::endl;
}

void PerfectLinks::stopDelivering() {
    running = false;
    close(sockfd);
}
