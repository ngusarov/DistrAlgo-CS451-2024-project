#include "PerfectLinks.hpp"
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

PerfectLinks::PerfectLinks(int sockfd, const struct sockaddr_in &myAddr, std::ofstream& logFile, int myProcessId)
    : sockfd(sockfd), myAddr(myAddr), logFile(logFile), myProcessId(myProcessId) {}

void PerfectLinks::startSending(const sockaddr_in &destAddr, int messageCount) {
    // Fill the message queue with messages in the "processID:messageID" format
    for (int i = 1; i <= messageCount; ++i) {
        std::string message = std::to_string(myProcessId) + ":" + std::to_string(i);
        std::lock_guard<std::mutex> lock(queueMutex);
        messageQueue.emplace(destAddr, std::make_pair(message, i));

        logBroadcast(i);
    }

    // Create a pool of 3 threads to handle sending messages
    for (int i = 0; i < 3; ++i) {
        sendThreads.emplace_back(&PerfectLinks::sendWorker, this);
    }

    // Join all sender threads
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
                packet.push_back(messageQueue.front());
                messageQueue.pop();
            }
        }

        if (packet.empty()) {
            continue;
        }

        // Combine the messages into a single packet
        std::string combinedPacket;
        for (const auto& msg : packet) {
            combinedPacket += msg.second.first + ";"; // Combine the messages with a delimiter
        }

        auto destAddr = packet[0].first;
        ssize_t sent_bytes = sendto(sockfd, combinedPacket.c_str(), combinedPacket.size(), 0,
                                    reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
        if (sent_bytes < 0) {
            perror("sendto failed (packet)");
        } else {
            std::cout << "Packet sent: " << combinedPacket << std::endl;
        }

        // Requeue unacknowledged messages
        for (const auto& msg : packet) {
            int messageId = msg.second.second;
            {
                std::lock_guard<std::mutex> ackLock(ackMutex);
                if (acknowledgments.find(messageId) == acknowledgments.end() || !acknowledgments[messageId]) {
                    std::lock_guard<std::mutex> queueLock(queueMutex);
                    messageQueue.push(msg);  // Put the message back in the queue
                }
            }
        }
    }
}

void PerfectLinks::receiveMessages() {
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

        // Split the packet into individual messages
        std::istringstream packetStream(receivedPacket);
        std::string message;
        while (std::getline(packetStream, message, ';')) {
            size_t colonPos = message.find(':');

            int senderProcessId = std::stoi(message.substr(0, colonPos));
            int messageId = std::stoi(message.substr(colonPos + 1));

            // Check for duplication
            {
                std::lock_guard<std::mutex> lock(deliveryMutex);
                std::pair<int, int> msgPair = {senderProcessId, messageId};
                if (deliveredMessages.find(msgPair) == deliveredMessages.end()) {
                    deliveredMessages.insert(msgPair);
                    logDelivery(messageId, senderProcessId);
                }
            }

            // Queue acknowledgment (messageId only)
            std::string ackMessage = std::to_string(messageId);
            {
                std::lock_guard<std::mutex> ackQueueLock(ackQueueMutex);
                ackQueue.push(std::make_pair(srcAddr, ackMessage));
            }
        }
    }
}

void PerfectLinks::ackWorker() {
    while (running) {
        std::vector<std::pair<sockaddr_in, std::string>> ackPacket;

        {
            std::lock_guard<std::mutex> lock(ackQueueMutex);
            for (int i = 0; i < packetSize && !ackQueue.empty(); ++i) {
                ackPacket.push_back(ackQueue.front());
                ackQueue.pop();
            }
        }

        if (ackPacket.empty()) {
            continue;
        }

        // Combine the acks into a single packet (only message IDs)
        std::string combinedAck;
        for (const auto& ack : ackPacket) {
            combinedAck += ack.second + ";";  // Combine the message IDs
        }

        auto destAddr = ackPacket[0].first;
        ssize_t sent_bytes = sendto(sockfd, combinedAck.c_str(), combinedAck.size(), 0,
                                    reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
        if (sent_bytes < 0) {
            perror("sendto failed (ack packet)");
        } else {
            std::cout << "ACK packet sent: " << combinedAck << std::endl;
        }
    }
}


void PerfectLinks::startReceivingAcks() {
    for (int i = 0; i < 3; ++i) {
        ackThreads.emplace_back(&PerfectLinks::ackWorker, this);
    }

    for (auto &thread : ackThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void PerfectLinks::receiveAcknowledgments() {
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
        std::string receivedMessage(buffer);

        // Acknowledgment consists only of message IDs
        std::istringstream ackStream(receivedMessage);
        std::string ack;
        std::cout << "Acknowledgment received for Message ID: ";
        while (std::getline(ackStream, ack, ';')) {
            int ackNumber = std::stoi(ack);
            {
                std::lock_guard<std::mutex> lock(ackMutex);
                acknowledgments[ackNumber] = true;
            }
            ackCv.notify_all();
            std::cout << ackNumber << " ";
        }
        std::cout << std::endl;
    }
}


void PerfectLinks::logBroadcast(int messageId) {
    logFile << "b " << messageId << std::endl;
}

void PerfectLinks::logDelivery(int messageId, int processId) {
    logFile << "d " << processId << " " << messageId << std::endl;
}

void PerfectLinks::stopDelivering() {
    running = false;
    close(sockfd);
}
