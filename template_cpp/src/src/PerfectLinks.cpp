#include "PerfectLinks.hpp"
#include "URB.hpp"  // Include the full URB definition here
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sstream>



// TODO: 
// unite receiving functions

// 1 thread for receiving
// 2 threads for sending: messages and acks : DO PAUSES and LOCKS!!!


PerfectLinks::PerfectLinks(int sockfd, const struct sockaddr_in &myAddr, int myProcessId)
    : sockfd(sockfd), myAddr(myAddr), myProcessId(myProcessId){

        flagSleep = false;
        flagShrinkQueue = false;

        packetSize = 8;  // TODO Send larger batches, depending on the system's capacity
        windowSize = 10000;  // TODO reconsider size : VIA OPTIMIZATION with time measurements
        // TODO
        // In the case of URB, we deliver only upon sending a message back (unlike in simple links). So this should be made a priority
        

        // Initialize sendPointer for all addresses in addressToProcessId
        for (const auto& [addr, processId] : addressToProcessId) {
            sendPointer[addr] = {0, -1};  // Initialize send pointer as {index, message ID} = {0, -1}
        }

        startTime = std::chrono::high_resolution_clock::now();
        
}

void PerfectLinks::sendMessage(const sockaddr_in &destAddr, std::pair<int, int> message) {
    {
        std::unique_lock<std::mutex> subQueueLock(subQueueMutex);
        subQueue[destAddr].push_back(message);
    }
    std::cout << "For " << addressToProcessId[destAddr] <<  " Put " << message.first << " " << message.second << " in the queue"<< std::endl;
}


void PerfectLinks::sendWorker() {
    while (running) {
        std::unordered_map<sockaddr_in, std::string, AddressHash, AddressEqual> packetsToSend;

        {
            std::unique_lock<std::mutex> pointerLock(pointerMutex);
            std::unique_lock<std::mutex> subQueueLock(subQueueMutex);

            for (auto& [destAddr, queue] : subQueue) {
                // Prepare packet for the current destination
                std::string combinedPacket = "m";  // Packet starts with "m" for message type
                size_t startIndex = sendPointer[destAddr].first;
                size_t i;

                for (i = 0; i < packetSize && startIndex + i < windowSize && startIndex + i < queue.size(); ++i) {
                    combinedPacket += std::to_string(queue[startIndex + i].first) + ":" + 
                                      std::to_string(queue[startIndex + i].second) + ";";
                }

                ++i;

                // std::cout << "StartIndex " << startIndex << " position i: "<< i << std::endl;

                // Update sendPointer for next cycle
                if (startIndex + i < windowSize && startIndex + i < queue.size()) {
                    sendPointer[destAddr] = {startIndex + packetSize, queue[startIndex + packetSize].second};
                } else {
                    flagShrinkQueue = true;
                    sendPointer[destAddr] = {0, queue.empty() ? -1 : queue.front().second};
                }

                if (combinedPacket.size() > 2) {  // Only add non-empty packets
                    packetsToSend[destAddr] = std::move(combinedPacket);
                }
            }
        }  // Release locks here

        // Send packets outside the critical section
        for (const auto& [destAddr, packet] : packetsToSend) {
            ssize_t sent_bytes = sendto(sockfd, packet.c_str(), packet.size(), 0,
                                        reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
            if (sent_bytes < 0) {
                perror("sendto failed");
            } else {
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);
                std::cout << "Packet sent to " << ntohs(destAddr.sin_port) << ": " << packet
                          << " Time " << duration.count() << " ms" << std::endl;
            }
        }

        // Shrink the queue if flagShrinkQueue is set
        if (flagShrinkQueue) {
            std::unique_lock<std::mutex> pointerLock(pointerMutex);
            std::unique_lock<std::mutex> subQueueLock(subQueueMutex);

            for (auto& [destAddr, queue] : subQueue) {
                auto& ackMap = acknowledgedMessages[destAddr];
                auto it = queue.begin();

                while (it != queue.end()) {
                    int origProcId = it->first;
                    int messageId = it->second;

                    if (ackMap[origProcId].find(messageId)) {
                        if (static_cast<size_t>(it - queue.begin()) == sendPointer[destAddr].first) {
                            sendPointer[destAddr].first = static_cast<size_t>((it + 1) - queue.begin());
                            sendPointer[destAddr].second = (sendPointer[destAddr].first < queue.size()) 
                                                            ? queue[sendPointer[destAddr].first].second 
                                                            : -1;
                        }
                        std::cout << "Erasing " << it->first << " " << it->second << " q size " << queue.size()-1 << std::endl;
                        it = queue.erase(it);
                    } else {
                        ++it;
                    }

                }
            }

            flagShrinkQueue = false;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(1));  // Avoid busy waiting
    }
}


//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------
//- ------------------------------------------------------------------------------------------------------------


void PerfectLinks::deliveryWorker() {
    while (running || !receivedQueue.empty()) {
        std::unordered_map<sockaddr_in, std::string, AddressHash, AddressEqual> ackPackets;

        {
            std::unique_lock<std::mutex> queueLock(receivedQueueMutex);
            receivedQueueCv.wait(queueLock, [&]() { return !receivedQueue.empty() || !running; });

            for (auto& [srcAddr, messages] : receivedQueue) {
                size_t count = 0;
                std::vector<std::tuple<std::pair<int, int>, int>> toReinsert;
                std::string combinedAck;  // Start ACK packet with "a"

                auto it = messages.begin();
                while (it != messages.end() && count < packetSize) {
                    const auto& [origPair, receptionCount] = *it;  // origPair = {origProcId, messageId}
                    combinedAck += std::to_string(origPair.first) + ":" + std::to_string(origPair.second) + ";";
                    it = messages.erase(it);
                    count++;

                    // Reduce count and reinsert if still relevant
                    if (receptionCount / 10 > 0) {
                        toReinsert.emplace_back(origPair, receptionCount / 10);
                    }
                }

                // Reinsert messages with reduced reception counts
                for (const auto& entry : toReinsert) {
                    messages.insert(entry);
                }

                // Prepare ACK packet for the current address
                if (!combinedAck.empty()) {
                    combinedAck = "a" + combinedAck;
                    ackPackets[srcAddr] = std::move(combinedAck);
                }
            }
        }  // Release the lock before sending packets

        // Send ACK packets outside the critical section
        for (const auto& [srcAddr, combinedAck] : ackPackets) {
            ssize_t sent_bytes = sendto(sockfd, combinedAck.c_str(), combinedAck.size(), 0,
                                        reinterpret_cast<const struct sockaddr*>(&srcAddr), sizeof(srcAddr));
            if (sent_bytes < 0) {
                perror("sendto failed (ack packet)");
            } else {
                std::cout << "ACK to " << ntohs(srcAddr.sin_port)
                          << ": " << combinedAck << std::endl;
            }
        }
    }
}


void PerfectLinks::receive() {
    char buffer[16384];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, reinterpret_cast<struct sockaddr*>(&srcAddr), &srcAddrLen);
        if (len < 0) {
            if (running) {
                continue;
            }
        }

        buffer[len] = '\0';
        std::string receivedPacket(buffer);

        if (receivedPacket.empty()) continue;

        char packetType = receivedPacket[0];  // First character determines type
        receivedPacket = receivedPacket.substr(1);  // Remove type indicator

        std::istringstream packetStream(receivedPacket);
        std::string entry;

        auto it = addressToProcessId.find(srcAddr);
        int senderProcessId = (it != addressToProcessId.end()) ? it->second : -1;

        if (packetType == 'm') {
            // Handle message packet

            while (std::getline(packetStream, entry, ';')) {
                size_t separator = entry.find(':');
                int origProcId = std::stoi(entry.substr(0, separator));
                int messageId = std::stoi(entry.substr(separator + 1));

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);

                {
                    std::unique_lock<std::mutex> queueLock(receivedQueueMutex);
                    auto it = std::find_if(receivedQueue[srcAddr].begin(), receivedQueue[srcAddr].end(),
                                           [origProcId, messageId](const auto& tuple) {
                                               return std::get<0>(tuple) == std::make_pair(origProcId, messageId);
                                           });

                    if (it != receivedQueue[srcAddr].end()) {
                        auto updatedMessage = *it;
                        receivedQueue[srcAddr].erase(it);
                        std::get<1>(updatedMessage)++;
                        receivedQueue[srcAddr].insert(updatedMessage);
                    } else {
                        receivedQueue[srcAddr].insert({{origProcId, messageId}, 1});
                    }

                    receivedQueueCv.notify_one();

                    std::cout << "Received m:" << senderProcessId << " "<< messageId
                              << " T=" << duration.count() << " ms" << std::endl;
                }

                deliverMessage(senderProcessId, origProcId, messageId, true);

            }
        } else if (packetType == 'a') {
            // Handle acknowledgment packet
            while (std::getline(packetStream, entry, ';')) {
                size_t separator = entry.find(':');
                int origProcId = std::stoi(entry.substr(0, separator));
                int messageId = std::stoi(entry.substr(separator + 1));

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);
                std::cout << "Ack a:" << origProcId<< " " << messageId
                          << " T=" << duration.count() << " ms" << std::endl;

                acknowledgeMessage(srcAddr, origProcId, messageId);
                deliverMessage(senderProcessId, origProcId, messageId, false);
            }
        }
    }
}


void PerfectLinks::acknowledgeMessage(sockaddr_in srcAddr, int origProcId, int messageId) {
    {
        std::unique_lock<std::mutex> ackLock(acknowledgedMessagesMutex);
        if (!acknowledgedMessages[srcAddr][origProcId].find(messageId)) {
            acknowledgedMessages[srcAddr][origProcId].addMessage(messageId);
        }
    }
}

void PerfectLinks::deliverMessage(int senderProcessId, int origProcId, int messageId, bool flagReBroadcast) {
    {
        std::unique_lock<std::mutex> deliveryLock(deliveryMutex);
        if (!deliveredMessages[senderProcessId][origProcId].find(messageId)) {
            deliveredMessages[senderProcessId][origProcId].addMessage(messageId);

            if (urb) {
                urb->notifyDelivery(origProcId, messageId);  // Notify URB of plDelivery
                if (flagReBroadcast){
                    urb->reBroadcast(senderProcessId, origProcId, messageId);  // Re-broadcast logic
                }
            }
        }
    }
}



void PerfectLinks::stopDelivering() {
    running = false;
    close(sockfd);
}
