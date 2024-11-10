#include "PerfectLinks.hpp"
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sstream>


PerfectLinks::PerfectLinks(int sockfd, const struct sockaddr_in &myAddr, std::ofstream& logFile, int myProcessId)
    : sockfd(sockfd), myAddr(myAddr), logFile(logFile), myProcessId(myProcessId) {
        logFile << std::unitbuf;  // Set the log file to unbuffered mode
    }

void PerfectLinks::startSending(const sockaddr_in &destAddr, int messageCount) {
    this->destAddr = destAddr;


    flagSleep = false;
    flagShrinkQueue = false;

    loopCounter = 0;

    {
        std::unique_lock<std::mutex> pointerLock(pointerMutex);
        std::unique_lock<std::mutex> subQueueLock(subQueueMutex);
        std::unique_lock<std::mutex> mainQueueLock(mainQueueMutex);

        mainQueue.addSegment({1, messageCount});

        windowSize = std::min(2000000, messageCount);  // Define window size
        
        mainQueue.deleteSegment({1, windowSize});

        for (int i = 1; i <= windowSize; ++i) {
            subQueue.push_back(i);
            logFile << "b " << i << "\n";  // Log the broadcast
        }
        if (!subQueue.empty()) {
            sendPointer = {0, subQueue[0]};  // Initialize send pointer
        }
    }

    // Launch sender threads
    for (int i = 0; i < 3; ++i) {
        sendThreads.emplace_back(&PerfectLinks::sendWorker, this);
    }

    doneLogging = true;

    // for (auto &thread : sendThreads) {
    //     if (thread.joinable()) {
    //         thread.join();
    //     }
    // }
}




void PerfectLinks::sendWorker() {
    while (running) {
        std::vector<int> packet;

        {
            std::unique_lock<std::mutex> pointerLock(pointerMutex);
            std::unique_lock<std::mutex> subQueueLock(subQueueMutex);

            // Prepare packet from the current pointer position
            for (size_t i = 0; i < static_cast<size_t>(packetSize) && sendPointer.first + i < subQueue.size(); ++i) {
                packet.push_back(subQueue[sendPointer.first + i]);
            }

            // Ensure the pointer is within bounds and update its value
            if (sendPointer.first < subQueue.size()) {
                // Update the send pointer
                sendPointer.first += static_cast<size_t>(packetSize);
                sendPointer.second = subQueue[sendPointer.first];
            } else {
                std::cout << "Pointer went up, reloop, packetSize: " << packet.size() << std::endl; 
                flagSleep = true;
                ++loopCounter;
                // If the pointer goes out of bounds, reset it to the start of the subQueue
                sendPointer = {0, subQueue.empty() ? -1 : subQueue.front()};
            }
        }   

        if (packet.empty() || flagSleep) {
            std::cout << "Putting threads to sleep" << std::endl;
            std::unique_lock<std::mutex> lock(sleepMutex);
            sleepCv.wait_for(lock, std::chrono::milliseconds(1));
            flagSleep = false;
            flagShrinkQueue = true;
            continue;
        }



        // Send the packet
        std::string combinedPacket;
        // combinedPacket.reserve(16384);
        for (int messageId : packet) {
            combinedPacket += std::to_string(messageId) + ";";
        }

        combinedPacket += std::to_string((-1)*loopCounter) + ";";

        ssize_t sent_bytes = sendto(sockfd, combinedPacket.c_str(), combinedPacket.size(), 0,
                                    reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
        if (sent_bytes < 0) {
            perror("sendto failed (packet)");
        } else {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);
            std::cout << "Packet sent: " << combinedPacket << " Time " << duration.count() << " ms" << std::endl;
        }

        // std::this_thread::sleep_for(std::chrono::microseconds(100));  // Try 100 µs, then adjust

    }
}






void PerfectLinks::receiveAcknowledgments() {
    char buffer[16384];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    // std::vector<int> acknowledgedMessages;
    MessageSegments acknowledgedMessages;

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, reinterpret_cast<struct sockaddr*>(&srcAddr), &srcAddrLen);
        if (len < 0) {
            if (running) {
                // perror("recvfrom failed");
            }
            continue;
        }

        buffer[len] = '\0';
        std::string receivedMessage(buffer);

        // Acknowledgment consists only of message IDs
        std::istringstream ackStream(receivedMessage);
        std::string ack;

        
        while (std::getline(ackStream, ack, ';')) {
            acknowledgedMessages.addMessage(std::stoi(ack));
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);
            std::cout << "Ack ID: " << std::stoi(ack) << " Time " << duration.count() << " ms" << std::endl;
        }

        if (flagShrinkQueue){
            std::unique_lock<std::mutex> pointerLock(pointerMutex);
            std::unique_lock<std::mutex> subQueueLock(subQueueMutex);
            std::unique_lock<std::mutex> mainQueueLock(mainQueueMutex);
            
            // Shrinking queue based on acknowledged messages
            for (const auto& segment : acknowledgedMessages.getSegments()) {
                auto leftIt = std::lower_bound(subQueue.begin(), subQueue.end(), segment.first);
                auto rightIt = std::upper_bound(subQueue.begin(), subQueue.end(), segment.second);

                if (leftIt != subQueue.end() && leftIt != rightIt && *leftIt <= segment.second) {
                    std::cout << "Erasing elements in range [" << *leftIt << ", " 
                            << *(--rightIt) << "]\n";  // Showing the range
                    subQueue.erase(leftIt, ++rightIt);  // Erase from left to right inclusive
                } else {
                    std::cout << "No valid range to erase for segment [" 
                            << segment.first << ", " << segment.second << "]\n";
                }
            }

            // Add new messages from the main queue
            while (subQueue.size() < static_cast<std::size_t>(windowSize)) {
                int nextMessage = mainQueue.getNextMessage();
                if (nextMessage == -1) break;
                subQueue.push_back(nextMessage);
                logFile << "b " << nextMessage << "\n";  // Log the broadcast
            }

            // Update sendPointer if necessary
            auto it = std::find(subQueue.begin(), subQueue.end(), sendPointer.second);
            if (it != subQueue.end()) {
                sendPointer = {static_cast<size_t>(std::distance(subQueue.begin(), it)), *it};
            } else {
                sendPointer = {0, subQueue.empty() ? -1 : subQueue[0]};
            }

            flagShrinkQueue = false;
        }

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
        std::vector<std::pair<sockaddr_in, int>> localBatch;

        {
            std::unique_lock<std::mutex> queueLock(receivedQueueMutex);
            receivedQueueCv.wait(queueLock, [&]() { return !receivedQueue.empty() || !running; });

            while (!receivedQueue.empty() && localBatch.size() < static_cast<size_t>(packetSize)) {
                localBatch.push_back(receivedQueue.front());
                receivedQueue.pop_front();
            }
        }

        std::unordered_map<sockaddr_in, std::vector<int>, AddressHash, AddressEqual> ackPackets;

        for (const auto& [srcAddr, messageId] : localBatch) {
            auto it = addressToProcessId.find(srcAddr);
            if (it != addressToProcessId.end()) {
                int senderProcessId = it->second;

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);
                std::cout << "ACK for message " << messageId 
                            << " from Process " << senderProcessId << " Time " << duration.count() << " ms" << std::endl;

                ackPackets[srcAddr].push_back(messageId);
            }
        }

        for (const auto& [destAddr, ackPacket] : ackPackets) {
            std::string combinedAck;
            for (const auto& messageId : ackPacket) {
                combinedAck += std::to_string(messageId) + ";";
            }

            ssize_t sent_bytes = sendto(sockfd, combinedAck.c_str(), combinedAck.size(), 0,
                                        reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
            if (sent_bytes < 0) {
                perror("sendto failed (ack packet)");
            }else{
                std::cout << "ACK packet sent to Address " << destAddr.sin_port 
                << ": " << combinedAck << std::endl;
            }
        }
    }
}





void PerfectLinks::receiveMessages() {
    char buffer[16384];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0,
                               reinterpret_cast<struct sockaddr*>(&srcAddr), &srcAddrLen);
        if (len < 0) {
            if (running) {
                continue;
            }
        }

        buffer[len] = '\0';
        std::istringstream packetStream(buffer);
        std::string message;

        std::unordered_map<sockaddr_in, std::vector<int>, AddressHash, AddressEqual> ackPackets;

        while (std::getline(packetStream, message, ';')) {
            int messageId = std::stoi(message);

            ackPackets[srcAddr].push_back(messageId);

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - startTime);

            auto it = addressToProcessId.find(srcAddr);
            // if (it != addressToProcessId.end()) {
            int senderProcessId = it->second;

            if (messageId <= 0){
                std::cout << "LOOP COUNT" << messageId << " from Address " << senderProcessId;
            }

            {
                std::unique_lock<std::mutex> queueLock(receivedQueueMutex);
                receivedQueue.emplace_back(srcAddr, messageId);
                std::cout << "Received message " << messageId 
                    << " from Address " << senderProcessId
                    << " Time " << duration.count() << " ms" << " Size: "<< receivedQueue.size() << std::endl;
            }
            receivedQueueCv.notify_one();  // Notify deliveryWorker

            {
                std::unique_lock<std::mutex> deliveryLock(this->deliveryMutex);
                if (!deliveredMessages[senderProcessId].find(messageId)) {

                    deliveredMessages[senderProcessId].addMessage(messageId);
                    logFile << "d " << senderProcessId << " " << messageId << "\n";
                    logFile.flush();
                }
            }
            // }
        }

    }
}









// void PerfectLinks::receiverLogWorker() {
//     const size_t batchSize = 500;  // Increase batch size to handle larger volumes efficiently
//     std::string logBatch;

//     while (running || !receiverLogQueue.empty()) {
//         {
//             std::unique_lock<std::mutex> logLock(this->logMutex);
//             logCv.wait(logLock, [&]() { return !receiverLogQueue.empty() || !running; });

//             while (!receiverLogQueue.empty() && logBatch.size() < batchSize) {
//                 auto logEntry = receiverLogQueue.front();
//                 receiverLogQueue.pop_front();

//                 // Accumulate log entries for delivery: "d <senderProcessId> <messageId>"
//                 logBatch += "d " + std::to_string(logEntry.first) + " " + std::to_string(logEntry.second) + "\n";
//             }
//         }

//         // Write the entire batch to the log file in one operation
//         if (!logBatch.empty()) {
//             logFile << logBatch;
//             logFile.flush();  // Explicitly flush after writing the batch
//             logBatch.clear();
//         }
//     }
// }



void PerfectLinks::stopDelivering() {
    running = false;
    close(sockfd);
}
