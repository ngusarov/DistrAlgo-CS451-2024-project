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
    // Start the sender logging thread
    senderLogThread = std::thread(&PerfectLinks::senderLogWorker, this);

    // Create a pool of 3 threads to handle sending messages
    for (int i = 0; i < 3; ++i) {
        sendThreads.emplace_back(&PerfectLinks::sendWorker, this);
    }

    doneLogging = false;
    // Main thread: Fill the message queue and enqueue logs while iterating
    for (int i = 1; i <= messageCount; ++i) {
        std::string message = std::to_string(myProcessId) + ":" + std::to_string(i);

        // Enqueue the messageId for logging (for sender)
        {
            std::unique_lock<std::mutex> logLock(logMutex);
            senderLogQueue.push_back(i);  // Log message ID only
        }
        // Notify the sender log thread that there's a new log entry
        logCv.notify_one();

        {
            std::unique_lock<std::mutex> queueLock(queueMutex);
            messageQueue.emplace_back(destAddr, std::make_pair(message, i));
        }

        // Notify one of the sending threads that a new message is available
        queueCv.notify_one();
    }

    doneLogging = true;

    // Join all sender threads
    for (auto &thread : sendThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // Stop the sender logging thread
    {
        std::unique_lock<std::mutex> logLock(logMutex);
        running = false;
    }
    logCv.notify_all();  // Wake up the log thread if it's waiting

    if (senderLogThread.joinable()) {
        senderLogThread.join();
    }
}


void PerfectLinks::sendWorker() {
    while (running) {
        // Temporary batch for processing messages
        std::vector<std::pair<sockaddr_in, std::pair<std::string, int>>> localBatch;
        std::unordered_set<int> toDeleteBatch;  // Collect IDs for deletion after sending
        std::vector<std::pair<sockaddr_in, std::pair<std::string, int>>> unacknowledgedBatch; // To re-queue

        {   
            std::unique_lock<std::mutex> queueLock(this->queueMutex);

            // Extract a batch for sending, skip marked for deletion
            for (int i = 0; i < packetSize && !messageQueue.empty(); ++i) {
                auto msg = messageQueue.front();
                int messageId = msg.second.second;

                if (toDeleteBatch.find(messageId) != toDeleteBatch.end()) {
                    messageQueue.pop_front();
                    continue;
                }

                messageQueue.pop_front();  // Temporarily remove message from queue
                localBatch.push_back(msg);  // Add to local batch
            }
        }

        if (localBatch.empty()) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            continue;
        }

        // Prepare messages for resending
        std::vector<std::pair<sockaddr_in, std::pair<std::string, int>>> packet;

        for (const auto& msg : localBatch) {
            int messageId = msg.second.second;

            {   
                std::unique_lock<std::mutex> ackLock(this->ackMutex);
                std::unique_lock<std::mutex> messageMapLock(this->messageMapMutex);

                if (acknowledgments.find(messageId) != acknowledgments.end()) {
                    --messageMap[messageId];
                    if (messageMap[messageId] <= 0) {
                        toDeleteBatch.insert(messageId);  // Mark message for deletion locally
                        acknowledgments.erase(messageId);
                        messageMap.erase(messageId);
                        std::cout << "Message " << messageId << " marked for removal " << std::endl;

                    }
                } else {
                    // Message not acknowledged; add to packet and to re-queue list
                    packet.push_back(msg);
                    unacknowledgedBatch.push_back(msg);  // Prepare to re-queue
                }
            }
        }

        if (packet.empty()) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            continue;
        }

        // Send combined packet
        std::string combinedPacket;
        combinedPacket.reserve(16384);
        for (const auto& msg : packet) {
            combinedPacket += msg.second.first + ";";
        }

        auto destAddr = packet[0].first;
        ssize_t sent_bytes = sendto(sockfd, combinedPacket.c_str(), combinedPacket.size(), 0,
                                    reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
        if (sent_bytes < 0) {
            perror("sendto failed (packet)");
        } else {
            std::cout << "Packet sent: " << combinedPacket << std::endl;
        }

        // Carefully remove messages marked for deletion from the queue
        {
            std::unique_lock<std::mutex> queueLock(this->queueMutex);

            for (auto it = messageQueue.rbegin(); it != messageQueue.rend(); ) {
                if (toDeleteBatch.find(it->second.second) != toDeleteBatch.end()) {
                    it = std::deque<std::pair<sockaddr_in, std::pair<std::string, int>>>::reverse_iterator(
                        messageQueue.erase(std::next(it).base()));
                } else {
                    ++it;
                }
            }

            // Re-queue the unacknowledged messages
            for (const auto& msg : unacknowledgedBatch) {
                messageQueue.push_back(msg);
            }
        }
    }
}








void PerfectLinks::senderLogWorker() {
    const size_t batchSize = 500;  // Increase batch size to handle larger volumes efficiently

    std::string logBatch;

    while (!doneLogging || !senderLogQueue.empty()) {
        {
            std::unique_lock<std::mutex> logLock(this->logMutex);
            logCv.wait(logLock, [&]() { return !senderLogQueue.empty() || !running; });

            while (!senderLogQueue.empty() && logBatch.size() < batchSize) {
                int messageId = senderLogQueue.front();
                senderLogQueue.pop_front();

                // Accumulate log entries for broadcast
                logBatch += "b " + std::to_string(messageId) + "\n";
            }
        }

        // Write the entire batch to the log file in one operation
        if (!logBatch.empty()) {
            logFile << logBatch;
            logFile.flush();  // Explicitly flush after writing the batch
            logBatch.clear();
        }
    }
    
    std::cerr << "Done with logging\n";

    // // Start slowly iterating through the front 10% of the messageQueue
    // while (running) {
    //     std::unordered_set<int> initialBatchIds;
    //     int tenPercentSize = 0;

    //     {
    //         // Lock only to calculate the 10% size and extract message IDs for analysis
    //         std::unique_lock<std::mutex> queueLock(this->queueMutex);
    //         tenPercentSize = static_cast<int>(messageQueue.size() * 0.1);
    //         if (tenPercentSize > 0) {
    //             // Record the IDs of the front 10% of messages for later comparison
    //             for (int i = 0; i < tenPercentSize; ++i) {
    //                 initialBatchIds.insert(messageQueue[static_cast<size_t>(i)].second.second);
    //             }
    //         }
    //     }

    //     // Analyze extracted messages outside the lock
    //     std::unordered_set<int> messagesToDelete;

    //     for (int messageId : initialBatchIds) {
    //         bool shouldErase = false;

    //         {
    //             // Lock only for acknowledgment and message map check
    //             std::unique_lock<std::mutex> ackLock(this->ackMutex);
    //             std::unique_lock<std::mutex> messageMapLock(this->messageMapMutex);

    //             if (acknowledgments.find(messageId) != acknowledgments.end() && messageMap[messageId] == 1) {
    //                 shouldErase = true;
    //             }
    //         }

    //         if (shouldErase) {
    //             // Mark the message ID for deletion
    //             messagesToDelete.insert(messageId);
    //         }
    //     }

    //     {
    //         // Lock again to find and delete the messages based on their shifted positions
    //         std::unique_lock<std::mutex> queueLock(this->queueMutex);
    //         std::unique_lock<std::mutex> ackLock(this->ackMutex);
    //         std::unique_lock<std::mutex> messageMapLock(this->messageMapMutex);

    //         // Iterate through the queue to find elements in the recorded initial batch
    //         for (auto it = messageQueue.begin(); it != messageQueue.end(); ++it) {
    //             int currentMessageId = it->second.second;

    //             // If the current element is not in the initial batch, stop the iteration
    //             if (initialBatchIds.find(currentMessageId) == initialBatchIds.end()) {
    //                 break;
    //             }

    //             // If the current element should be deleted, erase it from all structures
    //             if (messagesToDelete.find(currentMessageId) != messagesToDelete.end()) {
    //                 it = messageQueue.erase(it);
    //                 acknowledgments.erase(currentMessageId);
    //                 messageMap.erase(currentMessageId);

    //                 // Log the removal
    //                 std::cerr << "Message " << currentMessageId << " killed in queue" << " AckSize " << acknowledgments.size() << " QuSize " << messageQueue.size() << std::endl;

    //                 // Adjust the iterator to avoid skipping elements
    //                 --it;
    //             }
    //         }
    //     }

    //     // Pause briefly between iterations to avoid high CPU usage
    //     std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    // }
}




void PerfectLinks::receiveAcknowledgments() {
    char buffer[16384];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

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
            int ackNumber = std::stoi(ack);
            {
                std::unique_lock<std::mutex> ackLock(this->ackMutex);
                acknowledgments.insert(ackNumber);
                std::cout << "Ack ID: " << ackNumber << ";;" << " AckSize " << acknowledgments.size() << std::endl;
            }
            ackCv.notify_all();
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


void PerfectLinks::startSendingAcks() {
    for (int i = 0; i < 3; ++i) {
        ackThreads.emplace_back(&PerfectLinks::ackWorker, this);
    }

    for (auto &thread : ackThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}


void PerfectLinks::ackWorker() {
    while (running) {
        std::unordered_map<int, std::vector<std::tuple<sockaddr_in, int, int>>> ackPackets;

        {
            std::unique_lock<std::mutex> ackQueueLock(this->ackQueueMutex);

            // Process each queue of acks for each sender process
            for (auto& [senderProcessId, ackQueue] : processAckQueues) {
                std::vector<std::tuple<sockaddr_in, int, int>> ackPacket;
                for (int i = 0; i < packetSize && !ackQueue.empty(); ++i) {
                    auto ackTuple = ackQueue.front();
                    ackQueue.pop_front();

                    int messageId = std::get<2>(ackTuple);
                    std::pair<int, int> msgPair = {senderProcessId, messageId};

                    {
                        std::unique_lock<std::mutex> deliveryLock(this->deliveryMutex);

                        // Only acknowledge if the message is delivered
                        if (deliveredMessages.find(msgPair) != deliveredMessages.end()) {
                            ackPacket.push_back(ackTuple);

                            // Debugging output
                            std::cout << "ACK: " << senderProcessId << ":" << messageId << std::endl;
                        }
                    }
                }
                if (!ackPacket.empty()) {
                    ackPackets[senderProcessId] = ackPacket;  // Store the packet for this process
                }
            }
        }

        // Send acknowledgment packets for each process
        for (const auto& [senderProcessId, ackPacket] : ackPackets) {
            // Combine the acks into a single packet
            std::string combinedAck;
            combinedAck.reserve(16384);
            for (const auto& ack : ackPacket) {
                combinedAck += std::to_string(std::get<2>(ack)) + ";";  // Combine the message IDs
            }

            // Send to the correct process address
            auto destAddr = std::get<0>(ackPacket[0]);
            ssize_t sent_bytes = sendto(sockfd, combinedAck.c_str(), combinedAck.size(), 0,
                                        reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
            if (sent_bytes < 0) {
                perror("sendto failed (ack packet)");
            } else {
                std::cout << "ACK packet sent to Process " << senderProcessId << ": " << combinedAck << std::endl;
            }
        }
    }
}




void PerfectLinks::receiveMessages() {
    char buffer[16384]; // Adjusted buffer size
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    // Start the receiver logging thread
    receiverLogThread = std::thread(&PerfectLinks::receiverLogWorker, this);

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, reinterpret_cast<struct sockaddr*>(&srcAddr), &srcAddrLen);
        if (len < 0) {
            if (running) {
                // perror("recvfrom failed");
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
                std::unique_lock<std::mutex> deliveryLock(this->deliveryMutex);
                std::pair<int, int> msgPair = {senderProcessId, messageId};
                if (deliveredMessages.find(msgPair) == deliveredMessages.end()) {
                    deliveredMessages.insert(msgPair);

                    // Add the log entry to the receiver logging queue with the senderId
                    {
                        std::unique_lock<std::mutex> logLock(this->logMutex);
                        receiverLogQueue.push_back(std::make_pair(senderProcessId, messageId));
                    }

                    // Notify the receiver log thread that there's a new log entry
                    logCv.notify_one();
                }
            }

            // Queue acknowledgment (messageId only)
            std::string ackMessage = std::to_string(messageId);
            {
                std::unique_lock<std::mutex> ackQueueLock(this->ackQueueMutex);
                
                // Enqueue ack in the appropriate process queue based on senderProcessId
                processAckQueues[senderProcessId].push_back(std::make_tuple(srcAddr, senderProcessId, messageId));
            }

        }
    }

    // Stop the receiver logging thread
    {
        std::unique_lock<std::mutex> logLock(this->logMutex);
        running = false;
    }
    logCv.notify_all();  // Wake up the log thread if it's waiting

    if (receiverLogThread.joinable()) {
        receiverLogThread.join();
    }
}




void PerfectLinks::receiverLogWorker() {
    const size_t batchSize = 500;  // Increase batch size to handle larger volumes efficiently
    std::string logBatch;

    while (running || !receiverLogQueue.empty()) {
        {
            std::unique_lock<std::mutex> logLock(this->logMutex);
            logCv.wait(logLock, [&]() { return !receiverLogQueue.empty() || !running; });

            while (!receiverLogQueue.empty() && logBatch.size() < batchSize) {
                auto logEntry = receiverLogQueue.front();
                receiverLogQueue.pop_front();

                // Accumulate log entries for delivery: "d <senderProcessId> <messageId>"
                logBatch += "d " + std::to_string(logEntry.first) + " " + std::to_string(logEntry.second) + "\n";
            }
        }

        // Write the entire batch to the log file in one operation
        if (!logBatch.empty()) {
            logFile << logBatch;
            logFile.flush();  // Explicitly flush after writing the batch
            logBatch.clear();
        }
    }
}



void PerfectLinks::stopDelivering() {
    running = false;
    close(sockfd);
}
