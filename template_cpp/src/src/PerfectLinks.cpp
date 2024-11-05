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

    // Start the sender logging thread
    senderLogThread = std::thread(&PerfectLinks::senderLogWorker, this);

    // Create a pool of 3 threads to handle sending messages
    for (int i = 0; i < 3; ++i) {
        sendThreads.emplace_back(&PerfectLinks::sendWorker, this);
    }

    doneLogging = false;

    // Main thread: Fill the message queue with message IDs only
    for (int i = 1; i <= messageCount; ++i) {
        // Enqueue the messageId for logging (for sender)
        {
            std::unique_lock<std::mutex> logLock(logMutex);
            senderLogQueue.push_back(i);  // Log message ID only
        }
        logCv.notify_one();  // Notify the sender log thread

        // Add only message ID to the queue
        {
            std::unique_lock<std::mutex> queueLock(queueMutex);
            messageQueue.push_back(i);  // Only message ID is added
        }
        queueCv.notify_one();  // Notify one of the sending threads
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
        std::vector<int> localBatch;
        // std::unordered_set<int> toDeleteBatch;  // Collect IDs for deletion after sending
        std::vector<int> unacknowledgedBatch;    // To re-queue

        {   
            std::unique_lock<std::mutex> queueLock(this->queueMutex);

            // Extract a batch for sending, skip marked for deletion
            for (int i = 0; i < packetSize && !messageQueue.empty(); ++i) {
                int messageId = messageQueue.front();
                
                // if (toDeleteBatch.find(messageId) != toDeleteBatch.end()) {
                //     messageQueue.pop_front();
                //     continue;
                // }

                messageQueue.pop_front();  // Temporarily remove message from queue
                localBatch.push_back(messageId);  // Add to local batch
            }
        }

        if (localBatch.empty()) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            continue;
        }

        // Prepare messages for resending
        std::vector<int> packet;

        for (int messageId : localBatch) {
            if (messageId > 0){
            {   
                std::unique_lock<std::mutex> ackLock(this->ackMutex);
                std::unique_lock<std::mutex> messageMapLock(this->messageMapMutex);
                std::unique_lock<std::mutex> messageCounterMapLock(this->messageCounterMapMutex);

                if (acknowledgments.find(messageId) != acknowledgments.end()) {
                    --messageMap[messageId];
                    if (messageMap[messageId] <= 0) {
                        // toDeleteBatch.insert(messageId);  // Mark message for deletion locally
                        acknowledgments.erase(messageId);
                        messageMap.erase(messageId);
                        std::cout << "Message " << messageId << " marked for removal " << std::endl;

                    }
                } else {
                    

                    if (messageCounterMap.find(messageId) != messageCounterMap.end()){
                        ++messageCounterMap[messageId];
                    }else{
                        messageCounterMap[messageId] = 1;
                    }


                    // if (messageCounterMap[messageId] > 50) {
                    //     std::this_thread::sleep_for(std::chrono::nanoseconds(10));
                    //     // // 10% probability check
                    //     // if (rand() % 50 == 0) {
                    //     //     // Message not acknowledged; add to packet and to re-queue list
                    //     //     packet.push_back(messageId);
                    //     //     messageMap[messageId] = 1; // TODO do we need it here????
                    //     // }else{
                    //     //     --messageCounterMap[messageId];
                    //     // }
                    // } 
                    // else {
                    //     // Message not acknowledged; add to packet and to re-queue list
                    //     packet.push_back(messageId);
                    //     messageMap[messageId] = 1; // TODO do we need it here????
                    // }

                    // Message not acknowledged; add to packet and to re-queue list
                    packet.push_back(messageId);
                    messageMap[messageId] = 1; // TODO do we need it here????

                    unacknowledgedBatch.push_back(messageId);  // Prepare to re-queue
                }
            }
            }else{
                packet.push_back(messageId);
            }
        }

        if (packet.empty()) {
            std::cout << "Nothing to send: chilling" << std::endl;
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            continue;
        }
        // else if (packet.size() < 2){
        //     std::cout << "Too small packet: starting over" << std::endl;
        //     std::this_thread::sleep_for(std::chrono::nanoseconds(500));
        //     continue;
        // }

        // Send combined packet with only message IDs
        std::string combinedPacket;
        combinedPacket.reserve(16384);
        for (int messageId : packet) {
            combinedPacket += std::to_string(messageId) + ";";
        }

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

            // for (auto it = messageQueue.rbegin(); it != messageQueue.rend(); ) {
            //     if (toDeleteBatch.find(*it) != toDeleteBatch.end()) {
            //         std::cout << "Message " << *it << " fully removed " << std::endl;
            //         it = std::deque<int>::reverse_iterator(
            //             messageQueue.erase(std::next(it).base()));
            //     } else {
            //         ++it;
            //     }
            // }

            // Re-queue the unacknowledged messages
            for (int messageId : unacknowledgedBatch) {
                messageQueue.push_back(messageId);
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
    
    // std::cerr << "Done with logging\n";
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

                --messageCounterMap[ackNumber];
                if (messageCounterMap[ackNumber] <= 0){
                    {
                        std::unique_lock<std::mutex> queueLock(this->queueMutex);
                        messageQueue.push_front(ackNumber * (-1));
                        // messageQueue.push_front(ackNumber * (-1));
                        // messageQueue.push_front(ackNumber * (-1));
                    }
                    messageCounterMap.erase(ackNumber);

                    std::cout << "Erased counter " << ackNumber << "}" << std::endl;
                }

                if (messageMap.find(ackNumber) != messageMap.end()){ // TODO otherwise, already not interested
                    acknowledgments.insert(ackNumber); 
                }
                std::cout << "Ack ID: " << ackNumber << ";;" << " AckSize " << acknowledgments.size() << " QueSize " << messageQueue.size() << std::endl;
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
        std::unordered_map<sockaddr_in, std::vector<int>, AddressHash, AddressEqual> ackPackets;


        {
            std::unique_lock<std::mutex> ackQueueLock(this->ackQueueMutex);

            for (auto& [srcAddr, ackQueue] : processAckQueues) {
                std::vector<int> ackPacket;
                for (int i = 0; i < packetSize && !ackQueue.empty(); ++i) {
                    int messageId = ackQueue.front();
                    ackQueue.pop_front();

                    std::unique_lock<std::mutex> deliveryLock(this->deliveryMutex);
                    auto it = addressToProcessId.find(srcAddr);
                    if (it != addressToProcessId.end()) {
                        int senderProcessId = it->second;

                        // Check if the message was delivered
                        if (deliveredMessages.find({senderProcessId, messageId}) != deliveredMessages.end()) {
                            ackPacket.push_back(messageId);
                            std::cout << "ACK for message " << messageId << " from Address " << senderProcessId << " Size " << deliveredMessages.size() << std::endl;
                        }
                    }

                }
                if (!ackPacket.empty()) {
                    ackPackets[srcAddr] = ackPacket;
                }
            }
        }

        for (const auto& [destAddr, ackPacket] : ackPackets) {
            std::string combinedAck;
            combinedAck.reserve(16384);
            for (const auto& messageId : ackPacket) {
                combinedAck += std::to_string(messageId) + ";";
            }

            ssize_t sent_bytes = sendto(sockfd, combinedAck.c_str(), combinedAck.size(), 0,
                                        reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
            if (sent_bytes < 0) {
                perror("sendto failed (ack packet)");
            } else {
                std::cout << "ACK packet sent to Address " << destAddr.sin_port << ": " << combinedAck << std::endl;
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

        // Skip messages from unknown addresses
        if (addressToProcessId.find(srcAddr) == addressToProcessId.end()) {
            continue;  // Ignore messages from unrecognized addresses
        }

        int senderProcessId = addressToProcessId[srcAddr];

        // Check if the message was "received" from the process itself
        if (senderProcessId == myProcessId) {
            continue;  // Ignore messages from the process itself
        }

        // Split the packet into individual messages
        std::istringstream packetStream(receivedPacket);
        std::string message;

        
        while (std::getline(packetStream, message, ';')) {
            int messageId = std::stoi(message);

            // std::cout << "Received " << senderProcessId << " " << messageId << std::endl;

            // Check for duplication
            {
                std::unique_lock<std::mutex> deliveryLock(this->deliveryMutex);
                // Instead of directly accessing addressToProcessId[srcAddr], use find and AddressEqual
                auto it = addressToProcessId.find(srcAddr);
                if (it != addressToProcessId.end()) {
                    int senderProcessId = it->second;

                    // Check if the message is already delivered
                    std::pair<int, int> msgPair = {senderProcessId, messageId};

                    if (messageId < 0){
                        deliveredMessages.erase(std::make_pair(senderProcessId, messageId * (-1)));

                        std::cout << "Erased " << senderProcessId << " " << messageId * (-1) << " Size " << deliveredMessages.size() << std::endl;

                        continue;
                    }

                    if (deliveredMessages.find(msgPair) == deliveredMessages.end()) {
                        deliveredMessages.insert(msgPair);

                        // Queue log entry as a pair for delivery logs
                        std::unique_lock<std::mutex> logLock(this->logMutex);
                        receiverLogQueue.push_back(std::make_pair(senderProcessId, messageId));
                        logCv.notify_one();
                    }
                }

            }

            // Queue acknowledgment by associating srcAddr and messageId
            {
                std::unique_lock<std::mutex> ackQueueLock(this->ackQueueMutex);
                processAckQueues[srcAddr].push_back(messageId);  // Queue ack by source address
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
