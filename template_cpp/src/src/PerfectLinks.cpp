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

        // TODO
        // In the case of URB, we deliver only upon sending a message back (unlike in simple links). So this should be made a priority
        
        numOfNewAcks = 0;

        numOfNewAcksThreshold = 100;  // Threshold for cleaning the queue
        
      
        // minSeqNum = 1;
        startTime = std::chrono::high_resolution_clock::now();
}

void PerfectLinks::sendManyMessages(const sockaddr_in &destAddr, std::pair<int, int> message) {
    Message msg = {message.second, message.first, destAddr};  // Create the message structure

    messageMap[msg] = 0;  // Initialize send count
    messageSet.insert({msg, 0});  // Add to the set with initial send count

    // Prepare a string stream for logging
    // std::ostringstream logStream;
    // logStream << "For " << addressToProcessId[destAddr]
    //           << " Put {" << message.first << ", " << message.second << "} "
    //           << "in the queue. Current queue size: " << messageSet.size() << std::endl;

    // Output the accumulated logs
    // std::cout << logStream.str();
}


void PerfectLinks::sendMessage(const sockaddr_in &destAddr, std::pair<int, int> message) {
    Message msg = {message.second, message.first, destAddr};  // Create the message structure

    std::unique_lock<std::mutex> queueLock(queueMutex);
    // std::unique_lock<std::mutex> deliveryLock(deliveryMutex);

    // auto& delivMap = deliveredMessages[addressToProcessId[msg.address]];

    // if (delivMap[msg.initSenderId].find(msg.messageId)) {
    //     return;  // Skip if the message is already acknowledged or delivered
    // }

    if (messageMap.find(msg) == messageMap.end()) {
        // Add to the map and the set
        messageMap[msg] = 0;  // Initialize send count
        messageSet.insert({msg, 0});  // Add to the set with initial send count
        
        // Prepare a string stream for logging
        // std::ostringstream logStream;
        // logStream << "For " << addressToProcessId[destAddr]
        //         << " Put {" << message.first << ", " << message.second << "} "
        //         << "in the queue. Current queue size: " << messageSet.size() << std::endl;

        // Output the accumulated logs
        // std::cout << logStream.str();
    }
}


void PerfectLinks::sendWorker() {

    while (running) {
        std::pair<sockaddr_in, std::string> onePacketToSend;

        std::vector<MessageMetadata> reinsertionBuffer;


        {
            std::unique_lock<std::mutex> queueLock(queueMutex);
            std::unique_lock<std::mutex> packetsLock(packetsMutex);

            for (auto it = messageSet.begin(); it != messageSet.end();) {
                const Message& msg = it->message;
                int numSendings = it->numOfSendings;
                int seqNum = msg.messageId;
                const sockaddr_in& destAddr = msg.address;

                // std::stringstream ss;
                // ss << "Curr size of queue " << messageSet.size() << std::endl
                // << "Considering message " << msg.initSenderId << " " << seqNum 
                // << " for " << ntohs(destAddr.sin_port) << std::endl;

                // std::cout << ss.str();

                // Skip messages outside the sliding window // TODO: reconsider
                // if (numSendings >= 1){
                //     {        
                //         std::unique_lock<std::mutex> deliveryLock(deliveryMutex);

                //         auto& delivMap = deliveredMessages[addressToProcessId[msg.address]];

                //         // if (ackMap[msg.initSenderId].find(msg.messageId) || delivMap[msg.initSenderId].find(msg.messageId)) {
                //         if (delivMap[msg.initSenderId].find(msg.messageId)) {
                //             flagShrinkQueue = true; // Mark for cleanup
                //             continue;
                //         }
                //     }
                // }else
                if (numOfNewAcks > numOfNewAcksThreshold){
                    flagShrinkQueue = true; // Mark for cleanup
                    break;
                }else if (std::next(it) == messageSet.end()){
                    flagShrinkQueue = true; // Mark for cleanup
                    flushPackets = true; // Mark for flush
                }

                // Add message to the packet for the destination
                auto& packetQueue = packetsToSend[destAddr];
                if (std::none_of(packetQueue.begin(), packetQueue.end(),
                                 [msg](const std::pair<int, int>& m) {
                                     return m.first == msg.initSenderId && m.second == msg.messageId;
                                 })) {
                    packetQueue.emplace_back(msg.initSenderId, seqNum);

                    // Increment sending counter and update the set
                    MessageMetadata updatedMetadata = *it;
                    updatedMetadata.numOfSendings++;

                    // std::stringstream ss;
                    // ss << "Included " << msg.initSenderId << " " << seqNum << " for "
                    //           << addressToProcessId[destAddr] << "; Num sendings: " << numSendings << std::endl;
                    // std::cout << ss.str();

                    // Stop forming packets if the current destination packet is full
                    if (packetQueue.size() >= packetSize) {
                        std::string packet = "m";
                        for (const auto& m : packetQueue) {
                            // std::stringstream ss;
                            // ss << "Comb packet " << m.first << " " << m.second << " for "
                            //         << addressToProcessId[destAddr] << "; Num sendings: " << numSendings << std::endl;
                            // std::cout << ss.str();
                            packet += std::to_string(m.first) + ":" + std::to_string(m.second) + ";";
                        }
                        packetQueue.clear();  // Clear the packet queue after forming the packet
                        onePacketToSend = {destAddr, packet};


                        messageMap[msg] = numSendings + 1;  // Update the send count
                        it = messageSet.erase(it);
                        reinsertionBuffer.push_back(updatedMetadata);

                        break;
                    }
                    messageMap[msg] = numSendings + 1;  // Update the send count
                    it = messageSet.erase(it);
                    reinsertionBuffer.push_back(updatedMetadata);

                }else{
                    ++it;
                }

            }

            // Reinsert updated elements into the set after iteration
            for (const auto& metadata : reinsertionBuffer) {
                messageSet.insert(metadata);
            }
            reinsertionBuffer.clear();  // Clear the buffer
        }


        if (flushPackets){
            std::unique_lock<std::mutex> packetsLock(packetsMutex);
            for (auto& [destAddr, packetQueue] : packetsToSend) {
                if (!packetQueue.empty()) {
                    std::string packet = "m";
                    for (const auto& m : packetQueue) {
                        packet += std::to_string(m.first) + ":" + std::to_string(m.second) + ";";
                    }
                    packetQueue.clear();  // Clear the packet queue after forming the packet
                    onePacketToSend = {destAddr, packet};

                    // Send the packet outside the critical section
                    const std::string& packetToSend = onePacketToSend.second;
                    ssize_t sent_bytes = sendto(sockfd, packetToSend.c_str(), packetToSend.size(), 0,
                                                reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
                    if (sent_bytes < 0) {
                        perror("sendto failed");
                    } else {
                        std::stringstream ss;
                        ss << "Packet sent to " << addressToProcessId[destAddr] << ": " << packetToSend << std::endl;
                        std::cout << ss.str();
                    }
                }
            }
            flushPackets = false;
        }
        else if (!onePacketToSend.second.empty()) { // Send the formed packet outside the critical section
            const sockaddr_in& destAddr = onePacketToSend.first;
            const std::string& packet = onePacketToSend.second;

            ssize_t sent_bytes = sendto(sockfd, packet.c_str(), packet.size(), 0,
                                        reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
            if (sent_bytes < 0) {
                perror("sendto failed");
            } 
            else {
                std::stringstream ss;
                ss << "Packet sent to " << addressToProcessId[destAddr] << ": " << packet << std::endl;
                std::cout << ss.str();
            }
        }

        // Shrink the queue if necessary
        {
            std::unique_lock<std::mutex> queueLock(queueMutex);
            std::unique_lock<std::mutex> deliveryLock(deliveryMutex);

            if (flagShrinkQueue) {


                numOfNewAcks = 0;

                // Prepare a string stream for logging
                std::ostringstream logStream;

                // Log the initial queue size
                logStream << "Shrinking the queue. Initial size: " << messageSet.size() << std::endl;

                std::vector<MessageMetadata> removalBuffer;  // Buffer for elements to be removed

                for (auto it = messageSet.begin(); it != messageSet.end();) {
                    const Message& msg = it->message;

                    auto& delivMap = deliveredMessages[addressToProcessId[msg.address]];

                    // Identify messages to be removed
                    if (delivMap[msg.initSenderId].find(msg.messageId)) {
                        logStream << "Deleting message: {"
                                << msg.initSenderId << ", "
                                << msg.messageId << "} for port "
                                << addressToProcessId[msg.address]
                                << "; Queue size before deletion: " << messageSet.size() << std::endl;


                        removalBuffer.push_back(*it);  // Add to removal buffer
                        it = messageSet.erase(it);    // Safely erase from the set
                      
                        logStream << "Queue size after deletion: " << messageSet.size() << std::endl;
                    } else {
                        ++it;  // Move to the next element
                    }
                }

                // Process elements in the removal buffer
                for (const auto& metadata : removalBuffer) {
                    const Message& msg = metadata.message;
                    messageMap.erase(msg);  // Remove from the map
                }


                // Update minSeqNum
                if (!messageSet.empty()) {
                    minSeqNum = messageSet.begin()->message.messageId;
                } else {
                    minSeqNum = 0;
                }

                flagShrinkQueue = false;

                // Output the accumulated logs
                std::cout << logStream.str();
            }
        }



        std::this_thread::sleep_for(std::chrono::nanoseconds(1));  // Avoid busy waiting
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


void PerfectLinks::ackWorker() {
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

                    // // Reduce count and reinsert if still relevant
                    // if (receptionCount / 10 > 0) {
                    //     toReinsert.emplace_back(origPair, receptionCount / 10);
                    // }
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
            }
            // else {
            //     std::cout << "ACK to " + std::to_string(addressToProcessId[srcAddr]) + ": " + combinedAck.c_str() + "\n";
            // }
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


                    // std::stringstream ss;
                    // ss << "From " <<  senderProcessId << " Received m:" << origProcId << " " << messageId
                    // << " T=" << duration.count() << " ms" << std::endl;
                    // std::cout << ss.str();
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
                std::stringstream ss;
                ss << "From " <<  senderProcessId << " Ack a:" << origProcId << " " << messageId
                << " T=" << duration.count() << " ms" << std::endl;
                std::cout << ss.str();

                deliverMessage(senderProcessId, origProcId, messageId, false);
            }
        }
    }
}

void PerfectLinks::deliverMessage(int senderProcessId, int origProcId, int messageId, bool flagReBroadcast) {
    bool delivered = false;
    {
        std::unique_lock<std::mutex> deliveryLock(deliveryMutex);
        if (!deliveredMessages[senderProcessId][origProcId].find(messageId)) {
            deliveredMessages[senderProcessId][origProcId].addMessage(messageId);
            delivered = true;
            numOfNewAcks++;
        }
    }

    if (urb && delivered) {
        urb->notifyDelivery(origProcId, messageId);  // Notify URB of plDelivery
        if (flagReBroadcast){
            urb->reBroadcast(senderProcessId, origProcId, messageId);  // Re-broadcast logic
        }
    }
}



void PerfectLinks::stopDelivering() {
    running = false;
    close(sockfd);
}
