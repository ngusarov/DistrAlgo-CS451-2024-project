
#include "URB.hpp"
#include "PerfectLinks.hpp"  // Include the full PerfectLinks definition here

#include <iostream>  // For std::cout (debugging)
#include <sstream>   // For std::to_string()

UniformReliableBroadcast::UniformReliableBroadcast(PerfectLinks* pl, std::ofstream& logFile, 
        const struct sockaddr_in &myAddr, int myProcessId, const std::vector<int>& processIds)
    : pl(pl), logFile(logFile), 
        myAddr(myAddr), myProcessId(myProcessId), allProcessIds(processIds) {
    logFile << std::unitbuf;  // Set the log file to unbuffered mode

    numOfProcesses = static_cast<int>(processIds.size());
    if (processIds.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw std::overflow_error("Too many processes, exceeds int capacity");
    }

    // Initialize sendPointer for all addresses in addressToProcessId
    for (int processId : allProcessIds) {
        fifoDelivered[processId] = 0;
        urbDelivered[processId].addMessage(0);
    }

    windowSize = 100;  // Set the window size

    logBufferThreshold = 1000;  // Set the log buffer threshold
}

void UniformReliableBroadcast::broadcastMessage(int messageId) {
    {
        std::lock_guard<std::mutex> logBufferLock(logBufferMutex);
        logBuffer.push_back("b " + std::to_string(messageId) + "\n");
        if (logBuffer.size() >= logBufferThreshold) {
            flushLogBuffer();
        }
    }

    sockaddr_in destAddr;
    for (const auto& procId : allProcessIds) {
        if (procId == myProcessId) continue;  // Skip self
        destAddr = processIdToAddress[procId];
        pl->sendMessage(destAddr, {myProcessId, messageId});
    }
}

void UniformReliableBroadcast::broadcastManyMessages(int messageId) {
        
    logBuffer.push_back("b " + std::to_string(messageId) + "\n");

    sockaddr_in destAddr;
    for (const auto& procId : allProcessIds) {
        if (procId == myProcessId) continue;  // Skip self
        destAddr = processIdToAddress[procId];
        pl->sendManyMessages(destAddr, {myProcessId, messageId});
    }
}


void UniformReliableBroadcast::notifyDelivery(int origProcId, int messageId) {
    std::lock_guard<std::mutex> plLock(plDelivCountMutex);

    // Initialize or increment the plDeliveredCount for this message
    auto it = plDeliveredCount.find({origProcId, messageId});
    if (it == plDeliveredCount.end()) {
        plDeliveredCount[{origProcId, messageId}] = 2;  // Include sender's acknowledgment
    } else {
        plDeliveredCount[{origProcId, messageId}]++;    // Increment count
    }

    int count = plDeliveredCount[{origProcId, messageId}];


    std::stringstream ss;
    ss << "{" << origProcId << "," << messageId << "} count " 
    << (count) << "/" << numOfProcesses << std::endl;
    std::cout << ss.str();

    if (2*count > numOfProcesses) {  // If majority acked (one process has always acked by default - the sender)
        plDeliveredCount.erase({origProcId, messageId});
        urbDeliver(origProcId, messageId);
    }
}

void UniformReliableBroadcast::urbDeliver(int origProcId, int messageId) {
    int lastFifoDelivered;
    {
        std::lock_guard<std::mutex> fifoLock(fifoDelivMutex);
        lastFifoDelivered = fifoDelivered[origProcId];
    }    

    {
        std::lock_guard<std::mutex> urbLock(urbDelivMutex);   
        auto& segments = urbDelivered[origProcId].getSegments();
        if (messageId == lastFifoDelivered + 1) {
            fifoDeliver(origProcId, messageId);
            auto it = segments.begin();
            if (it != segments.end()) {
                auto next = std::next(it);
                if (next != segments.end() && next->first == fifoDelivered[origProcId] + 1) {
                    fifoDeliver(origProcId, next->second);
                }
            }
        }
        urbDelivered[origProcId].addMessage(messageId);
    }
}


void UniformReliableBroadcast::fifoDeliver(int origProcId, int maxMessageId) {
    int numOfFIFOdelivered = 0;
    {    
        std::lock_guard<std::mutex> fifoLock(fifoDelivMutex);
        std::lock_guard<std::mutex> logBufferLock(logBufferMutex);

        numOfFIFOdelivered = maxMessageId - fifoDelivered[origProcId];
        for (int msgId = fifoDelivered[origProcId] + 1; msgId <= maxMessageId; ++msgId) {
            fifoDelivered[origProcId] = msgId;
            logBuffer.push_back("d " + std::to_string(origProcId) + " " + std::to_string(msgId) + "\n");
            // std::cout << "FIFO Delivered d " + std::to_string(origProcId) + " " + std::to_string(msgId) + "\n";
            if (logBuffer.size() >= logBufferThreshold) {
                flushLogBuffer();
            }
        }
    }

    for (int i = 0; i < numOfFIFOdelivered; ++i) {
        // Broadcast the next message if this process is the origin
        if (origProcId == myProcessId) {
            int nextMessageId;
            {
                std::lock_guard<std::mutex> pendingLock(pendingMessagesMutex);
                nextMessageId = pendingMessages.getNextMessage();  // Fetch the next message
            }
            if (nextMessageId != -1) {
                broadcastMessage(nextMessageId);
            }
        }
    }
}



void UniformReliableBroadcast::reBroadcast(int senderProcessId, int origProcId, int messageId) {
    sockaddr_in destAddr;
    for (const auto& procId : allProcessIds) {
        if (procId == myProcessId || procId == senderProcessId) continue;  // Skip self and sender
        if (procId == origProcId || pl->deliveredMessages[procId][origProcId].find(messageId)) continue;  // Skip origin and from whom delivered
        destAddr = processIdToAddress[procId];
        pl->sendMessage(destAddr, {origProcId, messageId});
    }
}


void UniformReliableBroadcast::flushLogBuffer() {
    std::lock_guard<std::mutex> logLock(logMutex); // Ensure exclusive access to the file
    for (const auto& logEntry : logBuffer) {
        logFile << logEntry;
    }
    logBuffer.clear(); // Clear the buffer after flushing
    logFile.flush();   // Ensure immediate write to the file
}