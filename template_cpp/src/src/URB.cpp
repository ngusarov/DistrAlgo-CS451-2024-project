
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
}

void UniformReliableBroadcast::broadcastMessage(int messageId) {
    {
        std::lock_guard<std::mutex> logLock(logMutex);
        logFile << "b " + std::to_string( messageId) + "\n";
    }

    // // Self-acknowledge and deliver the message
    // pl->acknowledgeMessage(myAddr,  myProcessId, messageId);
    // pl->deliverMessage(myProcessId, myProcessId, messageId, false);

    sockaddr_in destAddr;
    for (const auto& procId : allProcessIds) {
        if (procId == myProcessId) continue;  // Skip self
        destAddr = processIdToAddress[procId];
        pl->sendMessage(destAddr, {myProcessId, messageId});
    }
}

void UniformReliableBroadcast::notifyDelivery(int origProcId, int messageId) {
    std::lock_guard<std::mutex> plLock(plDelivCountMutex);

    // Increment or initialize the plDeliveredCount for this message
    auto& count = plDeliveredCount[{origProcId, messageId}];
    count++;

    std::cout << "{"<<origProcId << ","<< messageId<<"}" << " count " << count << "/" << numOfProcesses << std::endl;

    if (count >= numOfProcesses-1) {  // If all processes have delivered
        plDeliveredCount.erase({origProcId, messageId});
        urbDeliver(origProcId, messageId);
    }
}

void UniformReliableBroadcast::urbDeliver(int origProcId, int messageId) {
    {
        std::lock_guard<std::mutex> fifoLock(fifoDelivMutex);
        std::lock_guard<std::mutex> urbLock(urbDelivMutex);

        int lastFifoDelivered = fifoDelivered[origProcId];
        auto& segments = urbDelivered[origProcId].getSegments();

        if (messageId == lastFifoDelivered + 1){
            fifoDeliver(origProcId);

            auto it = segments.begin();
            auto next = std::next(it);
            if (next != segments.end() && next->first == lastFifoDelivered + 2) {
                // Recursively handle the second segment
                for (int i = next->first; i <= next->second; ++i){
                    fifoDeliver(origProcId);
                }
            }
        }

        urbDelivered[origProcId].addMessage(messageId);
    }
}

void UniformReliableBroadcast::fifoDeliver(int origProcId) {
    {    
        std::lock_guard<std::mutex> fifoLock(fifoDelivMutex);
        std::lock_guard<std::mutex> logLock(logMutex);
        
        ++fifoDelivered[origProcId];
        logFile << "d " + std::to_string(origProcId) + " " + std::to_string(fifoDelivered[origProcId]) + "\n";
    }
}


void UniformReliableBroadcast::reBroadcast(int senderProcessId, int origProcId, int messageId) {
    sockaddr_in destAddr;
    for (const auto& procId : allProcessIds) {
        if (procId == myProcessId || procId == senderProcessId) continue;  // Skip self and sender
        destAddr = processIdToAddress[procId];
        pl->sendMessage(destAddr, {origProcId, messageId});
    }
}
