
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
        std::lock_guard<std::mutex> urbLock(urbDelivMutex);

        // Ensure the message hasn't been URB-delivered yet
        if (urbDelivered[origProcId].find(messageId)) return;

        urbDelivered[origProcId].addMessage(messageId);
    }

    {
        std::lock_guard<std::mutex> logLock(logMutex);
        logFile << "d " + std::to_string(origProcId) + " " + std::to_string(messageId) + "\n";
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
