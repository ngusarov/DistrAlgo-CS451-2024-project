
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

    deliveryWorkerThread = std::thread(&UniformReliableBroadcast::processDeliveryTasks, this);
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

    // Increment or initialize the plDeliveredCount for this message
    auto& count = plDeliveredCount[{origProcId, messageId}];
    count++; // TODO does it actually increment?

    std::stringstream ss;
    ss << "{" << origProcId << "," << messageId << "} count " 
    << (count + 1) << "/" << numOfProcesses << std::endl;
    std::cout << ss.str();

    if (count + 1 >= numOfProcesses/2+1) {  // If majority acked (one process has always acked by default - the sender)
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


void UniformReliableBroadcast::fifoDeliver(int origProcId, int maxMessageId) {
    {    
        std::lock_guard<std::mutex> fifoLock(fifoDelivMutex);
        std::lock_guard<std::mutex> logBufferLock(logBufferMutex);

        for (int msgId = fifoDelivered[origProcId] + 1; msgId <= maxMessageId; ++msgId) {
            fifoDelivered[origProcId] = msgId;
            logBuffer.push_back("d " + std::to_string(origProcId) + " " + std::to_string(msgId) + "\n");
            std::cout << "FIFO Delivered d " + std::to_string(origProcId) + " " + std::to_string(msgId) + "\n";
            if (logBuffer.size() >= logBufferThreshold) {
                flushLogBuffer();
            }
        }
    }
}


void UniformReliableBroadcast::enqueueDeliveryTask(int senderProcessId, int origProcId, int messageId, bool flagReBroadcast) {
    {
        std::unique_lock<std::mutex> lock(deliveryQueueMutex);
        deliveryQueue.emplace(senderProcessId, origProcId, messageId, flagReBroadcast);
    }
    deliveryQueueCv.notify_one();  // Notify the worker thread
}


void UniformReliableBroadcast::processDeliveryTasks() {
    while (running) {
        std::tuple<int, int, int, bool> task;

        {
            std::unique_lock<std::mutex> lock(deliveryQueueMutex);
            deliveryQueueCv.wait(lock, [this]() { return !deliveryQueue.empty() || !running; });

            if (!running && deliveryQueue.empty()) {
                return;  // Exit the loop if the worker is stopped and the queue is empty
            }

            task = deliveryQueue.front();
            deliveryQueue.pop();
        }

        // Process the delivery task
        int senderProcessId = std::get<0>(task);
        int origProcId = std::get<1>(task);
        int messageId = std::get<2>(task);
        bool flagReBroadcast = std::get<3>(task);

        notifyDelivery(origProcId, messageId);
        if (flagReBroadcast) {
            reBroadcast(senderProcessId, origProcId, messageId);
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


UniformReliableBroadcast::~UniformReliableBroadcast() {
    running = false;  // Signal the worker thread to stop
    deliveryQueueCv.notify_all();  // Wake up the worker thread
    if (deliveryWorkerThread.joinable()) {
        deliveryWorkerThread.join();  // Wait for the worker thread to finish
    }
}