#pragma once

#include <unordered_set>
#include <unordered_map>
#include <set>
#include <vector>
#include <tuple>
#include <utility>
#include <mutex>
#include <fstream>
#include <netinet/in.h>
#include <condition_variable>

#include "MessageSegments.hpp"  // For MessageSegments

// Forward declaration of PerfectLinks
class PerfectLinks;

// Custom hash function for pairs of ints
struct PairHash {
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2>& pair) const {
        std::size_t h1 = std::hash<T1>()(pair.first);
        std::size_t h2 = std::hash<T2>()(pair.second);
        return h1 ^ (h2 << 1); // Combine the two hash values
    }
};

class UniformReliableBroadcast {
public:
    UniformReliableBroadcast(PerfectLinks* pl, std::ofstream& logFile, 
                const struct sockaddr_in &myAddr, int myProcessId, const std::vector<int>& processIds);

    void broadcastMessage(int messageId);
    void notifyDelivery(int origProcId, int messageId);
    void urbDeliver(int origProcId, int messageId);
    void fifoDeliver(int origProcId);

public:
    PerfectLinks* pl;

    std::unordered_map<int, sockaddr_in> processIdToAddress;
    
    std::mutex logMutex;  // Mutex for accessing the log queues
    std::ofstream& logFile;  // Output file for logging

    sockaddr_in myAddr;  // The address of this process
    int myProcessId;
    

    std::vector<int> allProcessIds;
    int numOfProcesses;

    std::mutex fifoDelivMutex;
    std::unordered_map<int, int> fifoDelivered; // // origProcId : last messageId

    std::mutex urbDelivMutex;
    std::unordered_map<int, MessageSegments> urbDelivered; // origProcId : [...-messageId-...]
    std::mutex plDelivCountMutex;
    std::unordered_map<std::pair<int, int>, int, PairHash> plDeliveredCount; // {origProcId, messageId} : counts ( < numOfProcesses)

    void reBroadcast(int senderProcessId, int origProcId, int messageId);
};