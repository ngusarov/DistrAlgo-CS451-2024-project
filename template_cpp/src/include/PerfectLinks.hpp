#pragma once

#include <cstdlib>  // For rand()
#include <unordered_set>     // For unordered_set
#include <unordered_map>     // For unordered_map
#include <set>               // For set (if needed)
#include <queue>             // For queue
#include <mutex>             // For mutex and lock_guard
#include <condition_variable>// For condition_variable
#include <thread>            // For threads
#include <atomic>            // For atomic<bool>
#include <netinet/in.h>      // For sockaddr_in and related network functions
#include <sys/socket.h>      // For socket functions (sendto, recvfrom)
#include <arpa/inet.h>       // For inet_ntoa
#include <iostream>          // For input/output streams (std::cout, std::endl)
#include <fstream>           // For ofstream (log file)
#include <functional>        // For std::hash (used in PairHash)
#include <utility>           // For std::pair
#include <vector>            // For std::vector
#include <string>            // For std::string
#include <cstring>  // For memcmp

#include <chrono>

#include "MessageSegments.hpp"  // For MessageSegments

// Forward declaration of URB
class UniformReliableBroadcast;


// Custom hash function for sockaddr_in
struct AddressHash {
    std::size_t operator()(const sockaddr_in& addr) const {
        // Combine the IP address and port into a single hash value
        return std::hash<uint32_t>()(addr.sin_addr.s_addr) ^ std::hash<uint16_t>()(addr.sin_port);
    }
};

// Equality operator for sockaddr_in
struct AddressEqual {
    bool operator()(const sockaddr_in& a, const sockaddr_in& b) const {
        return a.sin_addr.s_addr == b.sin_addr.s_addr && a.sin_port == b.sin_port;
    }
};

struct MessageComparator {
    bool operator()(const std::tuple<std::pair<int, int>, int>& a,
                    const std::tuple<std::pair<int, int>, int>& b) const {
        if (std::get<1>(a) != std::get<1>(b)) {
            return std::get<1>(a) > std::get<1>(b);  // Compare counts (descending).
        }
        return std::get<0>(a) < std::get<0>(b);  // Compare {origProcId, messageId} lexicographically.
    }
};





class PerfectLinks {
public:
    PerfectLinks(int sockfd, const sockaddr_in &myAddr, int myProcessId);

    void sendMessage(const sockaddr_in &destAddr, std::pair<int, int> message);
    void sendWorker();
    void receive();
    void deliveryWorker();
    void stopDelivering();  // To stop receiving/sending

    void acknowledgeMessage(sockaddr_in srcAddr, int origProcId, int messageId);
    void deliverMessage(int senderProcessId, int origProcId, int messageId, bool flagReBroadcast);

    UniformReliableBroadcast* urb; // Pointer to URB instance

    unsigned long packetSize;  // Add packet size as a public member variable
    unsigned long windowSize;  // Add packet size as a public member variable

public:
    int sockfd;  // The socket file descriptor used for communication
    sockaddr_in myAddr;  // The address of this process
    int myProcessId;  // ID of the current process
    std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> addressToProcessId;

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    std::atomic<bool> running{true};  // Flag to indicate if the deliver thread should keep running

    // Data structures for sending messages
    std::mutex pointerMutex;  // Mutex for protecting sendPointer and subQueue operations
    std::unordered_map<sockaddr_in, std::pair<size_t, int>, AddressHash, AddressEqual> sendPointer;  // {recepientAddr, {index, message ID}}
    bool flagShrinkQueue;
    std::mutex subQueueMutex;
    std::unordered_map<sockaddr_in, std::deque<std::pair<int, int>>, AddressHash, AddressEqual> subQueue;  // {recepientAddr, [...{origProcID, messageID}...]}

    // for cleaning the subQueue
    std::mutex acknowledgedMessagesMutex;
    std::unordered_map<sockaddr_in, std::unordered_map<int, MessageSegments>, AddressHash, AddressEqual> acknowledgedMessages; // {recepientAddr :  { origProcID : Segments} ... }

    // Data structures for receiving messages
    std::mutex deliveryMutex;  // Protects the deliveredMessages map
    std::unordered_map<int, std::unordered_map<int, MessageSegments>> deliveredMessages;  // {recepientID :  { origProcID : Segments} ... }
    
    std::mutex receivedQueueMutex;
    std::condition_variable receivedQueueCv;
    std::unordered_map<sockaddr_in, std::set<std::tuple<std::pair<int, int>, int>, MessageComparator>, AddressHash, AddressEqual> receivedQueue; // recepientAddr : [...<<origProcID, messageID>, counts>...]

    bool flagSleep;
    std::condition_variable sleepCv;  // Condition variable for sleeping
    std::mutex sleepMutex;            // Mutex to protect the condition variable
    std::atomic<bool> sleepThreads{false};  // Flag to signal sleep
};
