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

struct MessageComparator_recQueue {
    bool operator()(const std::tuple<std::pair<int, int>, int>& a,
                    const std::tuple<std::pair<int, int>, int>& b) const {
        if (std::get<1>(a) != std::get<1>(b)) {
            return std::get<1>(a) > std::get<1>(b);  // Compare counts (descending).
        }
        return std::get<0>(a) < std::get<0>(b);  // Compare {origProcId, messageId} lexicographically.
    }
};


// Message struct
struct Message {
    int messageId;
    int initSenderId;
    sockaddr_in address;

    bool operator==(const Message& other) const {
        return messageId == other.messageId &&
               initSenderId == other.initSenderId &&
               memcmp(&address, &other.address, sizeof(sockaddr_in)) == 0;
    }
};

// Hash function for Message (for unordered_map)
struct MessageHash {
    size_t operator()(const Message& msg) const {
        size_t h1 = std::hash<int>()(msg.messageId);
        size_t h2 = std::hash<int>()(msg.initSenderId);
        size_t h3 = std::hash<uint32_t>()(msg.address.sin_addr.s_addr);
        size_t h4 = std::hash<uint16_t>()(msg.address.sin_port);
        return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
    }
};

// Metadata struct for multiset
struct MessageMetadata {
    Message message;
    int numOfSendings;

    bool operator<(const MessageMetadata& other) const {
        if (numOfSendings != other.numOfSendings) {
            return numOfSendings < other.numOfSendings;
        }
        return std::tie(message.messageId, message.initSenderId, message.address.sin_port) <
               std::tie(other.message.messageId, other.message.initSenderId, other.message.address.sin_port);
    }
};




class PerfectLinks {
public:
    PerfectLinks(int sockfd, const sockaddr_in &myAddr, int myProcessId);

    void sendMessage(const sockaddr_in &destAddr, std::pair<int, int> message);
    void sendManyMessages(const sockaddr_in &destAddr, std::pair<int, int> message);
    void sendWorker();
    void receive();
    void ackWorker();
    void stopDelivering();  // To stop receiving/sending

    void deliverMessage(int senderProcessId, int origProcId, int messageId, bool flagReBroadcast);

    UniformReliableBroadcast* urb; // Pointer to URB instance

    unsigned long packetSize;  // Add packet size as a public member variable

public:
    int sockfd;  // The socket file descriptor used for communication
    sockaddr_in myAddr;  // The address of this process
    int myProcessId;  // ID of the current process
    std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> addressToProcessId;

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    std::atomic<bool> running{true};  // Flag to indicate if the deliver thread should keep running

    // Data structures for sending messages
    std::mutex queueMutex;  // Protects access to messageMap and messageSet
    std::unordered_map<Message, int, MessageHash> messageMap;  // Lookup table for messages
    std::multiset<MessageMetadata> messageSet;  // Sorted by numOfSendings
    std::unordered_map<sockaddr_in, std::deque<std::pair<int, int>>, AddressHash, AddressEqual> packetsToSend;  // {destAddr : [{origProcID, messageID} ...]}
    std::atomic<int> minSeqNum;  // Lowest sequence number in the sliding window
    std::atomic<bool> flagShrinkQueue{false};  // Indicates if queue cleanup is needed


    // for cleaning the subQueue
    std::atomic<int> numOfNewAcks;
    std::atomic<int> numOfNewAcksThreshold;

    // Data structures for receiving messages
    std::mutex deliveryMutex;  // Protects the deliveredMessages map
    std::unordered_map<int, std::unordered_map<int, MessageSegments>> deliveredMessages;  // {recepientID :  { origProcID : Segments} ... }
    
    std::mutex receivedQueueMutex;
    std::condition_variable receivedQueueCv;
    std::unordered_map<sockaddr_in, std::set<std::tuple<std::pair<int, int>, int>, MessageComparator_recQueue>, AddressHash, AddressEqual> receivedQueue; // recepientAddr : [...<<origProcID, messageID>, counts>...]

    bool flagSleep;
    std::condition_variable sleepCv;  // Condition variable for sleeping
    std::mutex sleepMutex;            // Mutex to protect the condition variable
    std::atomic<bool> sleepThreads{false};  // Flag to signal sleep
};
