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

#include "MessageSegments.hpp"

// Custom hash function for pairs of ints
struct PairHash {
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2>& pair) const {
        std::size_t h1 = std::hash<T1>()(pair.first);
        std::size_t h2 = std::hash<T2>()(pair.second);
        return h1 ^ (h2 << 1); // Combine the two hash values
    }
};

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
    bool operator()(const std::tuple<sockaddr_in, int, int>& a,
                    const std::tuple<sockaddr_in, int, int>& b) const {
        // Compare based on the third element first (descending order)
        if (std::get<2>(a) != std::get<2>(b)) {
            return std::get<2>(a) > std::get<2>(b);
        }
        // If counts are the same, compare the second element (ascending order)
        if (std::get<1>(a) != std::get<1>(b)) {
            return std::get<1>(a) < std::get<1>(b);
        }
        // As a tie-breaker, compare the sockaddr_in structures using IP and port
        return memcmp(&std::get<0>(a), &std::get<0>(b), sizeof(sockaddr_in)) < 0;
    }
};




class PerfectLinks {
public:
    PerfectLinks(int sockfd, const sockaddr_in &myAddr, std::ofstream& logFile, int myProcessId);

    void startSending(const sockaddr_in &destAddr, int messageCount);
    void receiveMessages();  // For the receiver to receive and log messages
    void receiveAcknowledgments();  // For the sender to handle acknowledgment reception
    void startSendingAcks();  // For the receiver to send acknowledgments
    void stopDelivering();  // To stop receiving/sending
    void deliveryWorker();

    int packetSize;  // Add packet size as a public member variable
    int windowSize;  // Add packet size as a public member variable
    bool isReceiver;

    bool doneLogging;

public:
    int sockfd;  // The socket file descriptor used for communication
    sockaddr_in myAddr;  // The address of this process
    sockaddr_in destAddr;  // The address of this process
    std::ofstream& logFile;  // Output file for logging
    int myProcessId;  // ID of the current process

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;


    // Data structures for storing state
    
    std::unordered_map<int, MessageSegments> deliveredMessages;  // Map processId -> MessageSegments for efficient delivery tracking
    std::mutex deliveryMutex;  // Protects the deliveredMessages set
    std::atomic<bool> running{true};  // Flag to indicate if the deliver thread should keep running


    std::mutex pointerMutex;  // Mutex for protecting sendPointer and subQueue operations
    std::pair<size_t, int> sendPointer;  // {index, message ID}
    
    // Mutex for thread safety
    std::mutex subQueueMutex;
    std::mutex mainQueueMutex;
    // Replace subQueue with unordered_set
    std::deque<int> subQueue;  // Subqueue for sliding window
    MessageSegments mainQueue;  // Represents the full queue

    std::mutex acknowledgedMessagesMutex;  // Mutex for accessing the ackQueue
    MessageSegments acknowledgedMessages;



    int loopCounter;


    std::condition_variable queueCv;  // Condition variable for notifying send threads about new messages
    std::vector<std::thread> sendThreads;  // Thread pool for sending messages
    // Add these members
    std::condition_variable sleepCv;  // Condition variable for sleeping
    std::mutex sleepMutex;            // Mutex to protect the condition variable
    std::atomic<bool> sleepThreads{false};  // Flag to signal sleep
    bool flagSleep;
    bool flagShrinkQueue;

    // Acknowledgment queue management
    // Add the unordered_map to maintain ack queues for each process
    std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> addressToProcessId;

    std::unordered_map<sockaddr_in, std::deque<int>, AddressHash, AddressEqual> processAckQueues; // Queue of acks by address

    std::mutex ackQueueMutex;  // Mutex for accessing the ackQueue
    std::condition_variable ackCv;  // Condition variable for ack queue

    // Separate logging queues for sender and receiver
    std::deque<int> senderLogQueue;  // Queue for storing message IDs (sender)
    std::deque<std::pair<int, int>> receiverLogQueue;  // Queue for storing <senderId, messageId> (receiver)
    
    std::mutex logMutex;  // Mutex for accessing the log queues
    std::condition_variable logCv;  // Condition variable for notifying log thread about new logs
    std::thread senderLogThread;  // Thread to handle sender logging
    std::thread receiverLogThread;  // Thread to handle receiver logging

    std::set<std::tuple<sockaddr_in, int, int>, MessageComparator> receivedQueue;

    std::mutex receivedQueueMutex;  // Mutex for thread safety
    std::condition_variable receivedQueueCv;

    std::vector<std::thread> ackThreads;  // Thread pool for acknowledgment sending

    // std::unordered_set<int> allMessageIds;  // IDs of messages that are being tracked (sent but not yet acknowledged)
    // // Mutex for protecting the access to allMessageIds
    // std::mutex allMsgMutex;

    // void performFaultCheck();

    // Function declarations
    void sendWorker();
    void senderLogWorker();
    // void receiverLogWorker();
    // void ackWorker();  // Function to handle acknowledgment sending
    // void deleteFromQueue(int messageId);
};
