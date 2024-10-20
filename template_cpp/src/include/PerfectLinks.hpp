#pragma once

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

// Custom hash function for pairs of ints
struct PairHash {
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2>& pair) const {
        std::size_t h1 = std::hash<T1>()(pair.first);
        std::size_t h2 = std::hash<T2>()(pair.second);
        return h1 ^ (h2 << 1); // Combine the two hash values
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

    int packetSize;  // Add packet size as a public member variable
    bool isReceiver;

    bool doneLogging;

public:
    int sockfd;  // The socket file descriptor used for communication
    sockaddr_in myAddr;  // The address of this process
    std::ofstream& logFile;  // Output file for logging
    int myProcessId;  // ID of the current process

    // Mutex and condition variables for managing acknowledgment tracking
    std::mutex ackMutex;

    // Data structures for storing state
    std::unordered_set<int> acknowledgments;  // Keeps track of acknowledgment counters by message ID
    std::unordered_set<std::pair<int, int>, PairHash> deliveredMessages;  // Keeps track of delivered message pairs (processId, messageId)
    std::mutex deliveryMutex;  // Protects the deliveredMessages set
    std::atomic<bool> running{true};  // Flag to indicate if the deliver thread should keep running

    std::unordered_map<int, int> messageMap;
    std::mutex messageMapMutex;

    // Message queue management
    std::deque<std::pair<sockaddr_in, std::pair<std::string, int>>> messageQueue;  // Queue of messages to be sent
    std::mutex queueMutex;  // Mutex for accessing the message queue
    std::condition_variable queueCv;  // Condition variable for notifying send threads about new messages
    std::vector<std::thread> sendThreads;  // Thread pool for sending messages

    // Acknowledgment queue management
    // Add the unordered_map to maintain ack queues for each process
    std::unordered_map<int, std::deque<std::tuple<sockaddr_in, int, int>>> processAckQueues;
    std::mutex ackQueueMutex;  // Mutex for accessing the ackQueue
    std::condition_variable ackCv;  // Condition variable for ack queue

    // Separate logging queues for sender and receiver
    std::deque<int> senderLogQueue;  // Queue for storing message IDs (sender)
    std::deque<std::pair<int, int>> receiverLogQueue;  // Queue for storing <senderId, messageId> (receiver)
    
    std::mutex logMutex;  // Mutex for accessing the log queues
    std::condition_variable logCv;  // Condition variable for notifying log thread about new logs
    std::thread senderLogThread;  // Thread to handle sender logging
    std::thread receiverLogThread;  // Thread to handle receiver logging

    std::vector<std::thread> ackThreads;  // Thread pool for acknowledgment sending

    // std::unordered_set<int> allMessageIds;  // IDs of messages that are being tracked (sent but not yet acknowledged)
    // // Mutex for protecting the access to allMessageIds
    // std::mutex allMsgMutex;

    // void performFaultCheck();

    // Function declarations
    void sendWorker();
    void senderLogWorker();
    void receiverLogWorker();
    void ackWorker();  // Function to handle acknowledgment sending
};
