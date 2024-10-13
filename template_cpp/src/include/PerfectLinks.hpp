#pragma once

#include <unordered_set>     // For unordered_set
#include <unordered_map>     // For unordered_map
#include <queue>             // For queue
#include <mutex>             // For mutex and lock_guard
#include <condition_variable> // For condition_variable
#include <thread>            // For threads
#include <atomic>            // For atomic<bool>
#include <netinet/in.h>      // For sockaddr_in and related network functions
#include <sys/socket.h>      // For socket functions (sendto, recvfrom)
#include <arpa/inet.h>       // For inet_ntoa
#include <iostream>          // For input/output streams (std::cout, std::endl)
#include <fstream>           // For ofstream (log file)
#include <functional>        // For std::hash (used in PairHash)
#include <utility>           // For std::pair>
#include <sstream>           // For std::istringstream

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
    void receiveMessages();          // Receiver logic: receive messages and send acknowledgments
    void receiveAcknowledgments();   // Sender logic: receive acknowledgment messages
    void startReceivingAcks();       // Start acknowledgment threads
    void stopDelivering();

    int packetSize;

private:
    int sockfd;  // The socket file descriptor used for communication
    sockaddr_in myAddr;  // The address of this process
    std::ofstream& logFile;  // Output file for logging
    int myProcessId;  // ID of the current process

    // Mutex and condition variables for managing acknowledgment tracking
    std::mutex ackMutex;
    std::condition_variable ackCv;

    // Data structures for storing state
    std::unordered_map<int, bool> acknowledgments;  // Keeps track of acknowledgments by message ID
    std::unordered_set<std::pair<int, int>, PairHash> deliveredMessages;  // Keeps track of delivered message pairs (processId, messageId)
    std::mutex deliveryMutex;  // Protects the deliveredMessages set
    std::atomic<bool> running{true};  // Flag to indicate if the deliver thread should keep running

    // Thread management
    std::queue<std::pair<sockaddr_in, std::pair<std::string, int>>> messageQueue;  // Queue of messages to be sent
    std::queue<std::pair<sockaddr_in, std::string>> ackQueue;  // Queue of acknowledgments to be sent
    std::mutex queueMutex;  // Mutex for accessing the message queue
    std::mutex ackQueueMutex;  // Mutex for accessing the acknowledgment queue
    std::vector<std::thread> sendThreads;  // Thread pool for sending messages
    std::vector<std::thread> ackThreads;   // Thread pool for sending acknowledgments

    // Function declarations
    void sendWorker();
    void ackWorker();
    void logBroadcast(int messageId);
    void logDelivery(int messageId, int processId);
};
