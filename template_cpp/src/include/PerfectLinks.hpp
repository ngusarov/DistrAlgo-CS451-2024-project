#pragma once

#include <functional>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>

#include <Messages.hpp>


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

class PerfectLinks {
public:
    PerfectLinks(int sockfd, const sockaddr_in &myAddr, int myId, const std::unordered_map<int, sockaddr_in> &processIdToAddress,
                std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> &addressToProcessId);
    ~PerfectLinks(); // to ensure cleanup if needed

    // Send a message to a process identified by processId
    void sendMessage(int processId, const std::string &msg);

    // Register a callback to be called when a message is received
    // Callback signature: void(const sockaddr_in&, const std::string&)
    void registerDeliveryCallback(std::function<void(const sockaddr_in&, const std::string&)> cb);

    // Stop receiving and close resources
    void stop();

    int getProcessId(const sockaddr_in& addr);

private:

    int sockfd;
    sockaddr_in myAddr;
    int myId;
    std::unordered_map<int, sockaddr_in> processIdToAddress;
    std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> addressToProcessId;

    std::function<void(const sockaddr_in&, const std::string&)> deliveryCallback;
    std::mutex callbackMutex;

    std::atomic<bool> running;
    std::thread receiverThread;


private:
    void receiverLoop();

    // === Fragmentation ===
    void sendLargeProposalOrNack(int processId, const ParsedMessage& pm);
    void reallySend(int processId, const std::string &msg); // single sendto call

    // === Defragmentation buffer ===
    // Key for partial data: (senderId, type, problem_number, proposal_number)
    struct FragKey {
        int senderId;
        MessageType type;
        int problem;
        int proposal;
        // operator< or hashing for storage in map or unordered_map
        bool operator<(const FragKey &o) const {
            if (senderId != o.senderId) return senderId < o.senderId;
            if (type != o.type) return static_cast<int>(type) < static_cast<int>(o.type);
            if (problem != o.problem) return problem < o.problem;
            return proposal < o.proposal;
        }
    };

    // Partial data stored here while we accumulate enough elements to match setSize
    struct FragBuffer {
        int setSize = 0;                  // total # of elements expected
        std::unordered_set<int> elements; // or use vector<int> if duplicates/order matter
    };

    // For defragmentation
    std::mutex fragMutex;
    std::map<FragKey, FragBuffer> fragMap; // store partial sets here

    void handleIncoming(const sockaddr_in& srcAddr, const std::string& packet);
    void deliverUp(const sockaddr_in& srcAddr, const std::string& msg);
};