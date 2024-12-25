#include "PerfectLinks.hpp"
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>
#include <chrono>

PerfectLinks::PerfectLinks(int sockfd, const sockaddr_in &myAddr, int myId, const std::unordered_map<int, sockaddr_in> &processIdToAddress,
            std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> &addressToProcessId)
    : sockfd(sockfd), myAddr(myAddr), myId(myId), processIdToAddress(processIdToAddress),
                             addressToProcessId(addressToProcessId), running(true)
{
    // Start the receiver thread
    receiverThread = std::thread(&PerfectLinks::receiverLoop, this);
}

PerfectLinks::~PerfectLinks() {
    stop();
    if (receiverThread.joinable()) {
        receiverThread.join();
    }
}

int PerfectLinks::getProcessId(const sockaddr_in& addr) {
    auto it = addressToProcessId.find(addr);
    if (it != addressToProcessId.end()) {
        return it->second;
    }
    return -1; // or some invalid ID if not found
}

void PerfectLinks::registerDeliveryCallback(std::function<void(const sockaddr_in&, const std::string&)> cb) {
    std::lock_guard<std::mutex> lock(callbackMutex);
    deliveryCallback = cb;
}

void PerfectLinks::stop() {
    running = false;
}


// ============= Sending =============
void PerfectLinks::sendMessage(int processId, const std::string &msg) {
    // Parse
    ParsedMessage pm = parseMessage(msg);
    // If it's ACK or has <= 8 elements, just send as-is
    if (pm.type == MessageType::ACK) {
        reallySend(processId, msg);
        return;
    }

    // If it's P or N but has <= 8 elements, also send as-is
    if ((pm.type == MessageType::PROPOSAL || pm.type == MessageType::NACK) &&
        pm.values.size() <= 8)
    {
        reallySend(processId, msg);
        return;
    }

    // Otherwise it's P or N with more than 8 data elements => fragment
    if (pm.type == MessageType::PROPOSAL || pm.type == MessageType::NACK) {
        sendLargeProposalOrNack(processId, pm);
    }
}

void PerfectLinks::reallySend(int processId, const std::string &msg) {
    auto it = processIdToAddress.find(processId);
    if (it == processIdToAddress.end()) {
        std::cerr << "Unknown processId " << processId << " in sendMessage.\n";
        return;
    }
    const sockaddr_in &destAddr = it->second;

    // std::stringstream sstream;
    // sstream << "[PL] sent: " << processId << " : " << msg << "\n";
    // std::cout << sstream.str();

    ssize_t sent_bytes = sendto(sockfd, msg.c_str(), msg.size(), 0,
                                reinterpret_cast<const struct sockaddr*>(&destAddr),
                                sizeof(destAddr));
    if (sent_bytes < 0) {
        perror("sendto failed");
    }
}

void PerfectLinks::sendLargeProposalOrNack(int processId, const ParsedMessage& pm) {
    // pm.values.size() > 8
    // We'll send multiple partial messages, each with up to 8 of pm.values
    // Keep pm.type as 'P' or 'N'
    // Keep pm.problem_number, pm.proposal_number, pm.setSize the same in each partial
    // but only put up to 8 from pm.values in each sub-message.

    const int maxElemsPerPacket = 8;
    int total = static_cast<int>(pm.values.size());
    int startIndex = 0;

    while (startIndex < total) {
        int endIndex = std::min(startIndex + maxElemsPerPacket, total);
        std::vector<int> subVec(pm.values.begin()+startIndex, pm.values.begin()+endIndex);

        // Rebuild a message string
        std::string subMsg;
        if (pm.type == MessageType::PROPOSAL) {
            subMsg = serializeSubProposal(pm.problem_number, pm.proposal_number, total, subVec);
        } else {
            // NACK
            subMsg = serializeSubNack(pm.problem_number, pm.proposal_number, total, subVec);
        }

        reallySend(processId, subMsg);
        startIndex = endIndex;
    }
}



// ============= Receiving =============
void PerfectLinks::receiverLoop() {
    char buffer[16384];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer)-1, 0,
                               reinterpret_cast<struct sockaddr*>(&srcAddr),
                               &srcAddrLen);
        if (len < 0) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
            continue;
        }
        buffer[len] = '\0';
        std::string receivedPacket(buffer);

        // We do the partial reassembly if needed
        handleIncoming(srcAddr, receivedPacket);
    }
}

static std::function<void(const sockaddr_in&, const std::string&)> noCallback = nullptr;

void PerfectLinks::handleIncoming(const sockaddr_in& srcAddr, const std::string& packet) {


    ParsedMessage pm = parseMessage(packet);
    if (pm.type == MessageType::UNKNOWN) {
        // unknown format, ignore
        return;
    }

    // If ACK => deliver immediately
    if (pm.type == MessageType::ACK) {
        deliverUp(srcAddr, packet); // no fragmentation
        return;
    }

    // If P or N => accumulate in fragMap until we have >= setSize
    int senderId = getProcessId(srcAddr);
    if (senderId < 0) {
        // unknown sender, ignore or handle error
        return;
    }

    // std::stringstream sstream;
    // sstream << "[PL] received: " << senderId << " : " << packet << "\n";
    // std::cout << sstream.str();

    FragKey key{senderId, pm.type, pm.problem_number, pm.proposal_number};

    {
        std::lock_guard<std::mutex> lock(fragMutex);

        auto& frag = fragMap[key];
        if (frag.setSize == 0) {
            // first time we see this key
            frag.setSize = pm.setSize; 
        }
        // Insert new elements into frag.elements
        for (int val : pm.values) {
            frag.elements.insert(val);
        }

        // std::stringstream sstream;
        // sstream << "Current fragment: " << static_cast<int>(pm.values.size()) << " out of " << frag.setSize << "\n";
        // std::cout << sstream.str();

        // If we have enough elements, produce final message
        if (static_cast<int>(frag.elements.size()) >= frag.setSize) {
            // Build a single string as if it was one big "P:problem:proposal:setSize:elem1:..."
            std::vector<int> allElems(frag.elements.begin(), frag.elements.end());

            std::string finalMsg;
            if (pm.type == MessageType::PROPOSAL) {
                finalMsg = serializeProposal(pm.problem_number, pm.proposal_number, allElems);
            } else { 
                finalMsg = serializeNack(pm.problem_number, pm.proposal_number, allElems);
            }

            // Deliver finalMsg up
            deliverUp(srcAddr, finalMsg);

            // Remove from fragMap
            fragMap.erase(key);
        }
    }
}

void PerfectLinks::deliverUp(const sockaddr_in& srcAddr, const std::string& msg) {
    std::function<void(const sockaddr_in&, const std::string&)> cbCopy;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        cbCopy = deliveryCallback;
    }
    if (cbCopy) {
        cbCopy(srcAddr, msg);
    }
}