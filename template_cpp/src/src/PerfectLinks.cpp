#include "PerfectLinks.hpp"
#include <iostream>
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

void PerfectLinks::sendMessage(int processId, const std::string &msg) {
    auto it = processIdToAddress.find(processId);
    if (it == processIdToAddress.end()) {
        std::cerr << "Unknown processId " << processId << " in sendMessage.\n";
        return;
    }

    const sockaddr_in &destAddr = it->second;
    ssize_t sent_bytes = sendto(sockfd, msg.c_str(), msg.size(), 0,
                                reinterpret_cast<const struct sockaddr*>(&destAddr), sizeof(destAddr));
    if (sent_bytes < 0) {
        perror("sendto failed");
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

void PerfectLinks::receiverLoop() {
    char buffer[16384];
    struct sockaddr_in srcAddr;
    socklen_t srcAddrLen = sizeof(srcAddr);

    while (running) {
        ssize_t len = recvfrom(sockfd, buffer, sizeof(buffer)-1, 0, reinterpret_cast<struct sockaddr*>(&srcAddr), &srcAddrLen);
        if (len < 0) {
            // Non-blocking mode: if no data, just sleep briefly
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        buffer[len] = '\0';
        std::string receivedPacket(buffer);

        std::function<void(const sockaddr_in&, const std::string&)> cbCopy;
        {
            std::lock_guard<std::mutex> lock(callbackMutex);
            cbCopy = deliveryCallback;
        }

        if (cbCopy) {
            cbCopy(srcAddr, receivedPacket);
        }
    }
}