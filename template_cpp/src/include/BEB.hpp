#pragma once

#include <functional>
#include <vector>
#include <netinet/in.h>
#include <string>
#include <mutex>
#include <sstream>
#include <iostream>
#include <unordered_set>

#include "PerfectLinks.hpp"

class BEB {
public:
    // Constructor takes a pointer to PerfectLinks and the list of process IDs.
    BEB(PerfectLinks* pl, const std::vector<int>& processIds, int myId);

    void startBroadcast(const std::string& newMsg);

    // Stop the background broadcasting (if it is running).
    void stopBroadcast();

    // Send a message to a specific process
    void sendToProcess(const std::string& msg, int processId);

    // Register a callback to handle message deliveries
    // The callback signature: void(const sockaddr_in &srcAddr, const std::string &message)
    void registerDeliveryCallback(std::function<void(const sockaddr_in&, const std::string&)> cb);

    int getProcessId(const sockaddr_in& addr);

    // Helper to see if a process has responded
    bool hasResponded(int pid);
    void markResponded(int pid);




private:

    // The broadcasting loop. This runs in a separate thread.
    void broadcastLoop();

    // This method will be registered as a callback in PerfectLinks.
    // When PerfectLinks delivers a message, this method is called and then we forward to our callback.
    void onMessageReceived(const sockaddr_in& srcAddr, const std::string& message);

    PerfectLinks* pl;
    std::vector<int> processIds;
    int myId;

    std::function<void(const sockaddr_in&, const std::string&)> deliveryCallback;
    std::mutex callbackMutex; // Protects access to deliveryCallback

    // Track which processes have responded
    std::unordered_set<int> respondedSet;
    std::mutex respondedMutex;

    // Variables for the background broadcast logic
    std::thread broadcastThread;
    std::atomic<bool> runningBroadcast {false};  // Indicates if the broadcast loop is running
    std::mutex broadcastMutex;                   // Protects broadcastMsg, respondedSet, etc.
    std::string broadcastMsg;                    // The message we broadcast in a loop
};