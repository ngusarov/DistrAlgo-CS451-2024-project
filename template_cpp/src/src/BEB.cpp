#include "BEB.hpp"
#include <algorithm> // for std::find

BEB::BEB(PerfectLinks* pl, const std::vector<int>& processIds, int myId)
    : pl(pl), processIds(processIds), myId(myId) 
{
    // Register a callback with PerfectLinks so that when it receives a message,
    // we get notified and can forward it to the Lattice Agreement or higher-level layer.
    pl->registerDeliveryCallback([this](const sockaddr_in &src, const std::string &msg) {
        this->onMessageReceived(src, msg);
    });
}

void BEB::startBroadcast(const std::string& newMsg) {
    // If a broadcast loop is already running, stop it first
    stopBroadcast();

    // Set the new message
    {
        std::lock_guard<std::mutex> lock(broadcastMutex);
        broadcastMsg = newMsg;
        
        // Clear respondedSet so we broadcast to all again,
        // or keep it if you want partial memory from previous runs.
        respondedSet.clear(); 
    }

    // Start a new broadcasting thread
    runningBroadcast = true;
    broadcastThread = std::thread(&BEB::broadcastLoop, this);
}

void BEB::stopBroadcast() {
    // Signal the broadcast loop to stop
    runningBroadcast = false;

    // Join if the thread is running
    if (broadcastThread.joinable()) {
        broadcastThread.join();
    }
}

void BEB::broadcastLoop() {
    // Keep sending broadcastMsg until all processes respond or we stop
    while (runningBroadcast) {
        bool allResponded = true;

        for (int pid : processIds) {
            if (pid == myId) continue;

            if (!hasResponded(pid)) {
                allResponded = false;

                // Prepare debug print
                std::stringstream sstream;
                sstream << "[BEB] Broadcasting to PID " << pid 
                        << " : " << broadcastMsg << std::endl;
                std::cout << sstream.str();

                // Send
                pl->sendMessage(pid, broadcastMsg);
            }
        }

        if (allResponded) {
            // Everyone responded. We can stop this broadcast session
            break;
        }

        // Sleep to avoid flooding
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // If stopBroadcast() was called, runningBroadcast becomes false
        if (!runningBroadcast) break;
    }

    // Once we exit the loop, broadcasting is done
    runningBroadcast = false;
}

void BEB::markResponded(int pid) {
    // This is called by LatticeAgreement when it gets ACK/NACK from 'pid'
    std::lock_guard<std::mutex> lock(respondedMutex);
    respondedSet.insert(pid);
}

bool BEB::hasResponded(int pid) {
    std::lock_guard<std::mutex> lock(respondedMutex);
    return (respondedSet.find(pid) != respondedSet.end());
}


void BEB::sendToProcess(const std::string& msg, int processId) {
    std::stringstream sstream;
    sstream << "Sending " << processId << " : " << msg;
    sstream << std::endl;
    std::cout << sstream.str();
    pl->sendMessage(processId, msg);
}

void BEB::registerDeliveryCallback(std::function<void(const sockaddr_in&, const std::string&)> cb) {
    std::lock_guard<std::mutex> lock(callbackMutex);
    deliveryCallback = cb;
}

void BEB::onMessageReceived(const sockaddr_in& srcAddr, const std::string& message) {
    std::function<void(const sockaddr_in&, const std::string&)> cbCopy;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        cbCopy = deliveryCallback;
    }
    // If a callback is registered, call it
    if (cbCopy) {
        cbCopy(srcAddr, message);
    }
}

int BEB::getProcessId(const sockaddr_in& addr) {
    return pl->getProcessId(addr);
}