#pragma once

#include <unordered_set>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <netinet/in.h>

#include "BEB.hpp"
#include "Messages.hpp"

class LatticeAgreement {
public:
    LatticeAgreement(BEB* beb, int myId, int n, int f, std::ofstream& logFile);

    // Propose a set of integers. Blocks until a decision is reached.
    void propose(const std::vector<int>& proposedValue);

    // Called by BEB when a message is delivered.
    void onMessageReceived(const sockaddr_in& senderAddr, const std::string& msg);

private:
    // Acceptor and Proposer handlers
    void handleProposal(int senderId, int proposal_number, const std::vector<int>& proposedSet);
    void handleAck(int senderId, int proposal_number);
    void handleNack(int senderId, int proposal_number, const std::unordered_set<int>& acceptedSet);

    // Internal methods
    void decide(const std::unordered_set<int>& decidedValue);
    void sendProposal();
    void sendAck(int proposal_number, int proposerId);
    void sendNack(int proposal_number, int proposerId, const std::unordered_set<int>& acceptedSet);

    // Set operations
    void unionSets(std::unordered_set<int>& baseSet, const std::vector<int>& toAdd);
    void unionSets(std::unordered_set<int>& baseSet, const std::unordered_set<int>& toAdd);
    bool isSubset(const std::unordered_set<int>& A, const std::vector<int>& B);

private:
    BEB* beb;
    int myId;
    int n;
    int f;
    std::ofstream& logFile;

    // Proposer state
    int active_proposal_number;
    std::unordered_set<int> active_value;
    bool proposing;
    int acksReceived;
    int nacksReceived;
    std::unordered_set<int> mergedNackValue;

    bool decided;
    std::unordered_set<int> decidedSet;

    std::mutex mtx;
    std::condition_variable cv;

    // Acceptor state:
    int current_proposal_number;            // highest proposal_number seen
    std::unordered_set<int> accepted_value; // accepted_value for that proposal_number
    int proposal_origin;                    // who proposed current_proposal_number

    std::vector<std::string> decidedLines; // Stores decided lines in memory
};