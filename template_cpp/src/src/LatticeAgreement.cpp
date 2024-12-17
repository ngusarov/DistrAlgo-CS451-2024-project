#include "LatticeAgreement.hpp"
#include "Messages.hpp"
#include <iostream>

LatticeAgreement::LatticeAgreement(BEB* beb, int myId, int n, int f, std::ofstream& logFile)
    : beb(beb), myId(myId), n(n), f(f), logFile(logFile),
      active_proposal_number(0), proposing(false), acksReceived(0), nacksReceived(0), decided(false),
      current_proposal_number(-1), proposal_origin(-1)
{
    beb->registerDeliveryCallback([this](const sockaddr_in& addr, const std::string& msg) {
        this->onMessageReceived(addr, msg);
    });
}

void LatticeAgreement::propose(const std::vector<int>& proposedValue) {
    std::unique_lock<std::mutex> lock(mtx);
    // Start or restart a proposal round
    active_proposal_number++;
    active_value.clear();
    active_value.insert(proposedValue.begin(), proposedValue.end());

    acksReceived = 0;
    nacksReceived = 0;
    mergedNackValue.clear();
    proposing = true;
    decided = false;

    sendProposal();

    // Wait until decided
    cv.wait(lock, [this](){ return decided; });
}

void LatticeAgreement::onMessageReceived(const sockaddr_in& senderAddr, const std::string& msg) {
    ParsedMessage pm = parseMessage(msg);
    if (pm.type == MessageType::UNKNOWN) {
        return;
    }

    int senderId = beb->getProcessId(senderAddr);

    std::unique_lock<std::mutex> lock(mtx);

    switch (pm.type) {
        case MessageType::PROPOSAL:
            handleProposal(senderId, pm.proposal_number, pm.values);
            break;
        case MessageType::ACK:
            handleAck(senderId, pm.proposal_number);
            break;
        case MessageType::NACK: {
            std::unordered_set<int> valSet(pm.values.begin(), pm.values.end());
            handleNack(senderId, pm.proposal_number, valSet);
            break;
        }
        default:
            break;
    }
}

void LatticeAgreement::handleProposal(int senderId, int proposal_number, const std::vector<int>& proposedSet) {

    // Same proposal_number again, check subset
    if (isSubset(accepted_value, proposedSet)) {
        accepted_value = proposedSet;
        // accepted_value is subset of proposedSet, ACK again
        sendAck(proposal_number, proposal_origin);
    } else {
        // not a subset, union and NACK
        unionSets(accepted_value, proposedSet); // changes accepted Set
        sendNack(proposal_number, proposal_origin, accepted_value);
    }
    
}

void LatticeAgreement::handleAck(int senderId, int proposal_number) {
    if (proposal_number != active_proposal_number) return;

    acksReceived++;
    // If we get f+1 ACK and no NACK, decide active_value
    if (acksReceived >= f+1 && proposing) {
        decide(active_value);
    }
}

void LatticeAgreement::handleNack(int senderId, int proposal_number, const std::unordered_set<int>& acceptedSet) {
    if (!proposing || proposal_number != active_proposal_number) return;

    nacksReceived++;

    unionSets(active_value, acceptedSet);    

    if (nacksReceived > 0 && acksReceived+nacksReceived >= f+1) {
        active_proposal_number++;
        acksReceived = 0;
        nacksReceived = 0;
        sendProposal();
    }
}

void LatticeAgreement::decide(const std::unordered_set<int>& decidedValue) {
    decided = true;
    proposing = false;
    decidedSet = decidedValue;

    // Construct the line in memory
    std::ostringstream oss;
    for (auto elem : decidedSet) {
        oss << elem << " ";
    }
    std::string line = oss.str();
    if (!line.empty() && line.back() == ' ') {
        line.pop_back();
    }

    decidedLines.push_back(line);

    // If we have more than 5 lines, write them out now
    if (decidedLines.size() > 5) {
        flushBufferedLines(); // Write all lines to file now
    }

    cv.notify_all();
}

void LatticeAgreement::flushDecisions() {
    // Called from stop()
    flushBufferedLines();
}

void LatticeAgreement::flushBufferedLines() {
    // Write each line fully, then flush to ensure no partial lines
    for (const auto& line : decidedLines) {
        logFile << line << "\n";
        logFile.flush(); // Flush after each line
    }
    decidedLines.clear();
}

void LatticeAgreement::sendProposal() {
    std::vector<int> v(active_value.begin(), active_value.end());
    std::string msg = serializeProposal(active_proposal_number, v);
    beb->broadcast(msg);
}

void LatticeAgreement::sendAck(int proposal_number, int proposerId) {
    std::string msg = serializeAck(proposal_number);
    beb->sendToProcess(msg, proposerId);
}

void LatticeAgreement::sendNack(int proposal_number, int proposerId, const std::unordered_set<int>& acceptedSet) {
    std::vector<int> v(acceptedSet.begin(), acceptedSet.end());
    std::string msg = serializeNack(proposal_number, v);
    beb->sendToProcess(msg, proposerId);
}

void LatticeAgreement::unionSets(std::unordered_set<int>& baseSet, const std::vector<int>& toAdd) {
    for (int x : toAdd) {
        baseSet.insert(x);
    }
}

void LatticeAgreement::unionSets(std::unordered_set<int>& baseSet, const std::unordered_set<int>& toAdd) {
    for (int x : toAdd) {
        baseSet.insert(x);
    }
}

bool LatticeAgreement::isSubset(const std::unordered_set<int>& A, const std::vector<int>& B) {
    // Check if A ⊆ B
    // A subset B if every element of A is in B
    // Convert B into a set for O(1) membership checks
    std::unordered_set<int> Bset(B.begin(), B.end());
    for (int x : A) {
        if (Bset.find(x) == Bset.end()) {
            return false;
        }
    }
    return true;
}