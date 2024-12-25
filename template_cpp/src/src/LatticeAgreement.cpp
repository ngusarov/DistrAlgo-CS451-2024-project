#include "LatticeAgreement.hpp"
#include "Messages.hpp"
#include <iostream>

LatticeAgreement::LatticeAgreement(BEB* beb, int myId, int n, int f, std::ofstream& logFile, unsigned int p, unsigned int ds)
    : beb(beb), myId(myId), n(n), f(f), p(p), ds(ds), logFile(logFile),
      active_proposal_number(0), proposing(false), acksReceived(0), nacksReceived(0), decided(false)
{
    // accepted_values.resize(p);
    // If you want to pre-insert empty sets for keys 0..p-1:
    for (int i = 0; i < static_cast<int>(p); i++) {
        accepted_values[i] = std::unordered_set<int>(); // Insert empty set for key i
    } 
        
    beb->registerDeliveryCallback([this](const sockaddr_in& addr, const std::string& msg) {
        this->onMessageReceived(addr, msg);
    });
}

void LatticeAgreement::propose(int problem_number, const std::vector<int>& proposedValue){
    
    std::unique_lock<std::mutex> lock(mtx);

    current_problem_number = problem_number;

    active_proposal_number = 0;

    {
        std::unique_lock<std::mutex> lock(mtxProposedValue);
        // Start or restart a proposal round
        active_proposal_number++;
        proposed_value.clear();
        proposed_value.insert(proposedValue.begin(), proposedValue.end());

    }

    acksReceived = 0;
    nacksReceived = 0;
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

    if (pm.setSize == static_cast<int>(ds) && proposing) {
        std::unordered_set<int> valSet(pm.values.begin(), pm.values.end());
        decide(valSet);
    }

    int senderId = beb->getProcessId(senderAddr);

    std::stringstream sstream;
    sstream << "Received " << static_cast<int>(pm.type)  << " from " << senderId << " : " << msg;
    sstream << std::endl;
    std::cout << sstream.str();

    switch (pm.type) {
        case MessageType::PROPOSAL:
            handleProposal(senderId, pm.problem_number, pm.proposal_number, pm.values);
            break;
        case MessageType::ACK:
            beb->markResponded(senderId);
            handleAck(pm.problem_number, pm.proposal_number);
            break;
        case MessageType::NACK: {
            beb->markResponded(senderId);
            std::unordered_set<int> valSet(pm.values.begin(), pm.values.end());
            handleNack(pm.problem_number, pm.proposal_number, valSet);
            break;
        }
        default:
            break;
    }
}

void LatticeAgreement::handleProposal(int senderId, 
                                      int problem_number, 
                                      int proposal_number, 
                                      const std::vector<int>& proposedSet) 
{
    bool flagReply = true;
    if (problem_number > current_problem_number){
        flagReply = false;
    }


    // // 1) First, check if we have already ACKed this exact proposal
    // {
    //     std::lock_guard<std::mutex> lock(ackedProposalsMutex);
    //     auto ackKey = std::make_tuple(senderId, problem_number, proposal_number);
    //     if (ackedProposals.find(ackKey) != ackedProposals.end()) {
    //         // Already ACKed or handled => do nothing
    //         std::cout << "Already ACKed proposal from " << senderId 
    //                   << " problem=" << problem_number 
    //                   << " proposal_number=" << proposal_number << "\n";
    //         return;
    //     }
    // }

    
    std::stringstream sstream;
    sstream << "Handling PROPOSAL from " << senderId
            << " with proposal_number=" << proposal_number << "\n";
    std::cout << sstream.str();

    // We'll store local copies here (to use outside the lock)
    bool isSubsetFlag = false;
    std::unordered_set<int> newAcceptedValue;

    // ----- Lock only during read+update of accepted_values[problem_number] -----
    {
        std::lock_guard<std::mutex> lock(mtxAcceptedValue);
        
        // 1) Check if current accepted_value is subset of proposedSet
        isSubsetFlag = isSubset(accepted_values[problem_number], proposedSet);

        if (isSubsetFlag) {
            // If accepted_value is subset, we replace it entirely
            accepted_values[problem_number].clear();
            accepted_values[problem_number].insert(proposedSet.begin(), proposedSet.end());
        } else {
            // If not a subset, we union them
            unionSets(accepted_values[problem_number], proposedSet);
        }

        // Copy out the updated accepted_value so we can use it after unlocking
        newAcceptedValue = accepted_values[problem_number];
    } 
    // ---------------------- Lock is now released ----------------------

    if (!flagReply) return;

    if (isSubsetFlag) {
        std::cout << "It is subset; moving to ACK\n";
        if (senderId == myId) {
            // We call handleAck(...) directly if it's from ourselves
            handleAck(problem_number, proposal_number);
        } else {
            // Otherwise, we send an ACK message over the network
            sendAck(problem_number, proposal_number, senderId);
        }
    } else {

        // {
        //     std::lock_guard<std::mutex> lock(ackedProposalsMutex);
        //     auto ackKey = std::make_tuple(senderId, problem_number, proposal_number);
        //     ackedProposals.insert(ackKey);
        // }
        

        std::cout << "It is not subset; moving to NACK\n";
        if (senderId == myId) {
            // We call handleNack(...) directly with the new acceptedValue
            handleNack(problem_number, proposal_number, newAcceptedValue);
        } else {
            // Otherwise, we send NACK message
            sendNack(problem_number, proposal_number, senderId, newAcceptedValue);
        }
    }
}

void LatticeAgreement::handleAck(int problem_number, int proposal_number) {

    if (proposal_number != active_proposal_number 
        || problem_number != current_problem_number) return;

    acksReceived++;

    std::stringstream sstream;
    sstream << "Handling ACK" << " number " << proposal_number << "; acksReceived: " << acksReceived << "; nacksReceived: " << nacksReceived << "; proposing: " << proposing << "; decided: " << decided << ";";
    sstream << std::endl;
    std::cout << sstream.str();

    // If we get f+1 ACK and no NACK, decide proposed_value
    if (acksReceived >= f+1 && proposing) {
        decide(proposed_value);
    }
}

void LatticeAgreement::handleNack(int problem_number, int proposal_number, const std::unordered_set<int>& acceptedSet) {
    if (proposal_number != active_proposal_number
        || problem_number != current_problem_number) return;

    nacksReceived++;

    std::stringstream sstream;
    sstream << "Handling NACK" << " number " << proposal_number << "; acksReceived: " << acksReceived << "; nacksReceived: " << nacksReceived << "; proposing: " << proposing << "; decided: " << decided << ";";
    sstream << std::endl;
    // std::cout << sstream.str();

    
    {
        std::unique_lock<std::mutex> lock(mtxProposedValue);
        unionSets(proposed_value, acceptedSet);   
    }

    // sstream.clear();
    sstream << "New Proposed value: ";
    for (const auto& elem : proposed_value) {
        sstream << elem << " ";
    }
    sstream << std::endl;
    std::cout << sstream.str();

    if (nacksReceived > 0 && acksReceived+nacksReceived >= f+1 && proposing) {
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

    std::stringstream sstream;
    sstream << "DECIDED: ";
    for (auto elem : decidedSet) {
        sstream << elem << " ";
    }
    sstream << std::endl;
    std::cout << sstream.str();

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
    // 1. Serialize the proposal
    std::vector<int> v(proposed_value.begin(), proposed_value.end());
    std::string msg = serializeProposal(current_problem_number, active_proposal_number, v);

    // 2. "Deliver" the proposal to ourselves immediately
    {
        // Simulate receiving our own proposal as if it came from PerfectLinks/BEB
        // but with senderId = myId
        // We must parse it into 'pm' as usual
        ParsedMessage pm = parseMessage(msg);
        // Or skip parseMessage() and call handleProposal() directly if you prefer:
        // handleProposal(myId, active_proposal_number, v);

        // We'll do the parse step so that it's consistent with the rest of the flow:
        if (pm.type == MessageType::PROPOSAL) {
            handleProposal(myId, pm.problem_number, pm.proposal_number, pm.values);
        }
        // No else needed; we know we just serialized a PROPOSAL message
    }

    // 3. Start broadcasting to other processes
    beb->startBroadcast(msg);
}

void LatticeAgreement::sendAck(int problem_number, int proposal_number, int proposerId) {
    std::string msg = serializeAck(problem_number, proposal_number);
    beb->sendToProcess(msg, proposerId);
}

void LatticeAgreement::sendNack(int problem_number, int proposal_number, int proposerId, const std::unordered_set<int>& acceptedSet) {
    std::vector<int> v(acceptedSet.begin(), acceptedSet.end());
    std::string msg = serializeNack(problem_number, proposal_number, v);
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