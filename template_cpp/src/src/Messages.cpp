#include "Messages.hpp"
#include <sstream>
#include <iostream>
#include <algorithm>

std::string serializeProposal(int proposal_number, const std::vector<int>& values) {
    // Format: P:<proposal_number>:<setSize>:<elem1>:...:<elemN>
    std::ostringstream oss;
    oss << "P:" << proposal_number << ":" << values.size();
    for (int val : values) {
        oss << ":" << val;
    }
    return oss.str();
}

std::string serializeAck(int proposal_number) {
    // Format: A:<proposal_number>
    std::ostringstream oss;
    oss << "A:" << proposal_number;
    return oss.str();
}

std::string serializeNack(int proposal_number, const std::vector<int>& accepted_values) {
    // Format: N:<proposal_number>:<setSize>:<elem1>:...:<elemM>
    std::ostringstream oss;
    oss << "N:" << proposal_number << ":" << accepted_values.size();
    for (int val : accepted_values) {
        oss << ":" << val;
    }
    return oss.str();
}

ParsedMessage parseMessage(const std::string& msg) {
    // Split by ':'
    ParsedMessage pm;
    pm.type = MessageType::UNKNOWN;
    pm.proposal_number = -1;
    pm.values.clear();

    if (msg.empty()) return pm;

    std::vector<std::string> parts;
    {
        std::istringstream iss(msg);
        std::string token;
        while (std::getline(iss, token, ':')) {
            parts.push_back(token);
        }
    }

    if (parts.empty()) return pm;

    char messageTypeChar = parts[0].empty() ? '\0' : parts[0][0];

    switch (messageTypeChar) {
        case 'P': {
            // P:<proposal_number>:<setSize>:<elem1>:...:<elemN>
            // parts[0] = "P"
            if (parts.size() < 3) return pm; // Need at least P, proposal_number, setSize
            pm.type = MessageType::PROPOSAL;
            pm.proposal_number = std::stoi(parts[1]);
            int setSize = std::stoi(parts[2]);
            int expectedSize = 3 + setSize;
            if ((int)parts.size() < expectedSize) return pm; // Not enough elements
            for (int i = 0; i < setSize; i++) {
                pm.values.push_back(std::stoi(parts[3 + i]));
            }
            break;
        }
        case 'A': {
            // A:<proposal_number>
            // parts[0] = "A"
            if (parts.size() < 2) return pm; 
            pm.type = MessageType::ACK;
            pm.proposal_number = std::stoi(parts[1]);
            break;
        }
        case 'N': {
            // N:<proposal_number>:<setSize>:<elem1>:...:<elemM>
            if (parts.size() < 3) return pm;
            pm.type = MessageType::NACK;
            pm.proposal_number = std::stoi(parts[1]);
            int setSize = std::stoi(parts[2]);
            int expectedSize = 3 + setSize;
            if ((int)parts.size() < expectedSize) return pm;
            for (int i = 0; i < setSize; i++) {
                pm.values.push_back(std::stoi(parts[3 + i]));
            }
            break;
        }
        default:
            // Unknown message type
            break;
    }

    return pm;
}