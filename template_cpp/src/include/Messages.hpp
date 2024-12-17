#pragma once
#include <string>
#include <vector>

enum class MessageType { PROPOSAL, ACK, NACK, UNKNOWN };

struct ParsedMessage {
    MessageType type;
    int proposal_number;
    std::vector<int> values; // used for proposals (P) and nacks (N)
};

// Serialize functions
std::string serializeProposal(int proposal_number, const std::vector<int>& values);
std::string serializeAck(int proposal_number);
std::string serializeNack(int proposal_number, const std::vector<int>& accepted_values);

// Parse function
ParsedMessage parseMessage(const std::string& msg);