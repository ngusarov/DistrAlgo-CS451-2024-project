#pragma once
#include <string>
#include <vector>

enum class MessageType { PROPOSAL, ACK, NACK, UNKNOWN };

struct ParsedMessage {
    MessageType type;
    int problem_number;
    int proposal_number;
    int setSize;
    std::vector<int> values; // used for proposals (P) and nacks (N)
};

// Serialize functions
std::string serializeProposal(int problem_number, int proposal_number, const std::vector<int>& values);
std::string serializeAck(int problem_number, int proposal_number);
std::string serializeNack(int problem_number, int proposal_number, const std::vector<int>& accepted_values);

// Parse function
ParsedMessage parseMessage(const std::string& msg);