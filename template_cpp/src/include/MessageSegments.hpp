#pragma once

#include <iostream>
#include <set>
#include <utility>
#include <vector>

class MessageSegments {
public:
    void addMessage(int message) {
        addSegment({message, message});
    }

    void addSegment(std::pair<int, int> newSegment) {
        auto it = segments.lower_bound(newSegment);
        if (it != segments.begin()) {
            --it;
        }

        while (it != segments.end() && it->first <= newSegment.second + 1) {
            if (it->second < newSegment.first - 1) {
                ++it;
                continue;
            }
            newSegment.first = std::min(newSegment.first, it->first);
            newSegment.second = std::max(newSegment.second, it->second);
            it = segments.erase(it);
        }

        segments.insert(newSegment);
    }

    void deleteMessage(int message) {
        deleteSegment({message, message});
    }

    void deleteSegment(std::pair<int, int> delSegment) {
        auto it = segments.lower_bound(delSegment);
        if (it != segments.begin()) {
            --it;
        }

        std::vector<std::pair<int, int>> toAdd;

        while (it != segments.end() && it->first <= delSegment.second) {
            if (it->second < delSegment.first) {
                ++it;
                continue;
            }

            auto current = *it;
            it = segments.erase(it);

            if (current.first < delSegment.first) {
                toAdd.emplace_back(current.first, delSegment.first - 1);
            }

            if (current.second > delSegment.second) {
                toAdd.emplace_back(delSegment.second + 1, current.second);
            }
        }

        for (const auto& seg : toAdd) {
            segments.insert(seg);  // Direct insertion, no merging needed.
        }
    }

    bool find(int message) const {
        auto it = segments.lower_bound({message, message});
        
        // If the iterator points to a segment starting after the message or we're at the end
        if (it == segments.end() || it->first > message) {
            // Check the previous segment, as it could include the message
            if (it != segments.begin()) {
                --it;
                if (it->first <= message && it->second >= message) {
                    return true;  // Message found within this segment
                }
            }
            return false;  // Message not found
        }

        // Check if the message is in the current segment
        return it->first <= message && it->second >= message;
    }

    int getNextMessage() {
        if (segments.empty()) {
            return -1;  // No more messages in the queue
        }

        auto& front = *segments.begin();  // Access the first segment
        int nextMessage = front.first;

        auto it = segments.begin();
        if (it != segments.end()) {
            std::pair<int, int> updatedSegment = *it;  // Copy the segment
            ++updatedSegment.first;                    // Increment the left boundary

            // Validate the updated segment before proceeding
            if (updatedSegment.first <= updatedSegment.second) {
                segments.erase(it);  // Remove the old segment
                segments.insert(updatedSegment);  // Insert the updated segment
            } else {
                segments.erase(it);  // Simply remove the segment if it's no longer valid
            }
        }

        return nextMessage;
    }


    void print() const {
        for (const auto& seg : segments) {
            std::cout << "[" << seg.first << ", " << seg.second << "] ";
        }
        std::cout << "\n";
    }

    const std::set<std::pair<int, int>>& getSegments() const {
        return segments;
    }

private:
    std::set<std::pair<int, int>> segments;  // Maintains sorted, non-overlapping segments
};