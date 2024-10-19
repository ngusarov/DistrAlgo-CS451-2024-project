#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>
#include <fstream>
#include <fcntl.h>  // Include this to use F_GETFL, F_SETFL, O_NONBLOCK


#include "parser.hpp"
#include "hello.h"
#include "PerfectLinks.hpp"

// Declare the global PerfectLinks object
PerfectLinks* pl = nullptr;  // Initially a nullptr

// Signal handler for stopping the process and flushing logs
static void stop(int) {
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);

    std::cout << "Immediately stopping network packet processing.\n";
    std::cout << "Writing output.\n";

    if (pl != nullptr) {
        // Print deliveredMessages to a new file for testing purposes
        std::ofstream deliveredMessagesFile("deliveredMessages.txt");
        if (deliveredMessagesFile.is_open()) {
            std::lock_guard<std::mutex> lock(pl->deliveryMutex);  // Protect deliveredMessages
            for (const auto& msg : pl->deliveredMessages) {
                deliveredMessagesFile << "d "
                                      << msg.first << " "
                                      << msg.second << "\n";
            }
            deliveredMessagesFile.close();
        } else {
            std::cerr << "Failed to open deliveredMessages.log for writing!\n";
        }

        // Flushing logs based on whether it's a sender or receiver
        // std::lock_guard<std::mutex> logLock(pl->logMutex);
        // pl->running = false;

        // // Notify the log thread to flush and stop
        // pl->logCv.notify_all();

        // // Join the log threads and flush queues based on process type (sender/receiver)
        // if (pl->isReceiver) {
        //     if (pl->receiverLogThread.joinable()) {
        //         pl->receiverLogThread.join();
        //     }
        // } else {
        //     if (pl->senderLogThread.joinable()) {
        //         pl->senderLogThread.join();
        //     }
        // }

        // // Ensure the log file is properly flushed and closed
        // pl->logFile.flush();
        // pl->logFile.close();
    }

    exit(0);  // Exit the program
}

int main(int argc, char **argv) {
    signal(SIGTERM, stop);
    signal(SIGINT, stop);

    bool requireConfig = true;

    Parser parser(argc, argv);
    parser.parse();

    hello();
    std::cout << std::endl;

    std::cout << "My PID: " << getpid() << "\n";
    std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM " << getpid() << "` to stop processing packets\n\n";

    std::cout << "Doing some initialization...\n\n";

    // Step 1: Read configs and identify yourself and the rest of the processes.
    unsigned long myId = parser.id();
    auto hosts = parser.hosts();

    unsigned long receiverId;
    int messageCount;

    std::ifstream configFile(parser.configPath());
    if (!configFile.is_open()) {
        std::cerr << "Could not open config file: " << parser.configPath() << "\n";
        exit(EXIT_FAILURE);
    }

    configFile >> messageCount >> receiverId;
    configFile.close();

    bool isReceiver = (myId == receiverId);
    std::cout << "My ID: " << myId << (isReceiver ? " (Receiver)\n" : " (Sender)\n");

    // Step 2: Create and open the output file
    std::ofstream logFile(parser.outputPath());
    if (!logFile.is_open()) {
        std::cerr << "Failed to open output file: " << parser.outputPath() << "\n";
        exit(EXIT_FAILURE);
    }

    // Step 3: Create and bind the socket
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set SO_REUSEADDR to allow quick reuse of the port after process termination
    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("Failed to set SO_REUSEADDR");
    }

    // Optionally, enable SO_REUSEPORT for load balancing between processes
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        perror("Failed to set SO_REUSEPORT");
    }

    // Increase socket buffer sizes for sending and receiving
    int rcvbuf_size = 16384; 
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(rcvbuf_size)) < 0) {
        perror("Failed to set receive buffer size");
    }

    int sndbuf_size = 16384;
    if (setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(sndbuf_size)) < 0) {
        perror("Failed to set send buffer size");
    }

    // Optionally, set the socket to non-blocking mode
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("Failed to set non-blocking mode");
    }

    // Configure and bind the socket to the specified address and port
    struct sockaddr_in myAddr {};
    myAddr.sin_family = AF_INET;
    myAddr.sin_port = htons(hosts[myId - 1].port);
    myAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, reinterpret_cast<struct sockaddr*>(&myAddr), sizeof(myAddr)) < 0) {
        perror("bind failed");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    std::cout << "Socket bound to port " << ntohs(myAddr.sin_port) << std::endl;

    // // Initialize PerfectLinks with current process ID (myId) and log file
    // PerfectLinks pl(sockfd, myAddr, logFile, static_cast<int>(myId));

    // Initialize the PerfectLinks object globally
    PerfectLinks plInstance(sockfd, myAddr, logFile, static_cast<int>(myId));
    pl = &plInstance;  // Set the global pointer to the created PerfectLinks object


    pl->packetSize = 8;  // Send larger batches, depending on the system's capacity
    pl->isReceiver = isReceiver;

    if (isReceiver) {
        // Receiver logic - run until all senders stop
        std::thread receiverThread(&PerfectLinks::receiveMessages, pl);

        // Start threads to handle acknowledgment sending
        std::thread ackThread(&PerfectLinks::startSendingAcks, pl);

        receiverThread.join(); // Keep the receiver running until terminated
        ackThread.join(); // Keep acknowledgment threads running
    } else {
        // Sender logic
        std::thread receiverThread(&PerfectLinks::receiveAcknowledgments, pl);

        struct sockaddr_in receiverAddr = {};
        receiverAddr.sin_family = AF_INET;
        receiverAddr.sin_port = htons(hosts[receiverId - 1].port);
        inet_pton(AF_INET, "127.0.0.1", &receiverAddr.sin_addr);

        pl->startSending(receiverAddr, messageCount);

        receiverThread.join();  // Keep listening for acks
    }

    return 0;
}
