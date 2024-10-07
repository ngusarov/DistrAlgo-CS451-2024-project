#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>
#include <fstream>

#include "parser.hpp"
#include "hello.h"
#include "PerfectLinks.hpp"

static void stop(int) {
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);

    std::cout << "Immediately stopping network packet processing.\n";
    std::cout << "Writing output.\n";
    exit(0);
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

    // Initialize PerfectLinks with current process ID (myId) and log file
    PerfectLinks pl(sockfd, myAddr, logFile, static_cast<int>(myId));

    // Step 4: Start the receiving thread for both sender and receiver
    std::thread receiverThread(&PerfectLinks::deliver, &pl);

    if (isReceiver) {
        // Receiver logic - run until all senders stop
        receiverThread.join(); // Keep the receiver running until terminated
    } else {
        // Sender logic
        struct sockaddr_in receiverAddr = {};
        receiverAddr.sin_family = AF_INET;
        receiverAddr.sin_port = htons(hosts[receiverId - 1].port);
        inet_pton(AF_INET, "127.0.0.1", &receiverAddr.sin_addr); // Assuming localhost for simplicity

        // Start the thread pool for sending messages
        pl.startSending(receiverAddr, messageCount, static_cast<int>(receiverId));

        receiverThread.join(); // Keep listening for acks
    }

    return 0;
}
