#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>
#include <fstream>
#include <fcntl.h>  // Include this to use F_GETFL, F_SETFL, O_NONBLOCK
// #include <filesystem>  // Requires C++17

#include "parser.hpp"
#include "hello.h"
#include "PerfectLinks.hpp"
#include "URB.hpp"

// Declare the global PerfectLinks object
PerfectLinks* pl = nullptr;  // Initially a nullptr
UniformReliableBroadcast* urb = nullptr;


// void createLogFile(const std::string& outputPath) {
//     // Step 1: Ensure directory exists
//     std::filesystem::path filePath(outputPath);
//     std::filesystem::path dirPath = filePath.parent_path();

//     if (!dirPath.empty() && !std::filesystem::exists(dirPath)) {
//         // Create the directory and its parents if they don't exist
//         if (!std::filesystem::create_directories(dirPath)) {
//             std::cerr << "Failed to create directory: " << dirPath << "\n";
//             exit(EXIT_FAILURE);
//         }
//     }

//     // Step 2: Create and open the output file
//     std::ofstream logFile(outputPath);
//     if (!logFile.is_open()) {
//         std::cerr << "Failed to open output file: " << outputPath << "\n";
//         exit(EXIT_FAILURE);
//     }
// }


// Signal handler for stopping the process and flushing logs
static void stop(int) {
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);

    std::cout << "Immediately stopping network packet processing.\n";
    std::cout << "Writing output.\n";

    if (urb != nullptr) {
        std::cout << urb->logBuffer.size() << " messages left in the buffer.\n";
        urb->flushLogBuffer();
    }
    std::cout << "Finished writing" << std::endl;

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

    // unsigned long receiverId;
    int messageCount;

    std::ifstream configFile(parser.configPath());
    if (!configFile.is_open()) {
        std::cerr << "Could not open config file: " << parser.configPath() << "\n";
        exit(EXIT_FAILURE);
    }

    // configFile >> messageCount >> receiverId; // TODO this was for the PerfectLinks
    configFile >> messageCount; // This is for URB + FIFO
    configFile.close();

    // bool isReceiver = (myId == receiverId);
    std::cout << "My ID: " << myId << std::endl;// << (isReceiver ? " (Receiver)\n" : " (Sender)\n");

    // Step 2: Create and open the output file
    std::ofstream logFile(parser.outputPath());
    if (!logFile.is_open()) {
        std::cerr << "Failed to open output file: " << parser.outputPath() << "\n";
        exit(EXIT_FAILURE);
    }
    // std::string outputPath = parser.outputPath();
    // // createLogFile(outputPath);

    // // Proceed with other operations, now that the file is guaranteed to exist
    // std::ofstream logFile(outputPath);
    // if (logFile.is_open()) {
    //     logFile << "Logging initialized successfully.\n";
    //     logFile.close();
    //     std::cout << "Log file created and written to at: " << outputPath << std::endl;
    // } else {
    //     std::cerr << "Failed to write to log file at: " << outputPath << std::endl;
    // }

    // Step 3: Create and bind the socket
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Enable SO_REUSEADDR to quickly reuse the port after process termination
    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("Failed to set SO_REUSEADDR");
    }

    // Enable SO_REUSEPORT to allow multiple sockets to bind to the same port (useful in multi-process scenarios)
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        perror("Failed to set SO_REUSEPORT");
    }

    // Increase receive buffer size to handle high incoming traffic
    int rcvbuf_size = 512 * 1024;  // 512 KB
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(rcvbuf_size)) < 0) {
        perror("Failed to set receive buffer size");
    }

    // Increase send buffer size for high outgoing traffic
    int sndbuf_size = 512 * 1024;  // 512 KB
    if (setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(sndbuf_size)) < 0) {
        perror("Failed to set send buffer size");
    }

    // Enable non-blocking mode for faster handling of incoming/outgoing packets
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl(F_GETFL) failed");
        exit(EXIT_FAILURE);
    }
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("Failed to set non-blocking mode");
    }

    // Optional: Set IP_PKTINFO to retrieve the destination address of incoming packets
    int pktinfo = 1;
    if (setsockopt(sockfd, IPPROTO_IP, IP_PKTINFO, &pktinfo, sizeof(pktinfo)) < 0) {
        perror("Failed to set IP_PKTINFO");
    }

    int tos = 0x10;  // Low delay (e.g., for higher priority)
    if (setsockopt(sockfd, IPPROTO_IP, IP_TOS, &tos, sizeof(tos)) < 0) {
        perror("Failed to set TOS");
    }

    for (auto& host : hosts) {
        if (host.port < 2048) {
            host.port = static_cast<uint16_t>((2048 + host.id <= std::numeric_limits<uint16_t>::max()) 
                                        ? 2048 + host.id 
                                        : std::numeric_limits<uint16_t>::max());


            std::cout << "Host " << host.id << " had port below 2048. Reassigned to port " << host.port << std::endl;
        }
    }

    // Populate addressToProcessId
    std::unordered_map<sockaddr_in, int, AddressHash, AddressEqual> updatedAddressToProcessId;
    std::unordered_map<int, sockaddr_in> updatedProcessIdToAddress;
    std::vector<int> processIds;
    for (const auto& host : hosts) {
        struct sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(host.port));  // Ensure port uses uint16_t

        if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) <= 0) {
            std::cerr << "Invalid IP address for host: " << host.ip << std::endl;
            exit(EXIT_FAILURE);
        }

        updatedAddressToProcessId[addr] = static_cast<int>(host.id);  // Ensure host.id fits the map type
        updatedProcessIdToAddress[static_cast<int>(host.id)] = addr;
        processIds.push_back(static_cast<int>(host.id));
    }

    std::cout << "Updated addressToProcessId and hosts table:" << std::endl;
    for (const auto& [addr, processId] : updatedAddressToProcessId) {
        char ipStr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr.sin_addr, ipStr, sizeof(ipStr));
        std::cout << "Process " << processId << " -> " << ipStr << ":" << ntohs(addr.sin_port) << std::endl;
    }

    // Bind the socket for the current process
    struct sockaddr_in myAddr {};
    myAddr.sin_family = AF_INET;
    myAddr.sin_addr.s_addr = INADDR_ANY;
    myAddr.sin_port = htons(static_cast<uint16_t>(hosts[myId - 1].port));  // Use updated port

    std::cout << "Process " << myId << " binding to port " << ntohs(myAddr.sin_port) << std::endl;

    if (bind(sockfd, reinterpret_cast<struct sockaddr*>(&myAddr), sizeof(myAddr)) < 0) {
        perror("Bind failed");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    std::cout << "Socket successfully bound for Process " << myId << " at port " << ntohs(myAddr.sin_port) << std::endl;

    // Initialize the PerfectLinks object globally
    PerfectLinks plInstance(sockfd, myAddr, static_cast<int>(myId));
    plInstance.addressToProcessId = std::move(updatedAddressToProcessId);
    pl = &plInstance;

    // Initialize URB
    urb = new UniformReliableBroadcast(&plInstance, logFile, myAddr, static_cast<int>(myId), processIds);
    urb->processIdToAddress = std::move(updatedProcessIdToAddress);

    // Assign URB to PerfectLinks
    plInstance.urb = urb;

    {
        std::unique_lock<std::mutex> queueLock(pl->queueMutex);
        std::lock_guard<std::mutex> logBufferLock(urb->logBufferMutex);
        // Use URB broadcast // TODO Memory limitation won't allow to do it like this
        for (int i = 1; i <= urb->windowSize; ++i){
            urb->broadcastManyMessages(i);
        }
    }

    // Initialize the pendingMessages for future broadcasts
    if (urb->windowSize + 1 <= messageCount){
        urb->pendingMessages.addSegment({urb->windowSize + 1, messageCount});  // Example range
    }
    
    std::vector<std::thread> sendThreads;  // Thread pool for sending messages
    // Launch sender threads
    for (int i = 0; i < 1; ++i) {
        sendThreads.emplace_back(&PerfectLinks::sendWorker, pl);
    }

    std::vector<std::thread> receiverThreads;
    for (int i = 0; i < 1; ++i) {
        receiverThreads.emplace_back(&PerfectLinks::receive, pl);
    }

    std::vector<std::thread> ackThreads;
    for (int i = 0; i < 1; ++i) {
        ackThreads.emplace_back(&PerfectLinks::ackWorker, pl);
    }


    for (auto &thread : sendThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (auto &thread : receiverThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (auto &thread : ackThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    delete urb;

    return 0;
}

