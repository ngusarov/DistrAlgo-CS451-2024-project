#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>
#include <fstream>
#include <fcntl.h>  // Include this to use F_GETFL, F_SETFL, O_NONBLOCK
#include <unistd.h> // For getpid()
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "parser.hpp"
#include "PerfectLinks.hpp"
#include "BEB.hpp"
#include "LatticeAgreement.hpp"
#include "Messages.hpp"

// Forward declare global pointers if needed
// However, try to limit global usage. It's shown here for consistency with previous code.
PerfectLinks* pl = nullptr;
BEB* beb = nullptr;
LatticeAgreement* la = nullptr;

static void stop(int) {
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);

    std::cout << "Immediately stopping network packet processing.\n";
    std::cout << "Writing output.\n";

    if (la != nullptr) {
        la->flushDecisions(); // Write any remaining lines
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

    // Print the same initial messages as before
    std::cout << "My PID: " << getpid() << "\n";
    std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM " << getpid() << "` to stop processing packets\n\n";

    std::cout << "Doing some initialization...\n\n";

    // Step 1: Read configs and identify yourself and the rest of the processes.
    unsigned long myId = parser.id();
    auto hosts = parser.hosts();

    // Read lattice agreement config:
    std::ifstream configFile(parser.configPath());
    if (!configFile.is_open()) {
        std::cerr << "Could not open config file: " << parser.configPath() << "\n";
        exit(EXIT_FAILURE);
    }

    // p, vs, ds on the first line
    unsigned int p, vs, ds;
    configFile >> p >> vs >> ds;

    // Next p lines: each contains a proposal set (up to vs elements)
    std::vector<std::vector<int>> proposals(p);
    for (unsigned int i = 0; i < p; i++) {
        for (unsigned int j = 0; j < vs; j++) {
            int val;
            if (!(configFile >> val)) {
                // fewer elements than vs if line ends early
                break;
            }
            proposals[i].push_back(val);
        }
    }
    configFile.close();

    std::cout << "My ID: " << myId << std::endl;

    // Step 2: Create and open the output file
    std::ofstream logFile(parser.outputPath());
    if (!logFile.is_open()) {
        std::cerr << "Failed to open output file: " << parser.outputPath() << "\n";
        exit(EXIT_FAILURE);
    }
    // We will not buffer large logs; immediate logging after each decision happens in LatticeAgreement

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

    // Number of processes
    int n = (int)processIds.size();
    // Compute f if needed (assuming n=2f+1, or any other formula required by LA)
    int f = (n - 1) / 2;

    // Initialize PerfectLinks in a simple fire-and-forget mode
    PerfectLinks plInstance(sockfd, myAddr, (int)myId, updatedProcessIdToAddress);
    pl = &plInstance;

    // Initialize BEB
    BEB bebInstance(&plInstance, processIds);
    beb = &bebInstance;

    // Initialize LatticeAgreement
    // Assume LatticeAgreement takes (BEB*, myId, n, f, logFile)
    LatticeAgreement laInstance(&bebInstance, (int)myId, n, f, logFile);
    la = &laInstance;

    // Run multi-shot lattice agreement for p proposals
    for (unsigned int i = 0; i < p; i++) {
        // propose() would run one single-shot lattice agreement for proposals[i]
        // The LatticeAgreement is responsible for logging immediately once a decision is reached
        la->propose(proposals[i]);

        // After returning from propose(), the decided set should have been logged already.
        // If propose() is asynchronous, we'd have a different approach (e.g., a blocking wait),
        // but let's assume it's blocking until decision is reached for simplicity.
    }

    // Since we log immediately after each round, no need for a final flush here, but let's just ensure it:
    logFile.flush();

    return 0;
}