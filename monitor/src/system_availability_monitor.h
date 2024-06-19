#ifndef SYSTEM_AVAILABILITY_MONITOR_H
#define SYSTEM_AVAILABILITY_MONITOR_H

#include <unordered_map>
#include <string>
#include <chrono>
#include <thread>
#include <iostream>
#include <hiredis/hiredis.h>

class SystemAvailabilityMonitor {
public:
    SystemAvailabilityMonitor();
    ~SystemAvailabilityMonitor();
    void run();

private:
    void sendHeartbeat();
    void checkComponentStatus();
    void receiveHeartbeat();

    std::unordered_map<std::string, std::chrono::system_clock::time_point> lastHeartbeat;
    redisContext* context;
};

#endif // SYSTEM_AVAILABILITY_MONITOR_H
