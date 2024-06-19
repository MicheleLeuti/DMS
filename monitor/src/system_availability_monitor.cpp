#include "system_availability_monitor.h"

SystemAvailabilityMonitor::SystemAvailabilityMonitor() {
    context = redisConnect("127.0.0.1", 6379);
    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
    }
}

SystemAvailabilityMonitor::~SystemAvailabilityMonitor() {
    if (context) redisFree(context);
}

void SystemAvailabilityMonitor::run() {
    std::this_thread::sleep_for(std::chrono::seconds(20));
    std::thread heartbeatSender(&SystemAvailabilityMonitor::sendHeartbeat, this);
    std::thread heartbeatReceiver(&SystemAvailabilityMonitor::receiveHeartbeat, this);
    std::thread statusChecker(&SystemAvailabilityMonitor::checkComponentStatus, this);

    heartbeatSender.join();
    heartbeatReceiver.join();
    statusChecker.join();
}

void SystemAvailabilityMonitor::sendHeartbeat() {
    while (true) {
        redisReply* reply = (redisReply*)redisCommand(context, "PUBLISH %s %s", "heartbeat_channel", "heartbeat");
        if (reply) {
            freeReplyObject(reply);
        } else {
            std::cerr << "Failed to send heartbeat." << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

void SystemAvailabilityMonitor::receiveHeartbeat() {
    redisContext* subscriber = redisConnect("127.0.0.1", 6379);
    redisReply* reply = (redisReply*)redisCommand(subscriber, "SUBSCRIBE %s", "heartbeat_response_channel");
    if (reply) {
        freeReplyObject(reply);
    }

    while (true) {
        redisGetReply(subscriber, (void**)&reply);
        if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 3) {
            std::string component = reply->element[2]->str;
            lastHeartbeat[component] = std::chrono::system_clock::now();
        }
        if (reply) freeReplyObject(reply);
    }
    redisFree(subscriber);
}

void SystemAvailabilityMonitor::checkComponentStatus() {
    while (true) {
        auto now = std::chrono::system_clock::now();
        for (const auto& pair : lastHeartbeat) {
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - pair.second).count();
            if (duration > 10) {
                std::cerr << "Component " << pair.first << " is not responding." << std::endl;
            } else {
                std::cout << "Component " << pair.first << " is available." << std::endl;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }
}
