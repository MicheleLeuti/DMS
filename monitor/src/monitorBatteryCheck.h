#ifndef MONITOR_BATTERY_CHECK_H
#define MONITOR_BATTERY_CHECK_H

#include <libpq-fe.h>
#include <iostream>
#include <chrono>
#include <thread>

class MonitorBatteryCheck {
public:
    void run();
};

#endif // MONITOR_BATTERY_CHECK_H
