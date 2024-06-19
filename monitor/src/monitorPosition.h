#ifndef MONITOR_POSITION_H
#define MONITOR_POSITION_H

#include <libpq-fe.h>
#include <iostream>
#include <chrono>
#include <thread>

class MonitorPosition {
public:
    void run();
};

#endif // MONITOR_POSITION_H
