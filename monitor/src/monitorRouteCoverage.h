#ifndef MONITORROUTECOVERAGE_H
#define MONITORROUTECOVERAGE_H

#include <libpq-fe.h>

class MonitorRouteCoverage {
public:
    void run();
    void populateRoutes();
};

#endif // MONITORROUTECOVERAGE_H
