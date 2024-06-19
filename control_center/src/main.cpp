#include "control_center.h"
#include <thread>
#include <vector>
#include <utility>
#include <cmath>

double roundToTwoDecimalPlaces(double value) {
    return std::round(value * 100.0) / 100.0;
}

// Funzione per generare le rotte con coordinate arrotondate e ID
std::vector<std::tuple<int, std::pair<double, double>, std::pair<double, double>>> generateRoutes() {
    std::vector<std::tuple<int, std::pair<double, double>, std::pair<double, double>>> routes;
    int id = 1;

    for (double y = 0.01; y <= 6.0; y += 0.02) {
        double roundedY = roundToTwoDecimalPlaces(y);
        routes.push_back({id, {0.00, roundedY}, {6.0, roundedY}});
        id++;
    }
    return routes;
}

int main() {
    ControlCenter controlCenter;

    // Genera le rotte
    auto routes = generateRoutes();

    // Avvia un thread per ricevere lo stato del drone
    std::thread statusThread(&ControlCenter::receiveStatus, &controlCenter);
    std::thread instructionsThread(&ControlCenter::sendInstructions, &controlCenter, std::ref(routes));
    std::thread dbThread(&ControlCenter::updateDatabase, &controlCenter);
    std::thread heartThread(&ControlCenter::heartbeat, &controlCenter);
    // Starting the thread with droneStatuses

    statusThread.join();
    instructionsThread.join();
    dbThread.join();
    heartThread.join();
    return 0;
}
