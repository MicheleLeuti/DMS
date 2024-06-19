#ifndef CONTROL_CENTER_H
#define CONTROL_CENTER_H

#include <string>
#include <vector>
#include <utility>
#include <iostream>
#include <chrono>
#include <map>
#include <ctime>
#include <algorithm>
#include <unordered_map>
#include <libpq-fe.h>
#include <tuple> 
#include <hiredis/hiredis.h>
#include <cmath>
#include <thread>
#include <sstream> 

class ControlCenter {
public:
    void sendInstructions(const std::vector<std::tuple<int, std::pair<double, double>, std::pair<double, double>>>& routes);
    void receiveStatus();
    void updateDatabase();
    void heartbeat();
    std::unordered_map<std::string, std::tuple<std::string, int, double, double>> droneStatuses;
    std::map<int, std::chrono::time_point<std::chrono::system_clock>> routeIds;
    
private:
    static int extractDroneNumber(const std::string& droneId);
    std::vector<std::tuple<int, int, std::chrono::system_clock::time_point>> logs;
    void sendCreateInstruction(const std::string& type); // Metodo per creare un drone
    void sendRouteInstruction(const std::string& type, const std::string& droneId, const std::tuple<int, std::pair<double, double>, std::pair<double, double>>& route); // Metodo per inviare una rotta
    void sendRechargeInstruction(const std::string& droneId, const std::pair<double, double>& destination, int routeId); // Metodo per ricaricare un drone
    void sendDroneOnRoute(const std::tuple<int, std::pair<double, double>, std::pair<double, double>>& route, const std::string& droneId);
    void printMap(const std::unordered_map<std::string, std::tuple<std::string, int, double, double>>& map);
    double calculateDistanceToBase(double x, double y);
    double calculateTimeToBase(double distance);
    void updateDroneStatus(const std::string& droneId, const std::string& status, int batterySeconds, double posX, double posY);
    
   
    int nextDroneId = 1;
    std::string createNewDrone();
};

#endif // CONTROL_CENTER_H
