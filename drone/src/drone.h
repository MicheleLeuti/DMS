// drone.h
#ifndef DRONE_H
#define DRONE_H

#include <iostream>
#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <libpq-fe.h> 
#include <hiredis/hiredis.h>
#include <cmath>
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>
#include <sstream> 

class Drone {
public:
    static int nextId;
    static std::unordered_map<int, std::shared_ptr<Drone>> drones;
    int life;
    int droneId;
    double posX;
    double posY;
    int batterySeconds;
    std::string status;
    int droneRouteId;
    std::thread statusThread;
    std::thread printThread;
    std::thread instructionThread;
    std::thread updateThread;
    Drone(int id, int life);
    void startThreads();
    void updateDatabase(const std::unordered_map<int, std::shared_ptr<Drone>>& drones);
    void printStatus();
    void createNewDrone();
    void changeConditions();
    void repair();
    void heartbeat();
    void sendStatus();
    void receiveInstruction();
    void followInstruction(const std::string& id_str, const std::string& type, const std::string& data, int routeId);
    void moveToDestination(double x, double y);
    void followRoute(const std::vector<std::pair<double, double>>& route);
    void recharge();
    double distanceTo(double x, double y);
};

#endif // DRONE_H
