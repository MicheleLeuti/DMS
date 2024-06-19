#include "drone.h"

int Drone::nextId = 1;
std::unordered_map<int, std::shared_ptr<Drone>> Drone::drones;
int inMission = 0;
int toBase = 0;
int ready = 0;
int idle = 0;
int broken = 0;
int charging = 0;
int wind = 0;
double weather = 0;

Drone::Drone(int id, int life) : droneId(id), posX(3.0), posY(3.0), batterySeconds(1800000), status("idle"), life(life){}

void Drone::startThreads() {
    if (droneId == 0) {
        std::thread(&Drone::sendStatus, this).detach();
        std::thread(&Drone::printStatus, this).detach();
        std::thread(&Drone::receiveInstruction, this).detach();
        std::thread([this]() { this->updateDatabase(Drone::drones); }).detach();
        std::thread(&Drone::changeConditions, this).detach();
        std::thread(&Drone::heartbeat, this).detach();
    } else {
        std::thread(&Drone::receiveInstruction, this).detach();
    }
}

void Drone::changeConditions(){
    while(true){
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(-3, 3);
        wind = dis(gen);
        std::uniform_int_distribution<> dis1(0, 2);
        int var = dis1(gen);
        if (var == 0) weather = 1;
        if (var == 1) weather = 0.5;
        if (var == 2) weather = 0.1;
        std::string w = "";
        if (weather == 1) w = "sunny";
        if (weather == 0.5) w = "rainy";
        if (weather == 0.1) w = "foggy";
        std::cout<< "Changed parameters, wind: " << wind << " weather: " << w << std::endl;
        std::this_thread::sleep_for(std::chrono::minutes(1));
    }
}

void Drone::printStatus(){
    while(true){
        std::cout << "#idle: "<< idle << " #inMission: "<< inMission << " #ready: "<< ready << " #toBase: "<< toBase << " #recharge: "<< charging << " #broken: " << broken <<"\n" <<std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    }
}

void Drone::heartbeat() {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        return;
    }

    redisReply* reply = (redisReply*)redisCommand(context, "SUBSCRIBE %s", "heartbeat_channel");
    if (reply == nullptr) {
        std::cerr << "Error: cannot subscribe to heartbeat_channel" << std::endl;
        redisFree(context);
        return;
    }
    freeReplyObject(reply);

    while (true) {
        redisGetReply(context, (void**)&reply);
        if (reply == nullptr) {
            std::cerr << "Error: command failed" << std::endl;
            break;
        }

        if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 3 && std::string(reply->element[2]->str) == "heartbeat") {
            std::string responseMessage = "drone";
            redisContext* pubContext = redisConnect("127.0.0.1", 6379);
            if (pubContext == nullptr || pubContext->err) {
                if (pubContext) {
                    std::cerr << "Error: " << pubContext->errstr << std::endl;
                } else {
                    std::cerr << "Can't allocate redis context" << std::endl;
                }
                if (pubContext) redisFree(pubContext);
                continue;
            }

            redisReply* pubReply = (redisReply*)redisCommand(pubContext, "PUBLISH %s %s", "heartbeat_response_channel", responseMessage.c_str());
            if (pubReply) {
                freeReplyObject(pubReply);
            } else {
                std::cerr << "Failed to send heartbeat response." << std::endl;
            }
            if (pubContext) redisFree(pubContext);
        }

        if (reply) freeReplyObject(reply);
    }

    if (context) redisFree(context);
}

void Drone::updateDatabase(const std::unordered_map<int, std::shared_ptr<Drone>>& drones) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }
    while (true) {
        std::ostringstream query;
        query << "UPDATE drone SET "
              << "status = CASE";

        for (const auto& pair : drones) {
            const auto& drone = pair.second;
            if (drone) { // Check if the pointer is not null
                query << " WHEN drone_id = 'drone_" << drone->droneId << "' THEN '" << drone->status << "'";
            }
        }

        query << " ELSE status END, "
              << "battery_seconds = CASE";

        for (const auto& pair : drones) {
            const auto& drone = pair.second;
            if (drone) {
                query << " WHEN drone_id = 'drone_" << drone->droneId << "' THEN " << drone->batterySeconds;
            }
        }

        query << " ELSE battery_seconds END, "
              << "pos_x = CASE";

        for (const auto& pair : drones) {
            const auto& drone = pair.second;
            if (drone) {
                query << " WHEN drone_id = 'drone_" << drone->droneId << "' THEN " << drone->posX;
            }
        }

        query << " ELSE pos_x END, "
              << "pos_y = CASE";

        for (const auto& pair : drones) {
            const auto& drone = pair.second;
            if (drone) {
                query << " WHEN drone_id = 'drone_" << drone->droneId << "' THEN " << drone->posY;
            }
        }

        query << " ELSE pos_y END, "
              << "log_time = CASE";

        for (const auto& pair : drones) {
            const auto& drone = pair.second;
            if (drone) {
                query << " WHEN drone_id = 'drone_" << drone->droneId << "' THEN NOW()";
            }
        }

        query << " ELSE log_time END "
              << "WHERE drone_id IN (";

        bool first = true;

        for (const auto& pair : drones) {
            const auto& drone = pair.second;
            if (drone) {
                if (!first) {
                    query << ", ";
                }
                query << "'drone_" << drone->droneId << "'";
                first = false;
            }
        }

        query << ");";

        PGresult* res = PQexec(conn, query.str().c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "UPDATE failed: " << PQerrorMessage(conn) << std::endl;
        }
        if (res) PQclear(res);  
    }
    PQfinish(conn);
}



void Drone::createNewDrone() {
    int id = nextId++;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 10);
    int life = dis(gen);
    auto newDrone = std::make_shared<Drone>(id, life);
    drones[id] = newDrone;

    idle++;

    // Connessione al database
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }

    // Query per l'inserimento del nuovo drone
    std::string query = "INSERT INTO drone (drone_id, status, battery_seconds, pos_x, pos_y) VALUES ('drone_" + std::to_string(id) + "', 'idle', 1800000, 3.0, 3.0)";
    PGresult* res = PQexec(conn, query.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "INSERT INTO drone failed: " << PQerrorMessage(conn) << std::endl;
    }
    if (res) PQclear(res);
    PQfinish(conn);
    newDrone->startThreads();
    std::cout<<"Crated new drone with id: " << id << " , life: " << life << std::endl;
}

void Drone::repair() {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    this->batterySeconds = 1800000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> minutes_dist(3, 5);
    int minutes = minutes_dist(gen);
    std::cout<<"Started repairing drone: " << this->droneId << " extimated time: " << minutes << " minutes"<<std::endl;
    std::this_thread::sleep_for(std::chrono::minutes(minutes));
    std::uniform_int_distribution<> life_dist(1, 10);
    broken--;
    idle++;
    this->life = life_dist(gen);
    this->posX = 3.0;
    this->posY = 3.0;
    this->status = "idle";
    std::cout<<"Finished repairing drone: " << this->droneId << " new life: " << this->life << std::endl;
}

void Drone::sendStatus() {
    while (true) {
        for (const auto& pair : drones) {
            auto drone = pair.second;
            if (!drone) {
                std::cerr << "Error: drone is nullptr" << std::endl;
                continue;
            }
            if (drone->status.empty()) {
                std::cerr << "Error: drone status is empty" << std::endl;
                continue;
            }
            redisContext* context = redisConnect("127.0.0.1", 6379);
            char channel[] = "drone_status";
            redisReply *reply;

            if (context == nullptr || context->err) {
                if (context) {
                    std::cerr << "Error: " << context->errstr << std::endl;
                } else {
                    std::cerr << "Can't allocate redis context" << std::endl;
                }
                return;
            }

            int routeIdToSend = (drone->status == "idle") ? 0 : drone->droneRouteId;
            std::string statusMessage = "drone_" + std::to_string(drone->droneId) + ":" +
                                        std::to_string(drone->batterySeconds) + ":" +
                                        std::to_string(drone->posX) + "," + std::to_string(drone->posY) + ":" +
                                        drone->status + ":" +
                                        std::to_string(routeIdToSend);

            //std::cout << statusMessage << std::endl;

            reply = (redisReply*)redisCommand(context, "PUBLISH %s %s", channel, statusMessage.c_str());
            if (reply) {
                freeReplyObject(reply);
            }

            if (context) redisFree(context);
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(1)); 
    }
}

void Drone::receiveInstruction() {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    char channel[] = "instruction_channel";
    redisReply* reply;

    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
        return;
    }

    reply = (redisReply*)redisCommand(context, "SUBSCRIBE %s", channel);
    if (reply == nullptr) {
        std::cerr << "Error: cannot subscribe to instruction_channel" << std::endl;
        if (context) redisFree(context);
        return;
    }

    while (true) {
        if (reply) freeReplyObject(reply);
        redisGetReply(context, (void**)&reply);
        if (reply == nullptr) {
            std::cerr << "Error: command failed" << std::endl;
            break;
        }

        if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 3 && reply->element[2]->str != nullptr) {
            try {
                std::string instructionStr(reply->element[2]->str);

                if (instructionStr == "create_drone") {
                    if (droneId == 0) {
                        createNewDrone();
                    }
                } 
                else {
                    size_t firstColon = instructionStr.find(':');
                    size_t secondColon = instructionStr.find(':', firstColon + 1);

                    std::string id_str = instructionStr.substr(0, firstColon);
                    std::string type = instructionStr.substr(firstColon + 1, secondColon - firstColon - 1);
                    if (type == "follow_route"){
                        size_t thirdColon = instructionStr.find(':', secondColon + 1);
                        size_t fourthColon = instructionStr.find(':', thirdColon + 1);

                        if (firstColon == std::string::npos || secondColon == std::string::npos || thirdColon == std::string::npos || fourthColon == std::string::npos) {
                            std::cerr << "Invalid instruction format: " << instructionStr << std::endl;
                            continue;
                        }
                        int routeId;
                        try {
                            routeId = std::stoi(instructionStr.substr(thirdColon + 1, fourthColon - thirdColon - 1));
                        } catch (const std::invalid_argument& e) {
                            std::cerr << "Invalid route ID: " << instructionStr << std::endl;
                            continue;
                        }
                        std::string data = instructionStr.substr(fourthColon + 1);

                        followInstruction(id_str, type, data, routeId);
                    }
                    else{
                        size_t firstColon = instructionStr.find(':');
                        size_t secondColon = instructionStr.find(':', firstColon + 1);
                        size_t thirdColon = instructionStr.find(':', secondColon + 1);
                        std::string id_str = instructionStr.substr(0, firstColon);
                        std::string type = instructionStr.substr(firstColon + 1, secondColon - firstColon - 1);
                        int routeId;
                        try {
                            routeId = std::stoi(instructionStr.substr(secondColon + 1, thirdColon - secondColon - 1));
                        } catch (const std::invalid_argument& e) {
                            std::cerr << "Invalid route ID: " << instructionStr << std::endl;
                            continue;
                        }
                        std::string data = instructionStr.substr(thirdColon + 1);

                        followInstruction(id_str, type, data, routeId);
                    }
                }
                         
            } catch (const std::exception& e) {
                std::cerr << "Exception: " << e.what() << std::endl;
            }
        }
    }

    if (reply) freeReplyObject(reply);
    if (context) redisFree(context);
}

void Drone::followInstruction(const std::string& id_str, const std::string& type, const std::string& data, int routeId) {
    if (id_str != "drone_" + std::to_string(droneId)) {
        return; // L'istruzione non è per questo drone
    }
    try {
        if (type == "recharge") {
            double destX, destY;
            size_t commaPos = data.find(',');
            try{
                destX = std::stod(data.substr(0, commaPos));
                destY = std::stod(data.substr(commaPos + 1));
                if (this->status == "ready") {
                ready--;
                this->status = "to_base";
                toBase++;
                moveToDestination(destX, destY);
                }
            }
            catch (const std::invalid_argument& e) {
                std::cerr << "Invalid destination: " << destX << " " << destY <<std::endl;
            }   
        } else if (type == "follow_route") {
            if (this->status != "idle"){
                return;
            }
            std::vector<std::pair<double, double>> route;
            size_t startPos = 0;
            size_t commaPos, semicolonPos;
            double x;
            double y;
            bool flag = false;
            while ((semicolonPos = data.find(';', startPos)) != std::string::npos) {
                commaPos = data.find(',', startPos);
                try{
                    x = std::stod(data.substr(startPos, commaPos - startPos));
                    y = std::stod(data.substr(commaPos + 1, semicolonPos - commaPos - 1));
                    route.emplace_back(x, y);
                    startPos = semicolonPos + 1;
                }
                catch (const std::invalid_argument& e) {
                    std::cerr << "Invalid route point: " << x << " " << y <<std::endl;
                    flag = true;
                }
            }
            commaPos = data.find(',', startPos);
            try{
                if (flag == false){
                    x = std::stod(data.substr(startPos, commaPos - startPos));
                    y = std::stod(data.substr(commaPos + 1));
                    route.emplace_back(x, y);
                    this->status = "in_mission";
                    inMission++;
                    idle--;
                    this->droneRouteId = routeId;
                    followRoute(route);
                }
            }
            catch (const std::invalid_argument& e) {
                    std::cerr << "Invalid route point: " << x << " " << y <<std::endl;
            }  
        }
    } catch (const std::invalid_argument& e) {
        std::cerr << "Exception: invalid_argument - " << e.what() << std::endl;
    } catch (const std::out_of_range& e) {
        std::cerr << "Exception: out_of_range - " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
}

void Drone::moveToDestination(double x, double y) {
    double visibility = 0.01 * weather;
    std::round(visibility * 1000.0) / 1000.0;
    //int c = 0;
    while (distanceTo(x, y) > visibility) {
        //c++;
        double distance = distanceTo(x, y);
        double step = (30 + wind) / 3600000.0;
        visibility = 0.01 * weather;
        std::round(visibility * 1000.0) / 1000.0;
        /* per printare i cambiamenti
        int speed = 30 + wind;
        if (c == 10000){
            std::cout << "speed: " << speed << " visibility: " << visibility << std::endl;
            c = 0;
        } 
        */
        double moveX = step * (x - this->posX) / distance;
        double moveY = step * (y - this->posY) / distance;
        this->posX += moveX;
        this->posY += moveY;
        if (this->batterySeconds != 0) {
            this->batterySeconds--;
        }
        if (this->batterySeconds == 0){
            if (this->status == "to_base"){
                toBase--;
            }
            else{
                inMission--;
            }
            this->status = "broken";
            broken++;
            repair();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (this->status == "to_base") {
        //std::cout << "Reached control center for recharging." << std::endl;
        this->life--;
        if (this->life == 0){
            this->status = "broken";
            toBase--;
            broken++;
            repair();
        }
        else{
            this->status = "recharge";
            toBase--;
            charging++;
            recharge();
        }
    }
}

void Drone::followRoute(const std::vector<std::pair<double, double>>& route) {
    if (this->status == "in_mission") {
        //std::cout << "drone_" << droneId << " starting to follow the route." << std::endl;
        for (const auto& point : route) {
           // std::cout << "drone_" << droneId << " moving to: (" << point.first << ", " << point.second << "), current position: (" << posX << ", " << posY << ")" << std::endl;
            moveToDestination(point.first, point.second);
        }
        this->status = "ready";
        inMission--;
        ready++;
       // std::cout << "drone_" << droneId << " has finished following the route." << std::endl;
    }
}

void Drone::recharge() {
    std::random_device rd;
    std::mt19937 gen(rd());
    // Set the distribution to a range between 2 hours (7200 seconds) and 3 hours (10800 seconds)
    std::uniform_int_distribution<> dis(7200000, 10800000); // milliseconds

    int rechargeMilliseconds = dis(gen);

    std::string drone_id_m = "drone_" + std::to_string(droneId);
    std::string message = drone_id_m + " recharging for " + std::to_string(rechargeMilliseconds / 1000) + " seconds";
    // std::cout << message << std::endl;

    while (rechargeMilliseconds > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        rechargeMilliseconds--;
        if (rechargeMilliseconds % 60000 == 0 && rechargeMilliseconds > 0) {
            // std::cout << drone_id_m << " recharging, time left: " << rechargeMilliseconds / 60000 << " minutes." << std::endl;
        }
    }
    this->batterySeconds = 1800000; // Reset the battery to 1800 seconds
    this->status = "idle";
    idle++;
    charging--;
    // std::cout << "Recharged. Battery seconds: " << batterySeconds / 1000 << "s" << std::endl;
}


double Drone::distanceTo(double x, double y) {
    return sqrt(pow(x - posX, 2) + pow(y - posY, 2));
}
