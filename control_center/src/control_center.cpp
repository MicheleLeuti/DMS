#include "control_center.h"

void ControlCenter::sendInstructions(const std::vector<std::tuple<int, std::pair<double, double>, std::pair<double, double>>>& routes) {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }

    while (true) {
        int c = 0;
        std::vector<std::string> droneIds;

        // Contare il numero di droni in stato idle
        PGresult* res = PQexec(conn, "SELECT COUNT(*) FROM drone WHERE status = 'idle'");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "SELECT COUNT(*) failed: " << PQerrorMessage(conn) << std::endl;
            if (res) PQclear(res);
            continue;
        }
        int idleCount;
        try{
            idleCount = std::stoi(PQgetvalue(res, 0, 0));
        }
        catch (const std::invalid_argument& e) {
            std::cerr << "Invalid idle count: " << idleCount << std::endl;
            continue;
        }
        if (res) PQclear(res);

        // Creare nuovi droni se necessario
        if (idleCount < routes.size()) {
            for (int i = idleCount; i < routes.size(); ++i) {
                sendCreateInstruction("create_drone");
                c++;
                std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Aspetta la creazione del drone
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        // Ottenere gli ID dei droni in stato idle
        res = PQexec(conn, "SELECT drone_id FROM drone WHERE status = 'idle' AND drone_id != 'drone_0'");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "SELECT drone_id failed: " << PQerrorMessage(conn) << std::endl;
            if (res) PQclear(res);
            continue;
        }
        droneIds.clear();
        for (int i = 0; i < PQntuples(res); ++i) {
            droneIds.push_back(PQgetvalue(res, i, 0));
        }
        if (res) PQclear(res);

        
        if (droneIds.size() < routes.size()) {
            std::cerr << "DronesIds < RoutesIds" << std::endl;
            continue;
        }
        
        std::cout << "Starting sending routes...\n" << std::endl;
        for (size_t i = 0; i < routes.size(); ++i) {
            c++;
            sendDroneOnRoute(routes[i], droneIds[i]);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        std::cout << "Finished sending routes\n" << std::endl;
        c = 298000 - c * 50 - 10000;
        std::this_thread::sleep_for(std::chrono::milliseconds(30000));
    }

    PQfinish(conn);
}

void ControlCenter::heartbeat(){
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
            std::string responseMessage = "Control_center";
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

void ControlCenter::sendCreateInstruction(const std::string& type) {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    char channel[] = "instruction_channel";
    redisReply* reply;

    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        if (context) redisFree(context);
        return;
    }

    std::string instructionMessage = type;

    reply = (redisReply*)redisCommand(context, "PUBLISH %s %s", channel, instructionMessage.c_str());
    if (reply) {
        freeReplyObject(reply);
    } else {
        std::cerr << "Failed to send instruction." << std::endl;
    }
    if (context) redisFree(context);
}

void ControlCenter::sendRouteInstruction(const std::string& type, const std::string& droneId, const std::tuple<int, std::pair<double, double>, std::pair<double, double>>& route) {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    char channel[] = "instruction_channel";
    redisReply* reply;

    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        if (context) redisFree(context);
        return;
    }

    int routeId = std::get<0>(route);
    std::string instructionMessage = droneId + ":" + type + ":" + "routeId:" + std::to_string(routeId) + ":";

    auto start = std::get<1>(route);
    auto end = std::get<2>(route);
    instructionMessage += std::to_string(start.first) + "," + std::to_string(start.second) + ";";
    instructionMessage += std::to_string(end.first) + "," + std::to_string(end.second);

    reply = (redisReply*)redisCommand(context, "PUBLISH %s %s", channel, instructionMessage.c_str());
    if (reply) {
        freeReplyObject(reply);
    } else {
        std::cerr << "Failed to send instruction." << std::endl;
    }
    if (context) redisFree(context);
}

void ControlCenter::sendRechargeInstruction(const std::string& droneId, const std::pair<double, double>& destination, int routeId) {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    char channel[] = "instruction_channel";
    redisReply* reply;

    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        if (context) redisFree(context);
        return;
    }

    std::string instructionMessage = droneId + ":" + "recharge" + ":" + std::to_string(routeId) + ":";
    instructionMessage += std::to_string(destination.first) + "," + std::to_string(destination.second);

    reply = (redisReply*)redisCommand(context, "PUBLISH %s %s", channel, instructionMessage.c_str());
    if (reply) {
        freeReplyObject(reply);
    } else {
        std::cerr << "Failed to send instruction." << std::endl;
    }
    if (context) redisFree(context);
}

void ControlCenter::sendDroneOnRoute(const std::tuple<int, std::pair<double, double>, std::pair<double, double>>& route, const std::string& droneId) {
    sendRouteInstruction("follow_route", droneId, route);
}

void ControlCenter::receiveStatus() {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    char channel[] = "drone_status";
    redisReply* reply;

    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        if (context) redisFree(context);
        return;
    }

    std::cout << "Connected to Redis, subscribing to drone_status..." << std::endl;
    reply = (redisReply*)redisCommand(context, "SUBSCRIBE %s", channel);
    if (reply == nullptr) {
        std::cerr << "Error: cannot subscribe to drone_status" << std::endl;
        if (context) redisFree(context);
        return;
    }

    std::cout << "Monitoring drone status..." << std::endl;
    int c = 0;
    while (true) {
        c++;
        if (reply){
            freeReplyObject(reply);
        }
        redisGetReply(context, (void**)&reply);
        if (reply == nullptr) {
            std::cerr << "Error: command failed" << std::endl;
            break;
        }

        if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 3 && reply->element[2]->str != nullptr) {
            try {
                std::string statusMessage(reply->element[2]->str);
                size_t firstColon = statusMessage.find(':');
                size_t secondColon = statusMessage.find(':', firstColon + 1);
                size_t thirdColon = statusMessage.find(':', secondColon + 1);
                size_t fourthColon = statusMessage.find(':', thirdColon + 1);

                std::string droneId = statusMessage.substr(0, firstColon);
                int id = std::stoi(droneId.substr(6));
                int batterySeconds = std::stoi(statusMessage.substr(firstColon + 1, secondColon - firstColon - 1));
                std::string positionStr = statusMessage.substr(secondColon + 1, thirdColon - secondColon - 1);
                std::string status = statusMessage.substr(thirdColon + 1, fourthColon - thirdColon - 1);
                int routeId = std::stoi(statusMessage.substr(fourthColon + 1));

                size_t commaPos = positionStr.find(',');
                double posX = std::stod(positionStr.substr(0, commaPos));
                double posY = std::stod(positionStr.substr(commaPos + 1));

                updateDroneStatus(droneId, status, batterySeconds, posX, posY);

                if (c == 500) {
                    // printMap(droneStatuses);
                    c = 0;
                }

                if (status == "ready") {
                    routeIds[routeId] = std::chrono::system_clock::now();
                    logs.emplace_back(id, routeId, std::chrono::system_clock::now());
                    sendRechargeInstruction(droneId, {3.0, 3.0}, 0);
                }
            } 
            catch(const std::invalid_argument& e){
                std::cerr<<"Invalid argument: " << e.what() << std::endl;
            }
            catch (const std::exception& e) {
                std::cerr << "Exception: " << e.what() << std::endl;
            }
            
        }
    }
    if (reply){
        freeReplyObject(reply);
    }
    if (context){
        redisFree(context);
    }
}

void ControlCenter::updateDatabase() {
    std::this_thread::sleep_for(std::chrono::seconds(60));
    while (true) {
        PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
        if (PQstatus(conn) != CONNECTION_OK) {
            std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
            PQfinish(conn);
            return;
        }

        // Update routes table
        std::string updateQuery = "BEGIN; "; // Inizia una transazione

        for (const auto& route : routeIds) {
            int routeId = route.first;
            std::time_t visitedTime = std::chrono::system_clock::to_time_t(route.second);
            std::string visitedTimeStr = std::to_string(visitedTime);
            
            updateQuery += "UPDATE routes SET visited = true, visited_time = to_timestamp(" + visitedTimeStr + ") WHERE id = " + std::to_string(routeId) + "; ";
        }

        updateQuery += "COMMIT;"; // Termina la transazione

        PGresult* res = PQexec(conn, updateQuery.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "UPDATE 1 failed: " << PQerrorMessage(conn) << std::endl;
        }
        if (res) PQclear(res);

        // Update drone_logs table
        std::string insertLogsQuery = "BEGIN; "; // Inizia una transazione

        for (const auto& log : logs) {
            int id = std::get<0>(log);
            int rId = std::get<1>(log);
            auto timePoint = std::get<2>(log);
            std::time_t timeT1 = std::chrono::system_clock::to_time_t(timePoint);
            char buffer[80];
            strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&timeT1));
            std::string logTimeStr(buffer);

            insertLogsQuery += "INSERT INTO drone_logs (descrizione, log_time) VALUES ('drone_" + std::to_string(id) + " finished route: " + std::to_string(rId) + "', to_timestamp('" + logTimeStr + "', 'YYYY-MM-DD HH24:MI:SS')); ";
        }

        insertLogsQuery += "COMMIT;"; // Termina la transazione

        res = PQexec(conn, insertLogsQuery.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "INSERT logs failed: " << PQerrorMessage(conn) << std::endl;
        }
        if (res) PQclear(res);

        PQfinish(conn);
        std::cout << "Update database finished" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }
}

double ControlCenter::calculateDistanceToBase(double x, double y) {
    double baseX = 3.0;
    double baseY = 3.0;
    return sqrt(pow(baseX - x, 2) + pow(baseY - y, 2));
}

double ControlCenter::calculateTimeToBase(double distance) {
    double speed = 30.0 / 3600.0; // 30 km/h in km/s
    return distance / speed;
}

void ControlCenter::updateDroneStatus(const std::string& droneId, const std::string& status, int batterySeconds, double posX, double posY) {
    droneStatuses[droneId] = std::make_tuple(status, batterySeconds, posX, posY); 
}

int ControlCenter::extractDroneNumber(const std::string& droneId) {
    size_t pos = droneId.find('_');
    if (pos != std::string::npos) {
        return std::stoi(droneId.substr(pos + 1));
    }
    return -1;
}

void ControlCenter::printMap(const std::unordered_map<std::string, std::tuple<std::string, int, double, double>>& map) {
    std::vector<std::pair<std::string, std::tuple<std::string, int, double, double>>> vec(map.begin(), map.end());

    std::sort(vec.begin(), vec.end(), [](const std::pair<std::string, std::tuple<std::string, int, double, double>>& a, const std::pair<std::string, std::tuple<std::string, int, double, double>>& b) {
        return extractDroneNumber(a.first) < extractDroneNumber(b.first);
    });

    std::cout << "--------------------DRONE STATUSES-------------------------" << std::endl;
    for (const auto& pair : vec) {
        const std::string& status = std::get<0>(pair.second);
        int batterySeconds = std::get<1>(pair.second);
        double posX = std::get<2>(pair.second);
        double posY = std::get<3>(pair.second);

        std::cout << pair.first << ": " << status << ", Battery: " << batterySeconds << "s, Position: (" << posX << ", " << posY << ")" << std::endl;
    }
    std::cout << "-----------------------------------------------------------\n" << std::endl;
}
