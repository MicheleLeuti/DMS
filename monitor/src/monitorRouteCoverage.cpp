#include "monitorRouteCoverage.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <cmath>
#include <ctime>

void MonitorRouteCoverage::populateRoutes() {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }

    const char* countRoutesQuery = "SELECT COUNT(*) FROM routes;";
    PGresult* res = PQexec(conn, countRoutesQuery);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "COUNT query failed: " << PQerrorMessage(conn) << std::endl;
        if (res) PQclear(res);
        return;
    }

    int count = std::stoi(PQgetvalue(res, 0, 0));
    if (res) PQclear(res);

    if (count > 0) {
        return; // Routes table is already populated.
    }

    const char* insertRouteQuery = "INSERT INTO routes (pos_x, pos_y, visited, visited_time) VALUES ($1, $2, $3, $4);";
    for (double y = 0.01; y <= 6.0; y += 0.02) {
        char posY[6];
        snprintf(posY, sizeof(posY), "%.2f", y);
        const char* paramValues[4] = { "6.0", posY, "false", nullptr };

        res = PQexecParams(conn, insertRouteQuery, 4, nullptr, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "INSERT INTO routes failed: " << PQerrorMessage(conn) << std::endl;
        }
        if (res) PQclear(res);
    }
    PQfinish(conn);
    std::cout << "Routes populated successfully." << std::endl;
}

void MonitorRouteCoverage::run() {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }

    while (true) {
        const char* checkRoutesQuery = R"(
            SELECT pos_x, pos_y, visited_time 
            FROM routes 
            WHERE visited = true AND visited_time < (NOW() - INTERVAL '5 minutes');
        )";

        PGresult* res = PQexec(conn, checkRoutesQuery);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "SELECT query failed: " << PQerrorMessage(conn) << std::endl;
            if (res) PQclear(res);
            continue;
        }

        int rows = PQntuples(res);
        for (int i = 0; i < rows; ++i) {
            double posX = std::stod(PQgetvalue(res, i, 0));
            double posY = std::stod(PQgetvalue(res, i, 1));
            std::cout << "ALARM: route(" << posX << ", " << posY << ") not visited in the last 5 minutes." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
        if (res) PQclear(res);
    }
    PQfinish(conn);
}
