#include "monitorDataIntegrity.h"
#include <libpq-fe.h>
#include <iostream>
#include <thread>
#include <chrono>

void MonitorDataIntegrity::run() {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }
    while (true) {
        // Verifica di duplicati
        const char* duplicateQuery = R"(
            SELECT drone_id, COUNT(*)
            FROM drone
            GROUP BY drone_id, log_time
            HAVING COUNT(*) > 1;
        )";

        PGresult* res = PQexec(conn, duplicateQuery);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "Duplicate check query failed: " << PQerrorMessage(conn) << std::endl;
        } else {
            int rows = PQntuples(res);
            if (rows > 0) {
                std::cout << "Data integrity issue detected: Duplicates found!" << std::endl;
                for (int i = 0; i < rows; ++i) {
                    std::string droneId = PQgetvalue(res, i, 0);
                    std::string count = PQgetvalue(res, i, 1);
                    std::cout << "Drone ID: " << droneId << ", Duplicate entries: " << count << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(10));
                }
            } 
        }
        if (res) PQclear(res);

        // Verifica di nulli in colonne obbligatorie
        const char* nullCheckQuery = R"(
            SELECT COUNT(*)
            FROM drone
            WHERE drone_id IS NULL OR status IS NULL OR battery_seconds IS NULL OR pos_x IS NULL OR pos_y IS NULL OR log_time IS NULL;
        )";

        res = PQexec(conn, nullCheckQuery);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "Null check query failed: " << PQerrorMessage(conn) << std::endl;
        } else {
            int count = std::stoi(PQgetvalue(res, 0, 0));
            if (count > 0) {
                std::cout << "Data integrity issue detected: Null values found!" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(10));
            }
        }
        if (res) PQclear(res);

        // Verifica di valori fuori range
        const char* rangeCheckQuery = R"(
            SELECT COUNT(*)
            FROM drone
            WHERE battery_seconds < 0 OR pos_x < 0 OR pos_y < 0;
        )";

        res = PQexec(conn, rangeCheckQuery);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "Range check query failed: " << PQerrorMessage(conn) << std::endl;
        } else {
            int count = std::stoi(PQgetvalue(res, 0, 0));
            if (count > 0) {
                std::cout << "Data integrity issue detected: Out of range values found!" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(10));
            }    
        }
        if (res) PQclear(res);

        
    }
    PQfinish(conn);
}
