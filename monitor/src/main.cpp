#include "monitorRouteCoverage.h"
#include "monitorBatteryCheck.h"
#include "monitorPosition.h"
#include "monitorDataIntegrity.h"
#include "system_availability_monitor.h"
#include <csignal>
#include <iostream>
#include <libpq-fe.h>
#include <thread>

MonitorRouteCoverage monitorRoute;
MonitorBatteryCheck monitorBattery;
MonitorPosition monitorPos;
MonitorDataIntegrity dataIntegrityMonitor;
SystemAvailabilityMonitor monitorSys;

void signalHandler(int signum) {
    std::cout << "Interrupt signal (" << signum << ") received.\n";
    exit(signum);
}

void createTables() {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }
    const char* dropDroneTable = "DROP TABLE IF EXISTS drone;";
    const char* dropDroneLogsTable = "DROP TABLE IF EXISTS drone_logs;";
    const char* dropRoutesTable = "DROP TABLE IF EXISTS routes;";
    const char* createDroneTable = R"(
        CREATE TABLE IF NOT EXISTS drone (
            id SERIAL PRIMARY KEY,
            drone_id VARCHAR(50),
            status VARCHAR(50),
            battery_seconds INT,
            pos_x DOUBLE PRECISION,
            pos_y DOUBLE PRECISION,
            log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    )";
    const char* createRoutesTable = R"(
        CREATE TABLE IF NOT EXISTS routes (
            id SERIAL PRIMARY KEY,
            pos_x DOUBLE PRECISION,
            pos_y DOUBLE PRECISION,
            visited BOOLEAN DEFAULT FALSE,
            visited_time TIMESTAMP
        );
    )";

    const char* createDroneLogsTable = R"(
        CREATE TABLE IF NOT EXISTS drone_logs (
            id SERIAL PRIMARY KEY,
            descrizione VARCHAR(255),
            log_time TIMESTAMP
        );
    )";

    PGresult* res = PQexec(conn, dropDroneTable);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "DROP TABLE drone failed: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);


    res = PQexec(conn, dropDroneLogsTable);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "DROP TABLE drone_logs failed: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);

    res = PQexec(conn, dropRoutesTable);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "DROP TABLE routes failed: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);

    res = PQexec(conn, createDroneTable);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "CREATE TABLE drone failed: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);

    res = PQexec(conn, createDroneLogsTable);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "CREATE TABLE drone_logs failed: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);

    res = PQexec(conn, createRoutesTable);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "CREATE TABLE routes failed: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);

    std::cout << "Tables created successfully." << std::endl;
    PQfinish(conn);
}

int main() {
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    createTables();
    monitorRoute.populateRoutes();
    // Creazione dei thread per monitorRoute, monitorBattery, monitorPos, dataIntegrity e monitorCommunication
    std::thread routeThread(&MonitorRouteCoverage::run, &monitorRoute);
    std::thread batteryThread(&MonitorBatteryCheck::run, &monitorBattery);
    std::thread positionThread(&MonitorPosition::run, &monitorPos);
    std::thread dataIntegrityThread(&MonitorDataIntegrity::run, &dataIntegrityMonitor);
    std::thread sysThread(&SystemAvailabilityMonitor::run, &monitorSys);
    // Unione dei thread ai thread principali
    routeThread.join();
    batteryThread.join();
    positionThread.join();
    dataIntegrityThread.join();
    sysThread.join();
    return 0;
}
