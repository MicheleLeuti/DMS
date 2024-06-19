#include "monitorBatteryCheck.h"

void MonitorBatteryCheck::run() {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }
    while (true) {
        // Connessione al database

        // Query per contare i droni con battery_seconds = 0 e status 'in_mission' o 'ready'
        const char* query = R"(
            SELECT COUNT(*)
            FROM drone
            WHERE battery_seconds = 0
            AND (status = 'in_mission' OR status = 'ready' OR status = 'broken');
        )";

        PGresult* res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "COUNT query failed: " << PQerrorMessage(conn) << std::endl;
            if (res) PQclear(res);
            PQfinish(conn);
            return;
        }

        // Estrazione del risultato della query
        int count = std::stoi(PQgetvalue(res, 0, 0));
        if (count > 0){
            std::cout << "ALARM: #Drones with low battery: " << count << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
        if (res) PQclear(res);
    }
    PQfinish(conn);
}
