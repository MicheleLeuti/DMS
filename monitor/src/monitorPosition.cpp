#include "monitorPosition.h"

void MonitorPosition::run() {
    PGconn* conn = PQconnectdb("dbname=dronelogdb user=droneuser password=dronepassword hostaddr=127.0.0.1 port=5432");
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return;
    }
    while (true) {
        const char* query = R"(
            SELECT drone_id, pos_x, pos_y
            FROM drone
            WHERE pos_x < 0.0 OR pos_x > 6.0 OR pos_y < 0.0 OR pos_y > 6.0;
        )";

        PGresult* res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "SELECT query failed: " << PQerrorMessage(conn) << std::endl;
            if (res) PQclear(res);
            PQfinish(conn);
            return;
        }

        int rows = PQntuples(res);
        if (rows > 0) {
            for (int i = 0; i < rows; ++i) {
                std::string droneId = PQgetvalue(res, i, 0);
                std::string posX = PQgetvalue(res, i, 1);
                std::string posY = PQgetvalue(res, i, 2);
                std::cout << "Position anomaly detected! Drone ID: " << droneId << ", Position: (" << posX << ", " << posY << ")" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(10));
            }
        } 
        if (res) PQclear(res);
        
    }
    PQfinish(conn);
}
