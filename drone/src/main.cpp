#include "drone.h"
#include <thread>

int main() {
    auto mainDrone = std::make_shared<Drone>(0, 1);  // Creazione del primo drone con id = 0

    // Avvia i thread per il drone principale
    mainDrone->startThreads();

    // Mantieni il programma in esecuzione per 1 ora
    std::this_thread::sleep_for(std::chrono::hours(24));

    return 0;
}
