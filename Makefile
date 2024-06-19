CXX = g++
CXXFLAGS = -std=c++11 -g -I/usr/include/hiredis -I/usr/include -I/usr/include/postgresql
LDFLAGS = -lhiredis -lpq

all: control_center drone monitor

# Target for control_center
control_center: control_center/obj/control_center.o control_center/obj/main.o
	$(CXX) -o control_center/bin/control_center control_center/obj/control_center.o control_center/obj/main.o $(LDFLAGS)

control_center/obj/control_center.o: control_center/src/control_center.cpp
	$(CXX) $(CXXFLAGS) -c control_center/src/control_center.cpp -o control_center/obj/control_center.o

control_center/obj/main.o: control_center/src/main.cpp
	$(CXX) $(CXXFLAGS) -c control_center/src/main.cpp -o control_center/obj/main.o

# Target for drone
drone: drone/obj/drone.o drone/obj/main.o
	$(CXX) -o drone/bin/drone drone/obj/drone.o drone/obj/main.o $(LDFLAGS)

drone/obj/drone.o: drone/src/drone.cpp
	$(CXX) $(CXXFLAGS) -c drone/src/drone.cpp -o drone/obj/drone.o

drone/obj/main.o: drone/src/main.cpp
	$(CXX) $(CXXFLAGS) -c drone/src/main.cpp -o drone/obj/main.o

# Target for monitor
monitor: monitor/obj/main.o monitor/obj/monitorRouteCoverage.o monitor/obj/monitorBatteryCheck.o monitor/obj/monitorPosition.o monitor/obj/monitorDataIntegrity.o monitor/obj/system_availability_monitor.o
	$(CXX) -o monitor/bin/monitor monitor/obj/main.o monitor/obj/monitorRouteCoverage.o monitor/obj/monitorBatteryCheck.o monitor/obj/monitorPosition.o monitor/obj/monitorDataIntegrity.o monitor/obj/system_availability_monitor.o $(LDFLAGS)

monitor/obj/main.o: monitor/src/main.cpp
	$(CXX) $(CXXFLAGS) -c monitor/src/main.cpp -o monitor/obj/main.o

monitor/obj/monitorRouteCoverage.o: monitor/src/monitorRouteCoverage.cpp
	$(CXX) $(CXXFLAGS) -c monitor/src/monitorRouteCoverage.cpp -o monitor/obj/monitorRouteCoverage.o

monitor/obj/monitorBatteryCheck.o: monitor/src/monitorBatteryCheck.cpp
	$(CXX) $(CXXFLAGS) -c monitor/src/monitorBatteryCheck.cpp -o monitor/obj/monitorBatteryCheck.o

monitor/obj/monitorPosition.o: monitor/src/monitorPosition.cpp
	$(CXX) $(CXXFLAGS) -c monitor/src/monitorPosition.cpp -o monitor/obj/monitorPosition.o

monitor/obj/monitorDataIntegrity.o: monitor/src/monitorDataIntegrity.cpp
	$(CXX) $(CXXFLAGS) -c monitor/src/monitorDataIntegrity.cpp -o monitor/obj/monitorDataIntegrity.o

monitor/obj/system_availability_monitor.o: monitor/src/system_availability_monitor.cpp
	$(CXX) $(CXXFLAGS) -c monitor/src/system_availability_monitor.cpp -o monitor/obj/system_availability_monitor.o

.PHONY: clean
clean:
	rm -f control_center/obj/*.o control_center/bin/control_center
	rm -f drone/obj/*.o drone/bin/drone
	rm -f monitor/obj/*.o monitor/bin/monitor

