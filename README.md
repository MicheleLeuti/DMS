
# Drone Monitoring System Setup Guide

This guide provides detailed instructions on how to set up and run the Drone Monitoring System, including the creation and configuration of the PostgreSQL database.

## Prerequisites

Before you begin, make sure you have the following software installed:

- PostgreSQL
- Redis
- GCC and Make
- libhiredis-dev
- libpq-dev

## Installation

### Step 1: Install Required Packages

Open a terminal and run the following commands to install the required packages:

```sh
sudo apt-get update
sudo apt-get install make g++ postgresql redis-server libhiredis-dev libpq-dev
```

### Step 2: Configure PostgreSQL

1. **Switch to the postgres user**:
    ```sh
    sudo -i -u postgres
    ```

2. **Create a new PostgreSQL user**:
    ```sh
    createuser --interactive --pwprompt
    ```

    When prompted, enter the following:
    - Username: `droneuser`
    - Password: `dronepassword`
    - Superuser: `n`
    - Create new roles: `n`
    - Create databases: `n`

3. **Create a new PostgreSQL database**:
    ```sh
    createdb dronelogdb -O droneuser
    ```

4. **Grant all privileges to the new user**:
    ```sh
    psql -c "GRANT ALL PRIVILEGES ON DATABASE dronelogdb TO droneuser;"
    ```

5. **Exit the postgres user**:
    ```sh
    exit
    ```

### Step 3: Clone the Repository

Clone the repository containing the Drone Monitoring System code:
repository url: https://github.com/MicheleLeuti/DMS

```sh
git clone <repository-url>
cd <repository-directory>
```

### Step 4: Build the Project

Navigate to the project directory and run `make` to build the project:

```sh
make
```

## Running the Application

### Initialize the Database

1. **Run the Monitor to Initialize the Database**:

    The monitor will create the necessary tables and populate the `area_coverage` table if it is empty. It will also reset the table if it is already populated.

    ```sh
    ./monitor/bin/monitor
    ```

    This will perform the initial setup and populate the `area_coverage` table with all the necessary points.

2. **Running Control Center and Drone Programs**:

    Open two new terminal windows. In the first terminal, navigate to the control_center directory and run the control_center program:

    ```sh
    ./control_center/bin/control_center
    ```

    In the second terminal, navigate to the drone directory and run the drone program:

    ```sh
    ./drone/bin/drone
    ```

## Monitoring and Logging

The monitor will start after an initial delay of 30 minutes, and it will periodically check the area coverage, log the drone statuses, and print the results.

### Clean Up

To clean up the database, you can drop the database and recreate it using the steps mentioned in **Step 2**.

---

This guide should help you set up and run the Drone Monitoring System on a new computer. If you encounter any issues, please refer to the error messages for troubleshooting or consult the relevant documentation for PostgreSQL, Redis, and the libraries used.
