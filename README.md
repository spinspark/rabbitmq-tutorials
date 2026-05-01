# Rust code for RabbitMQ tutorials (using Lapin)

Here you can find the Rust code examples for [RabbitMQ
tutorials](https://www.rabbitmq.com/getstarted.html).

The examples use [lapin](https://github.com/CleverCloud/lapin) client library.

These tutorials assume a RabbitMQ server node running locally using default ports.

## Requirements

* [Rust and Cargo](https://www.rust-lang.org/tools/install)

## Code
Each cargo command should be launched in a separate shell.

#### [Tutorial one: "Hello World!"](https://www.rabbitmq.com/tutorials/tutorial-one-python.html)

    cargo run --bin receive
    cargo run --bin send

#### [Tutorial two: Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python.html)

    cargo run --bin worker
    cargo run --bin new_task "hi" # specify a custom message

#### [Tutorial three: Publish/Subscribe](https://www.rabbitmq.com/tutorials/tutorial-three-python.html)

    cargo run --bin receive_logs
    cargo run --bin emit_log "hi" # specify a custom message

#### [Tutorial four: Routing](https://www.rabbitmq.com/tutorials/tutorial-four-python.html)

    cargo run --bin receive_logs_direct info error # specify log levels
    cargo run --bin emit_log_direct error "help!" # specify severity and custom message

#### [Tutorial five: Topics](https://www.rabbitmq.com/tutorials/tutorial-five-python.html)

    cargo run --bin receive_logs_topic kern.* # specify topic filter
    cargo run --bin emit_log_topic kern.mem "No memory left!" # specify topic and message

#### [Tutorial six: RPC](https://www.rabbitmq.com/tutorials/tutorial-six-python.html)

    cargo run --bin rpc_server
    cargo run --bin rpc_client

# Quick Start: RabbitMQ on Linux

The fastest way to get RabbitMQ running is via **Docker**, which avoids dependency issues.

### 1. Docker Method (Fastest)
Run this command to pull the image and start the server with the management dashboard:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

*   **Standard Port:** `5672`
*   **Management UI:** `http://localhost:15672` (User/Pass: `guest`/`guest`)

---

### 2. Native Method (Ubuntu/Debian)
If you don't use Docker, run these three commands:

```bash
# Install the server
sudo apt update && sudo apt install rabbitmq-server -y

# Enable the Web Dashboard
sudo rabbitmq-plugins enable rabbitmq_management

# Start the service
sudo systemctl start rabbitmq-server
```

---

### 3. Connection Test
To verify it is running, check the status:
```bash
sudo rabbitmqctl status
```

**Note:** The `guest` user is restricted to `localhost`. For remote connections, you must create a new user.
