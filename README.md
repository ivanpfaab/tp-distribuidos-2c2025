# Go Echo Server and Client with Docker

This project contains a simple echo server and client application written in Go, containerized with Docker for easy deployment and testing.

## Project Structure

```
tp-distribuidos-2c2025/
├── server/
│   ├── main.go          # Echo server implementation
│   ├── go.mod           # Go module file
│   ├── Dockerfile       # Server container configuration
│   └── .dockerignore    # Docker ignore file
├── client/
│   ├── main.go          # Client implementation
│   ├── go.mod           # Go module file
│   ├── Dockerfile       # Client container configuration
│   └── .dockerignore    # Docker ignore file
├── docker-compose.yml   # Docker Compose configuration
└── README.md           # This file
```

## Features

- **RabbitMQ-Based Server**: Server that consumes batch messages from RabbitMQ and processes CSV data
- **CSV Batch Client**: Client that reads CSV files and sends batches via RabbitMQ message queues
- **Data Processing Pipeline**: Year filter, time filter, amount filter, join workers, and aggregation
- **Multiple Client Support**: Supports concurrent clients with unique client IDs
- **Docker Support**: All applications are containerized for easy deployment
- **Docker Compose**: Orchestrates all services with proper networking and dependencies
- **Message Queue Architecture**: All inter-service communication via RabbitMQ

## Prerequisites

- Docker and Docker Compose installed on your system
- Go 1.21+ (for local development)

## Quick Start

### Using Makefile (Recommended)

The project includes a comprehensive Makefile with convenient commands:

1. **Start both services:**
   ```bash
   make up
   # or
   make start
   ```

2. **Watch the client process:**
   - The client will automatically connect to the server
   - It will read messages from `input.txt` and send them to the server
   - You'll see both the sent messages and server responses
   - The client will stop when it reaches the end of the file or encounters "exit"

3. **Stop the services:**
   ```bash
   make down
   # or
   make stop
   ```

4. **View available commands:**
   ```bash
   make help
   ```

### Using Docker Compose

1. **Start both services:**
   ```bash
   docker-compose up --build
   ```

2. **Interact with the client:**
   - The client will automatically connect to the server
   - Type messages and press Enter to send them
   - Type `exit` to quit the client

3. **Stop the services:**
   ```bash
   docker-compose down
   ```

### Manual Docker Commands

1. **Build and run the server:**
   ```bash
   # Build server image
   docker build -t server ./server
   
   # Run server container
   docker run -p 8080:8080 --name server server
   ```

2. **Build and run the client (in another terminal):**
   ```bash
   # Build client image
   docker build -t client ./client
   
   # Run client container (connect to server)
   docker run -it --rm --network host client
   ```

### Local Development

1. **Run the server locally:**
   ```bash
   cd server
   go run main.go
   ```

2. **Run the client locally (in another terminal):**
   ```bash
   cd client
   # Update server address in main.go to "localhost:8080"
   go run main.go
   ```

## Usage

1. Start the echo server (it will listen on port 8080)
2. The client will automatically connect and read from `input.txt`
3. Each line in the file will be sent as a message to the server
4. The server will echo back each message with "Echo: " prefix
5. The client stops when it reaches the end of the file or encounters "exit"

### Custom Input Files

To use your own input file:

```bash
# Using docker-compose with custom file
docker-compose run --rm -v $(pwd)/yourfile.txt:/app/input.txt client ./main /app/input.txt

# Using make with custom file
make run-client-file FILE=yourfile.txt
```

## Network Configuration

- **Server Port**: 8080
- **Protocol**: TCP
- **Docker Network**: `echo-network` (bridge driver)

## Docker Images

- **server**: Contains the TCP echo server
- **client**: Contains the interactive client

## Troubleshooting

- **Connection refused**: Make sure the server is running before starting the client
- **Port already in use**: Change the port mapping in docker-compose.yml if 8080 is occupied
- **Client can't connect**: Ensure both containers are on the same Docker network

## Makefile Commands

The project includes a comprehensive Makefile with the following commands:

### Basic Commands
- `make help` - Show all available commands
- `make docker-compose-up` / `make start` - Start both services with docker-compose
- `make docker-compose-down` / `make stop` - Stop all services
- `make docker-compose-logs` - Show logs from all services
- `make status` - Show container and image status

### Individual Service Commands
- `make build-server` - Build server Docker image
- `make build-client` - Build client Docker image
- `make build-all` - Build both images
- `make run-server` - Run server container only
- `make run-client` - Run client container only (interactive)
- `make logs-server` - Show server logs only
- `make logs-client` - Show client logs only

### Development Commands
- `make dev-server` - Run server locally (without Docker)
- `make dev-client` - Run client locally (without Docker)
- `make test-connection` - Test server connection with netcat

### Cleanup Commands
- `make clean` - Remove containers and images
- `make clean-all` - Remove everything including volumes
- `make restart` - Restart all services
- `make rebuild` - Rebuild and recreate services

## Development

To modify the applications:

1. Edit the Go source files in `server/` or `client/`
2. Rebuild the Docker images: `make rebuild`
3. Test your changes

## License

This project is for educational purposes.