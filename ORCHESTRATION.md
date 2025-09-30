# System Orchestration Guide

## Quick Start

1. **Create shared network** (one-time setup):
   ```bash
   docker network create shared-network
   ```

2. **Start Query Orchestrator** (Terminal 1):
   ```bash
   cd server/controller/query-orchestrator
   make up
   ```

3. **Start Main System** (Terminal 2):
   ```bash
   make docker-compose-up
   ```

**RabbitMQ Management UI**: http://localhost:15673 (admin/password)