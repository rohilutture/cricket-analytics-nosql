# CricketIQ: Cricket Analytics Pipeline

CricketIQ is a comprehensive ETL and analytics pipeline for cricket data. It leverages **MongoDB** for document-based storage and analytics, and **Neo4j** for graph-based analysis (e.g., batter vs. bowler matchups). The project is designed to process Cricsheet JSON data.

## Features

- **Data Ingestion (ETL):** Transforms Cricsheet JSON match files into a structured format.
- **MongoDB Storage:**
  - `matches`: Stores match metadata and outcome information.
  - `deliveries`: Stores flattened delivery-level data for deep analytics.
- **Graph Analysis (Neo4j):**
  - Models `Players`, `Teams`, and `Matches`.
  - Creates `FACED` relationships to analyze batter-bowler interactions.
- **Analytics:** Includes Python scripts for aggregation queries and Cypher queries for graph analysis.
- **Dockerized Environment:** Easy setup using Docker Compose.

## Prerequisites

- [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- [Python 3.8+](https://www.python.org/) (if running scripts locally)

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Configure Environment Variables

Create a `.env` file based on `.env.example` (if available) or use the defaults.

```bash
cp .env.example .env
```

**Default Environment Variables:**

| Variable | Default | Description |
| :--- | :--- | :--- |
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection URI |
| `MONGO_DB` | `cricket_iq` | Database name |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt URI |
| `NEO4J_USER` | `neo4j` | Neo4j Username |
| `NEO4J_PASSWORD` | `password` | Neo4j Password (or `neo4j123` in Docker) |

### 3. Start Services

Use Docker Compose to start MongoDB and Neo4j.

```bash
docker-compose up -d
```

This will start:
- **MongoDB** on port `27017`
- **Neo4j** on ports `7474` (HTTP) and `7687` (Bolt)

## Usage

### 1. Download Data

Download JSON match data from [Cricsheet](https://cricsheet.org/downloads/). Extract the files to a local directory (e.g., `./data/matches`).

### 2. ETL: Load Data into MongoDB

Run the `etl_cricsheet_to_mongo.py` script to load the JSON data into MongoDB.

```bash
# Install dependencies first
pip install -r requirements.txt

# Run the ETL script
python etl_cricsheet_to_mongo.py --data_dir ./data/matches
```

### 3. Load Data into Neo4j

Once data is in MongoDB, populate the Neo4j graph.

```bash
python neo4j_loader.py --limit 100000  # Optional limit for testing
```

### 4. Run Analytics

You can run the example analytics script to see some insights.

```bash
python mongo_analytics_examples.py
```

## Analytics & Queries

### MongoDB
The `mongo_analytics_examples.py` script demonstrates:
- Top batters by runs
- Top bowlers by wickets
- Head-to-head stats (e.g., Kohli vs. Southee)

### Neo4j (Cypher)
Check `cypher_queries.cypher` for useful queries, such as:
- Bowler vs. Batter records
- Partnership proxies
- PageRank for central players

Open the Neo4j Browser at `http://localhost:7474` (default login: `neo4j` / `neo4j123` from `docker-compose.yml`) and run the queries.

## Project Structure

```
├── docker-compose.yml          # Docker services definition
├── etl_cricsheet_to_mongo.py   # ETL script (JSON -> MongoDB)
├── neo4j_loader.py             # Graph loader (MongoDB -> Neo4j)
├── mongo_analytics_examples.py # Example PyMongo queries
├── cypher_queries.cypher       # Example Cypher queries
├── requirements.txt            # Python dependencies
└── Dockerfile                  # Dockerfile for analytics container
```
