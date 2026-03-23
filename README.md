# db-dump

CDC (Change Data Capture) tool that captures real-time changes from a PostgreSQL database and writes them to human-readable text files using [Debezium](https://debezium.io/). This will help LLM AI Agents to debug and understand database-backed applications. Typical MCP tools are too limited and consume too many tokens. In this approach AI can use standard UNIX tools giving it more power.

## How it works

- Connects to PostgreSQL's logical replication stream via Debezium
- Captures every INSERT, UPDATE, and DELETE on all tables in the `public` schema
- Writes each change event to an individual timestamped file in `logs/requests/`
- Maintains a master log in `logs/db-dump.txt`
- Tracks its position using an offset file, so it resumes where it left off on restart

## Setup

1. Enable logical replication in `postgresql.conf`:
   ```
   wal_level = logical
   max_replication_slots = 4
   max_wal_senders = 4
   ```
   Restart PostgreSQL after changing these.

2. Grant replication permissions to your database user:
   ```sql
   ALTER ROLE myuser REPLICATION;
   ```

3. Build and run:
   ```bash
   mvn clean package
   export DB_NAME=mydb DB_USER=myuser DB_PASSWORD=mypassword
   java -jar target/db-dump.jar
   ```

## Prerequisites

- Java 21+
- PostgreSQL with logical replication enabled (`wal_level = logical`)
- A database user with replication permissions

## Configuration

Set the following environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_NAME` | Yes | — | PostgreSQL database name |
| `DB_USER` | Yes | — | PostgreSQL user |
| `DB_PASSWORD` | Yes | — | PostgreSQL password |
| `DB_HOST` | No | `localhost` | PostgreSQL host |
| `DB_PORT` | No | `5432` | PostgreSQL port |
| `OFFSET_FILE` | No | `data/offsets.dat` | Path to store CDC offsets |
| `LOGS_DIR` | No | `logs` | Directory for output files |

## Build

```bash
mvn clean package
```

## Run

```bash
export DB_NAME=mydb
export DB_USER=myuser
export DB_PASSWORD=mypassword
java -jar target/db-dump.jar
```

## Output format

Each change event is written as a text file like:

```
>>>> CHANGE
Operation: INSERT
Table: users
Key: a1b2c3d4
Timestamp: 2025-02-16T10:30:45.123Z

Before: null

------------------------

After:
{
  "id" : "a1b2c3d4-e5f6-...",
  "name" : "example"
}
```

## Output

- **Master log** (`logs/db-dump.txt`): One line per change: `<filename> <operation> <table>`
- **Per-event files** (`logs/requests/`): Named `HH-mm-ss.SSS_db-cdc_<OPERATION>_<table>_<key>.txt`. Content: operation, table, key, timestamp, before/after state as JSON.

## Sample AI Prompt

All database changes are logged to `db-logs` with file names `HH-mm-ss.SSS_db-cdc_<OPERATION>_<table>_<short-key>.txt`. Use these to analyze database activity. Use standard UNIX tools to narrow down the files.
eg:
list all changes to the users table `ls | grep '_users_'`
show all DELETE operations `ls | grep '_DELETE_'`
read the last change `ls | tail -n 1 | xargs cat`
show all changes in order `cat db-dump.txt`

## License

This project is not yet licensed. See [LICENSE](LICENSE) for details once a license is chosen.
