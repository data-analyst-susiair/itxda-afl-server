# ITXDA Pipeline

This project is a data pipeline application built with FastAPI and Python. It orchestrates data processing tasks, specifically for logbook sheets and entries.

## Features

- **FastAPI Server**: Exposes endpoints to trigger pipelines.
- **Data Processing**: Uses Pandas, DuckDB, and SQLAlchemy for data manipulation and storage.
- **Pipeline Orchestration**: Manages workflows for logbook data.

## Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) (for dependency management)

## Installation

1.  **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd itxda_pipeline
    ```

2.  **Install dependencies:**

    ```bash
    uv sync
    ```

## Configuration

The application is configured using environment variables. The following variables can be set to customize the application's behavior and database connections:

### Mandatory Variables

The application will fail to start if any of these variables are missing.

| Variable | Description |
|----------|-------------|
| `POSTGRES_DB_URL` | Connection URL for the PostgreSQL database. |
| `SSH_HOST` | Hostname for the SSH tunnel. |
| `SSH_USERNAME` | Username for SSH authentication. |
| `SSH_PASSWORD` | Password for SSH authentication. |
| `MYSQL_USERNAME` | Username for MySQL authentication. |
| `MYSQL_PASSWORD` | Password for MySQL authentication. |
| `MYSQL_DB_NAME` | Name of the MySQL database. |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SSH_PORT` | Port for SSH connection. | `22` |
| `SSH_REMOTE_BIND_PORT` | Remote bind port for SSH tunnel. | `3306` |
| `MYSQL_HOST` | Hostname for the MySQL database. | `localhost` |
| `IS_TESTING` | Enable testing mode. | `true` |
| `IS_DEBUGGING` | Enable debugging mode. | `False` |
| `DATA_ANALYST_USER_ID` | User ID for data analyst operations. | `41` |
| `SECRET_KEY` | Secret key for API authentication (X-Key header). | `None` |
| `CAESAR_SHIFT` | Shift value for the Caesar cipher used in authentication. | `3` |

## Running the Application

### Local Development

To start the API server locally:

```bash
uv run main.py
```

The server will start at `http://0.0.0.0:8000`.

### Using Docker

1.  **Build the Docker image:**

    ```bash
    docker build -t itxda-pipeline .
    ```

2.  **Run the container:**

    ```bash
    docker run -p 8000:8000 itxda-pipeline
    ```

## API Endpoints

### `GET /afl`

Triggers the execution of the logbook pipelines.

**Authentication:**
Requires an `X-Key` header containing the `SECRET_KEY` encrypted with a Caesar cipher using the configured `CAESAR_SHIFT` (default 3).

-   **Response:**
    -   `200 OK`: Pipeline execution completed successfully.
    -   `500 Internal Server Error`: Pipeline execution failed.
    -   `401 Unauthorized`: Invalid Secret Key.

### `GET /health`

Health check endpoint.

-   **Response:**
    -   Returns the current testing status.

## Project Structure

-   `main.py`: Application entry point.
-   `src/api/`: FastAPI application definition.
-   `src/pipelines/`: Pipeline logic (logbook entry, logbook sheet).
-   `src/config/`: Configuration settings.
-   `src/db/`: Database connection handling.
