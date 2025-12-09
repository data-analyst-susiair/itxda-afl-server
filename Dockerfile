# Use an official Python runtime as a parent image
FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory in the container
WORKDIR /app

# Copy the project configuration files
COPY pyproject.toml uv.lock ./

# Install dependencies
# --frozen ensures we use the exact versions in uv.lock
# --no-install-project skips installing the project itself as a package (since we run it directly)
RUN uv sync --frozen --no-install-project

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 8000

# Define the command to run the application
# We use `uv run` to ensure it runs in the environment created by `uv sync`
CMD ["uv", "run", "main.py"]
