import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Postgres Configuration
    POSTGRES_DB_URL = os.getenv("POSTGRES_DB_URL")

    # SSH Configuration
    SSH_HOST = os.getenv("SSH_HOST")
    SSH_USERNAME = os.getenv("SSH_USERNAME")
    SSH_PASSWORD = os.getenv("SSH_PASSWORD")
    SSH_PORT = int(os.getenv("SSH_PORT", "22"))
    SSH_REMOTE_BIND_ADDRESS = (
        "127.0.0.1",
        int(os.getenv("SSH_REMOTE_BIND_PORT", "3306")),
    )

    # MySQL Configuration
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB_NAME = os.getenv("MYSQL_DB_NAME")

    # Application Settings
    IS_TESTING = os.getenv("IS_TESTING", "true").lower() == "true"
    IS_DEBUGGING = os.getenv("IS_DEBUGGING", "False").lower() == "true"
    DATA_ANALYST_USER_ID = int(os.getenv("DATA_ANALYST_USER_ID", "41"))

    def __init__(self):
        mandatory_vars = [
            "POSTGRES_DB_URL",
            "SSH_HOST",
            "SSH_USERNAME",
            "SSH_PASSWORD",
            "MYSQL_USERNAME",
            "MYSQL_PASSWORD",
            "MYSQL_DB_NAME",
        ]
        missing_vars = [var for var in mandatory_vars if getattr(self, var) is None]
        if missing_vars:
            raise ValueError(
                f"Missing mandatory environment variables: {', '.join(missing_vars)}"
            )


settings = Settings()
