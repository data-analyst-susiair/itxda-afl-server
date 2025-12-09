import os
import sys

print("Starting test_settings.py")

# Clear env vars to ensure it fails
keys = [
    "POSTGRES_DB_URL",
    "SSH_HOST",
    "SSH_USERNAME",
    "SSH_PASSWORD",
    "MYSQL_USERNAME",
    "MYSQL_PASSWORD",
    "MYSQL_DB_NAME",
]
for key in keys:
    if key in os.environ:
        del os.environ[key]

try:
    # Add current directory to sys.path
    sys.path.append(os.getcwd())
    from src.config.settings import settings

    print("Settings loaded successfully (Unexpected)")
except ValueError as e:
    print(f"Caught expected error: {e}")
except Exception as e:
    print(f"Caught unexpected error: {e}")
