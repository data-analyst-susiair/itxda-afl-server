import pymysql
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from src.config.settings import settings
from contextlib import contextmanager

class DatabaseManager:
    def __init__(self):
        self.postgres_engine = create_engine(settings.POSTGRES_DB_URL)
        self.tunnel = None

    def get_postgres_engine(self):
        return self.postgres_engine

    @contextmanager
    def mysql_connection(self):
        """
        Context manager that establishes an SSH tunnel (if not testing)
        and yields a MySQL connection.
        """
        if settings.IS_TESTING:
            conn = pymysql.connect(
                host='localhost',
                user=settings.MYSQL_USERNAME,
                port=3306,
                password="susiair", # From notebook logic
                database="itxda",
            )
            try:
                yield conn
            finally:
                conn.close()
        else:
            with SSHTunnelForwarder(
                (settings.SSH_HOST, settings.SSH_PORT),
                ssh_username=settings.SSH_USERNAME,
                ssh_password=settings.SSH_PASSWORD,
                remote_bind_address=settings.SSH_REMOTE_BIND_ADDRESS,
                local_bind_address=('127.0.0.1', 0) # Let OS pick a random port
            ) as tunnel:
                conn = pymysql.connect(
                    host='127.0.0.1',
                    user=settings.MYSQL_USERNAME,
                    port=tunnel.local_bind_port,
                    password=settings.MYSQL_PASSWORD,
                    database=settings.MYSQL_DB_NAME,
                    cursorclass=pymysql.cursors.DictCursor # Using DictCursor for easier access
                )
                try:
                    yield conn
                finally:
                    conn.close()

db_manager = DatabaseManager()
