import os
import psycopg2
from psycopg2.extensions import connection
from dotenv import load_dotenv

load_dotenv()

class PostgresConnection:
    """Manages a connection to the PostgreSQL database."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        self.host = host
        self.port = port

        # default: connect to data source
        self.dbname = dbname or os.environ.get("COM_DB_NAME")
        self.user = user or os.environ.get("COM_DB_USER")
        self.password = password or os.environ.get("COM_DB_PASSWORD")
        self._conn: connection | None = None

    def connect(self) -> connection:
        self._conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
        return self._conn

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> connection:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()
