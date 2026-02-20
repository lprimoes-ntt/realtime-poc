import logging
import random
import string
from typing import List

import pyodbc

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Connection strings for the two databases based on docker-compose.yml
DB1_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=ShoriDB_1;"  # Use DB explicitly defined in Debezium config
    "UID=sa;"
    "PWD=YourStr0ngP@ssw0rd!;"
    "TrustServerCertificate=yes;"
)

DB2_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1434;"
    "DATABASE=ShoriDB_3;"
    "UID=sa;"
    "PWD=YourStr0ngP@ssw0rd!;"
    "TrustServerCertificate=yes;"
)


def random_string(length: int = 10) -> str:
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


def setup_database(conn: pyodbc.Connection) -> None:
    """Ensure the target table exists."""
    cursor = conn.cursor()
    # Create a simple test table if it doesn't exist
    cursor.execute(
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='test_cdc' AND xtype='U')
        BEGIN
            CREATE TABLE test_cdc (
                id INT IDENTITY(1,1) PRIMARY KEY,
                data_payload VARCHAR(255) NOT NULL,
                created_at DATETIME DEFAULT GETDATE()
            )

            -- Attempt to enable CDC on the table since it's newly created
            EXEC sys.sp_cdc_enable_table
                @source_schema = N'dbo',
                @source_name = N'test_cdc',
                @role_name = NULL;
        END
        """
    )
    # Enable CDC on the table if it's not already enabled (Requires DB to have CDC enabled)
    # Note: For Debezium to work, the Database itself must have CDC enabled:
    # EXEC sys.sp_cdc_enable_db;
    # EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'test_cdc', @role_name = NULL;

    conn.commit()
    cursor.close()


def insert_batch(conn: pyodbc.Connection, batch_size: int = 50) -> None:
    """Insert a batch of rows quickly."""
    cursor = conn.cursor()
    query = "INSERT INTO test_cdc (data_payload) VALUES (?)"

    # Generate batch data
    data: List[tuple[str]] = [(random_string(20),) for _ in range(batch_size)]

    try:
        cursor.executemany(query, data)
        conn.commit()
        logger.info(f"Successfully inserted batch of {batch_size} rows.")
    except Exception as e:
        logger.error(f"Failed to insert batch: {e}")
        conn.rollback()
    finally:
        cursor.close()


def run_workload_generator() -> None:
    """Simulates a workload that spikes intermittently."""
    logger.info("Starting workload generator...")

    try:
        logger.info("Connecting to DB1...")
        conn1 = pyodbc.connect(DB1_CONN_STR)
        setup_database(conn1)

        logger.info("Connecting to DB2...")
        conn2 = pyodbc.connect(DB2_CONN_STR)
        setup_database(conn2)

    except pyodbc.Error as e:
        logger.error(f"Failed to connect to databases: {e}")
        logger.error(
            "Make sure ODBC Driver 18 for SQL Server is installed on your machine."
        )
        return

    logger.info("Databases connected and initialized. Starting workload loop...")

    try:
        while True:
            # Hammering both databases without sleeping
            batch_size = random.randint(5000, 10000)

            logger.info(f"Hammering BOTH databases with {batch_size} rows each!")
            insert_batch(conn1, batch_size)
            insert_batch(conn2, batch_size)

    except KeyboardInterrupt:
        logger.info("Workload generator stopped by user.")
    finally:
        conn1.close()
        conn2.close()


if __name__ == "__main__":
    run_workload_generator()
