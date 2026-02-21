import logging
import random
import string
import concurrent.futures
from typing import List, Tuple, Any
import time

import pyodbc

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Connection strings for the two databases based on docker-compose.yml
DB1_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=ShoriDB_1;"
    "UID=sa;"
    "PWD=YourStr0ngP@ssw0rd!;"
    "TrustServerCertificate=yes;"
)

DB2_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1434;"
    "DATABASE=ShoriDB_2;"
    "UID=sa;"
    "PWD=YourStr0ngP@ssw0rd!;"
    "TrustServerCertificate=yes;"
)


def random_string(length: int = 10) -> str:
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


def setup_database(conn: pyodbc.Connection) -> None:
    """Ensure the target tables have seed data."""
    cursor = conn.cursor()
    
    # Check if we have seed users
    cursor.execute("SELECT COUNT(*) FROM dbo.Users")
    user_count = cursor.fetchone()[0]
    
    if user_count == 0:
        logger.info("Inserting seed data (Customers, Users, Projects)...")
        # Seed Customers
        cursor.execute("INSERT INTO dbo.Customers (name, industry) VALUES ('Acme Corp', 'Technology')")
        cursor.execute("INSERT INTO dbo.Customers (name, industry) VALUES ('Global Tech', 'Finance')")
        
        # Seed Users
        cursor.execute("INSERT INTO dbo.Users (full_name, email, role) VALUES ('Alice Admin', 'alice@test.com', 'Admin')")
        cursor.execute("INSERT INTO dbo.Users (full_name, email, role) VALUES ('Bob Builder', 'bob@test.com', 'Developer')")
        cursor.execute("INSERT INTO dbo.Users (full_name, email, role) VALUES ('Charlie Check', 'charlie@test.com', 'Tester')")
        
        # Seed Projects
        cursor.execute("INSERT INTO dbo.Projects (customer_id, name, project_key) VALUES (1, 'Acme Alpha', 'ACM')")
        cursor.execute("INSERT INTO dbo.Projects (customer_id, name, project_key) VALUES (2, 'Global Beta', 'GLB')")
        
        conn.commit()

    cursor.close()


def insert_batch(conn: pyodbc.Connection, batch_size: int = 50) -> None:
    """Insert a batch of Tickets quickly."""
    cursor = conn.cursor()
    query = """
        INSERT INTO dbo.Tickets (project_id, reporter_id, assignee_id, title, description, status, priority) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    statuses = ['Open', 'In Progress', 'Resolved', 'Closed']
    priorities = ['Low', 'Medium', 'High', 'Critical']
    
    data: List[Tuple[Any, ...]] = []
    for _ in range(batch_size):
        project_id = random.randint(1, 2)
        reporter_id = random.randint(1, 3)
        assignee_id = random.randint(1, 3)
        title = f"Task {random_string(8)}"
        description = f"Description for {title}"
        status = random.choice(statuses)
        priority = random.choice(priorities)
        
        data.append((project_id, reporter_id, assignee_id, title, description, status, priority))

    try:
        cursor.executemany(query, data)
        conn.commit()
        logger.info(f"Successfully inserted batch of {batch_size} tickets.")
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            while True:
                # Hammering both databases with slightly smaller batches so we don't overwhelm SSE/UI
                # as we're doing complex operations now
                batch_size = random.randint(50, 500)

                logger.info(
                    f"Hammering BOTH databases with {batch_size} rows each in parallel!"
                )

                # Submit both batches to run concurrently
                f1 = executor.submit(insert_batch, conn1, batch_size)
                f2 = executor.submit(insert_batch, conn2, batch_size)

                # Wait for both batch inserts to complete before moving to the next iteration
                concurrent.futures.wait([f1, f2])
                
                # Sleep briefly to not completely murder the local disk and CPU
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Workload generator stopped by user.")
    finally:
        conn1.close()
        conn2.close()


if __name__ == "__main__":
    run_workload_generator()
