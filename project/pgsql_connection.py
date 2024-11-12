import psycopg2
import logging
import pandas as pd
from sqlalchemy import create_engine


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class PostgreSQLDatabase:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        """Initialize the database connection parameters."""
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def execute_query(self, query):
        """Execute a query."""
        try:
            self.cursor.execute(query)
            self.conn.commit()
            logging.info("Query executed successfully.")
        except Exception as e:
            logging.error(f"An error occurred while executing the query: {e}")

    def get_results(self):
        return self.cursor.fetchall()

    def append_dataframe(self, df, table_name, truncate=False):
        """Append a Pandas DataFrame to an existing table."""
        try:
            # Create SQLAlchemy engine
            engine = create_engine(
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            )
            # Append the DataFrame to the specified table
            df.to_sql(
                table_name,
                engine,
                if_exists="append" if not truncate else "replace",
                index=False,
            )
            logging.info(f"Mode {'append' if not truncate else 'replace'}")
            logging.info(
                f"DataFrame appended to the table '{table_name}' successfully."
            )
        except Exception as e:
            logging.error(f"An error occurred while appending the DataFrame: {e}")

    def __enter__(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self.cursor = self.conn.cursor()
            logging.info("Connection to the database established.")
        except Exception as e:
            logging.errывor(f"An error occurred while connecting to the database: {e}")
            raise  # Reraise the exception after logging it

        return self

    def __exit__(self, type, value, traceback):
        """Close the cursor and connection."""
        if self.cursor:
            self.cursor.close()
            logging.info("Cursor closed.")
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")


# Example usage
if __name__ == "__main__":
    data = {"name": ["Alice", "Bob"], "age": [50, 40]}
    df = pd.DataFrame(data)
    print(df)
    with PostgreSQLDatabase("postgres", "postgres", "password") as db:
        db.execute_query("SELECT * FROM public.employees;")
        print(db.get_results())
        db.append_dataframe(df, "employees")
        db.execute_query("SELECT * FROM public.employees;")
        print(db.get_results())
