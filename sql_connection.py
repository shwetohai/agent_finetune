
import psycopg2
import pandas as pd



class PostgresDatabase:
    def __init__(self, host, database, user, password, port=5432):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            print("Connected to the database.")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from the database.")

    def fetch_data(self, query):
        """
        Executes a SQL query and returns the result as a DataFrame.
        """
        try:
            if self.connection is None:
                raise Exception("Database is not connected.")
                
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]  # Get column names
            data = cursor.fetchall()  # Fetch all rows
            df = pd.DataFrame(data, columns=columns)  # Create DataFrame
            cursor.close()
            return df

        except Exception as e:
            print(f"Error fetching data: {e}")
            return None
