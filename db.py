import os
import logging
from sqlalchemy import create_engine
import config

# Database connection factory

DB_type = 'PR'
def get_engine():
    server = os.getenv('DB_SERVER_' + DB_type)
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME_' + DB_type + 'user')
    password = os.getenv('DB_PASSWORD_' + DB_type + 'user')
    
    if not all([server, database, username, password]):
        logging.error("Database credentials are not fully set in environment variables.")
        raise ValueError("Database credentials are not fully set in environment variables.")

    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(connection_string)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"