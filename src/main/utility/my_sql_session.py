import mysql.connector
import logging
from mysql.connector import Error

logger = logging.getLogger(__name__)

def get_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='your_database',
            user='your_user',
            password='your_password'
        )
        if connection.is_connected():
            logger.info("Connected to MySQL database")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {str(e)}")
        raise

    return None 