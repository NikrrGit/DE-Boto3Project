import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MySQL connection parameters
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

def setup_database():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Create customer_purchases table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_purchases (
            customer_id VARCHAR(50) PRIMARY KEY,
            total_purchases DECIMAL(10,2),
            avg_purchase DECIMAL(10,2),
            purchase_count INT
        )
        """)
        
        # Create sales_team_incentives table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_team_incentives (
            salesperson_id VARCHAR(50) PRIMARY KEY,
            total_sales DECIMAL(10,2),
            commission DECIMAL(10,2)
        )
        """)
        
        conn.commit()
        print("✅ Database tables created successfully!")
        
    except Exception as e:
        print(f"❌ Error setting up database: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_database() 