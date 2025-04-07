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

def verify_processing():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        print("\n=== Verifying Customer Purchases ===")
        cursor.execute("SELECT * FROM customer_purchases LIMIT 5")
        customer_results = cursor.fetchall()
        print("\nSample Customer Purchases:")
        for row in customer_results:
            print(f"Customer ID: {row[0]}, Total Purchases: {row[1]}, "
                  f"Avg Purchase: {row[2]}, Purchase Count: {row[3]}")
            
        print("\n=== Verifying Sales Team Incentives ===")
        cursor.execute("SELECT * FROM sales_team_incentives LIMIT 5")
        sales_results = cursor.fetchall()
        print("\nSample Sales Team Incentives:")
        for row in sales_results:
            print(f"Salesperson ID: {row[0]}, Total Sales: {row[1]}, "
                  f"Commission: {row[2]}")
            
        # Get counts
        cursor.execute("SELECT COUNT(*) FROM customer_purchases")
        customer_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM sales_team_incentives")
        sales_count = cursor.fetchone()[0]
        
        print(f"\nTotal Customer Records: {customer_count}")
        print(f"Total Sales Team Records: {sales_count}")
        
        # Verify data integrity
        if customer_count > 0 and sales_count > 0:
            print("\n✅ Spark processing verification successful!")
            print("Data has been properly processed and stored in MySQL")
        else:
            print("\n❌ Verification failed: No data found in tables")
            
    except Exception as e:
        print(f"\n❌ Error during verification: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    verify_processing() 