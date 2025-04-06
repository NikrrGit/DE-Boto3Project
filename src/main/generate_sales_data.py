import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Generate random sales data
def generate_sales_data(num_records=100):
    # Product list
    products = ['quaker oats', 'refined oil', 'sugar', 'flour']
    
    # Generate random dates within the last 3 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    data = {
        'customer_id': np.random.randint(1, 11, num_records),
        'store_id': np.random.choice([121, 122, 123], num_records),
        'product_name': np.random.choice(products, num_records),
        'sales_date': [(start_date + timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d') for _ in range(num_records)],
        'sales_person_id': np.random.randint(1, 10, num_records),
        'price': np.random.uniform(10, 200, num_records).round(2),
        'quantity': np.random.randint(1, 10, num_records)
    }
    
    df = pd.DataFrame(data)
    df['total_cost'] = (df['price'] * df['quantity']).round(2)
    
    return df

# Generate and save the data
if __name__ == "__main__":
    # Generate 100 records
    sales_data = generate_sales_data(100)
    
    # Save to CSV
    sales_data.to_csv('sales_data.csv', index=False)
    print("Sales data generated and saved to sales_data.csv") 