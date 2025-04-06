import pandas as pd
import logging
from typing import Optional
import mysql.connector
from pyspark.sql import DataFrame
import pyarrow as pa

logger = logging.getLogger(__name__)

class DatabaseWriter:
    def __init__(self, connection):
        self.connection = connection
        
    def write_to_mysql(self, df: DataFrame, table_name: str):
        try:
            df.write.format('jdbc').options(
                url='jdbc:mysql://your_host/your_database',
                driver='com.mysql.cj.jdbc.Driver',
                dbtable=table_name,
                user='your_user',
                password='your_password'
            ).mode('append').save()
            logger.info(f"Data written to MySQL table {table_name} successfully")
        except Exception as e:
            logger.error(f"Error writing to MySQL table {table_name}: {str(e)}")
            raise
    
    def _process_batch(self, batch_df: pd.DataFrame, table_name: str) -> None:
        if batch_df.empty:
            return
        self._create_table_if_not_exists(table_name, batch_df)
        columns = batch_df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        """
        cursor = self.connection.cursor()
        values = [tuple(row) for row in batch_df.values]
        cursor.executemany(insert_query, values)
        self.connection.commit()
        cursor.close()
    
    def _create_table_if_not_exists(self, table_name: str, pandas_df: pd.DataFrame) -> None:
        try:
            type_mapping = {
                'int64': 'BIGINT',
                'float64': 'DOUBLE',
                'object': 'VARCHAR(255)',
                'datetime64[ns]': 'DATETIME',
                'bool': 'BOOLEAN',
                'category': 'VARCHAR(255)'
            }
            columns = []
            for col, dtype in pandas_df.dtypes.items():
                mysql_type = type_mapping.get(str(dtype), 'VARCHAR(255)')
                columns.append(f"`{col}` {mysql_type}")
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            self.connection.commit()
            cursor.close()
            logger.info(f"Created or verified table {table_name}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise 