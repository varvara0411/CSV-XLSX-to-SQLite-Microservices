import sqlite3
import pandas as pd
import os
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path='instance/database.db'):
        self.db_path = db_path
        self._ensure_db_directory()
    
    def _ensure_db_directory(self):
        """Creates database directory if it doesn't exist"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

    def _detect_type(self, value):
        """Type of one value"""
        if value is None or value == "":
            return "TEXT"
        
        try:
            int(value)
            return "INTEGER"
        except:
            pass
        try:
            float(value)
            return "REAL"
        except:
            pass
        try:
            datetime.strptime(str(value), "%Y-%m-%d")
            return "TIMESTAMP"
        except:
            pass
        try:
            datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
            return "TIMESTAMP"
        except:
            pass
        return "TEXT"

    def _infer_column_types_from_df(self, df):
        """Types of DataFrame columns"""
        column_types = []
        
        for col_name in df.columns:
            column_data = df[col_name]
            detected = "INTEGER"
            
            for value in column_data:
                if pd.isna(value):
                    continue
                
                t = self._detect_type(value)
                
                if t == "TEXT":
                    detected = "TEXT"
                    break
                elif t == "TIMESTAMP" and detected not in ["TEXT"]:
                    detected = "TIMESTAMP"
                elif t == "REAL" and detected not in ["TEXT", "TIMESTAMP"]:
                    detected = "REAL"
            
            column_types.append(detected)
        
        return column_types
    
    def create_table_from_dataframe(self, df, table_name):
        """
        Creates database table from DataFrame
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
    
        try:
            # Types of columns
            column_types = self._infer_column_types_from_df(df)
        
            columns = []
            for i, col_name in enumerate(df.columns):
                sql_type = column_types[i]
                # Escape column names (replace spaces and special characters)
                safe_col_name = col_name.replace(' ', '_').replace('-', '_')
                columns.append(f'"{safe_col_name}" {sql_type}')
        
            # Create table
            create_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns)})'
            cursor.execute(create_query)
        
            # Clear table if it already has data
            cursor.execute(f'DELETE FROM "{table_name}"')
        
            # Insert data row by row
            for _, row in df.iterrows():
                placeholders = ', '.join(['?' for _ in range(len(row))])
                insert_query = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
            
                # Convert values to appropriate format
                values = []
                for val in row:
                    if pd.isna(val) or val == 'nan' or val == 'NaT':
                        values.append(None)
                    else:
                        values.append(val) 
                cursor.execute(insert_query, values)
        
            conn.commit()
            # Return 3 values: success, message, column_types
            return True, f"Table '{table_name}' successfully created with {len(df)} records", column_types
        
        except Exception as e:
            conn.rollback()
            return False, f"Error creating table: {str(e)}", None
    
        finally:
            conn.close()
    
    def get_table_names(self):
        """Returns list of all tables in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        conn.close()
        return tables
    
    def get_table_data(self, table_name):
        """Returns table data for viewing"""
        conn = sqlite3.connect(self.db_path)
        try:
            # Get data
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
            return df
        except Exception as e:
            return None
        finally:
            conn.close()