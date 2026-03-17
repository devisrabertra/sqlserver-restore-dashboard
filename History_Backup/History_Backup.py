import pyodbc
import pandas as pd
import sys
import os

# Add parent directory to path to import GetConnection
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# fungsi GetConnection yang sudah Anda punya
def GetConnection(connection):
    if connection.lower() == "ogdbatest01":
        server = "OGDBATEST01"
        database = "Dashboard_project"
        user = "tes"
        password = "mydbaccess"
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password}",
            autocommit=True,
            timeout=300
        )
        return conn
    return None

def get_backup_history_test():
    """
    Function untuk testing query backup history
    """
    try:
        # Query yang lebih sederhana untuk testing
        query = """
        SELECT TOP 100
            bs.database_name, 
            bs.backup_start_date, 
            bs.backup_finish_date,
            CASE bs.type 
                WHEN 'D' THEN 'Database' 
                WHEN 'I' THEN 'Differential' 
                WHEN 'L' THEN 'Log' 
            END AS backup_type, 
            bs.backup_size, 
            bmf.physical_device_name
        FROM msdb.dbo.backupmediafamily bmf
        INNER JOIN msdb.dbo.backupset bs 
            ON bmf.media_set_id = bs.media_set_id 
        WHERE (CONVERT(datetime, bs.backup_start_date, 102) >= GETDATE() - 33)
        ORDER BY bs.backup_finish_date DESC
        """
        
        conn = GetConnection("ogdbatest01")
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"❌ Error in test: {str(e)}")
        return None

if __name__ == "__main__":
    print("=" * 50)
    print("🧪 Testing Backup History Query")
    print("=" * 50)
    
    df = get_backup_history_test()
    
    if df is not None and not df.empty:
        print(f"✅ Success! Found {len(df)} records")
        print("\nSample data:")
        print(df.head())
    else:
        print("❌ No data found or error occurred")