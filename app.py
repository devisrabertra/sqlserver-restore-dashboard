from flask import Flask, render_template, request, jsonify
import pyodbc
import time
import subprocess
import os
import pandas as pd

# ==================================================
# ADVANCED IR IMPORT
# ==================================================
from advanced_ir import ir_system

app = Flask(__name__)

# ---------------------------------------------
# Koneksi ke SQL Server
# ---------------------------------------------
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

# ---------------------------------------------
# Ambil nama server
# ---------------------------------------------
def get_server_name():
    conn = GetConnection("ogdbatest01")
    cursor = conn.cursor()
    cursor.execute("SELECT @@SERVERNAME;")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0]

# ---------------------------------------------
# Ambil Daftar Database
# ---------------------------------------------
def get_database_list():
    query = """
    SELECT name 
    FROM sys.databases 
    WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
    ORDER BY name;
    """
    conn = GetConnection("ogdbatest01")
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in rows]

# ---------------------------------------------
# Fungsi untuk menjalankan RESTORE HEADERONLY
# ---------------------------------------------
def run_restore_headeronly(backup_file_path):
    conn = GetConnection("ogdbatest01")
    cursor = conn.cursor()
    query = f"""
    RESTORE HEADERONLY FROM DISK = '{backup_file_path}';
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    conn.close()
    
    # Filter hanya 3 kolom
    filtered = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        filtered.append({
            "ServerName": row_dict.get("ServerName"),
            "DatabaseName": row_dict.get("DatabaseName"),
            "DatabaseCreationDate": row_dict.get("DatabaseCreationDate")
        })
    return filtered

# ---------------------------------------------
# Fungsi untuk menjalankan script Python restore
# ---------------------------------------------
def run_restore_script(script_name):
    """
    Menjalankan script Python untuk restore database
    """
    try:
        # Path ke folder restore_db
        restore_folder = os.path.join(os.path.dirname(__file__), 'restore_db')
        script_path = os.path.join(restore_folder, script_name)
        
        # Pastikan file exists
        if not os.path.exists(script_path):
            return {
                "success": False,
                "message": f"File script {script_name} tidak ditemukan di {restore_folder}"
            }
        
        print(f"🚀 Menjalankan script: {script_path}")
        print(f"⏰ Waktu mulai: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Jalankan script Python
        result = subprocess.run(
            ['python', script_path],
            capture_output=True,
            text=True,
            timeout=1800,  # Timeout 30 menit
            cwd=restore_folder  # Set working directory ke folder script
        )
        
        print(f"✅ Waktu selesai: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Return code: {result.returncode}")
        print(f"📝 Stdout: {result.stdout}")
        if result.stderr:
            print(f"❌ Stderr: {result.stderr}")
        
        if result.returncode == 0:
            return {
                "success": True,
                "message": f"Restore berhasil via {script_name}",
                "output": result.stdout,
                "error_output": result.stderr
            }
        else:
            return {
                "success": False,
                "message": f"Error saat menjalankan {script_name}",
                "output": result.stdout,
                "error_output": result.stderr
            }
            
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "message": f"Proses restore timeout (30 menit). Script {script_name} masih berjalan.",
            "output": "",
            "error_output": "Timeout: Process took too long"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error menjalankan script {script_name}: {str(e)}",
            "output": "",
            "error_output": str(e)
        }

# ---------------------------------------------
# Fungsi untuk mengambil daftar database dari history backup
# ---------------------------------------------
def get_backup_databases():
    """
    Mengambil daftar unik database dari history backup
    """
    try:
        query = """
        SELECT DISTINCT database_name
        FROM msdb.dbo.backupset 
        WHERE (CONVERT(datetime, backup_start_date, 102) >= GETDATE() - 33)
        ORDER BY database_name
        """
        
        conn = GetConnection("ogdbatest01")
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        databases = [row[0] for row in rows]
        return databases
        
    except Exception as e:
        print(f"Error getting backup databases: {str(e)}")
        return []

# ---------------------------------------------
# Fungsi untuk mengambil history backup dengan filter dan limit
# ---------------------------------------------
def get_backup_history(database_name=None, backup_type=None, limit=1000):
    """
    Mengambil history backup dari database dengan filter dan limit
    """
    try:
        # Build query secara dinamis
        base_query = """
        SELECT TOP ({}) 
            CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
            bs.database_name, 
            bs.backup_start_date, 
            bs.backup_finish_date,
            CASE bs.type 
                WHEN 'D' THEN 'Database' 
                WHEN 'I' THEN 'Differential' 
                WHEN 'L' THEN 'Log' 
            END AS backup_type, 
            bs.backup_size, 
            bmf.physical_device_name, 
            bs.name AS backupset_name
        FROM msdb.dbo.backupmediafamily bmf
        INNER JOIN msdb.dbo.backupset bs 
            ON bmf.media_set_id = bs.media_set_id 
        WHERE (CONVERT(datetime, bs.backup_start_date, 102) >= GETDATE() - 33)
        """
        
        # Parameter list
        params = [limit]
        
        # Tambahkan filter database jika ada
        if database_name and database_name != "all":
            base_query += " AND bs.database_name = ?"
            params.append(database_name)
        
        # Tambahkan filter backup type jika ada
        if backup_type and backup_type != "all":
            type_mapping = {
                "database": "D",
                "differential": "I", 
                "log": "L"
            }
            if backup_type in type_mapping:
                base_query += " AND bs.type = ?"
                params.append(type_mapping[backup_type])
        
        base_query += " ORDER BY bs.backup_finish_date DESC"
        
        # Format query dengan limit
        final_query = base_query.format(limit)
        
        conn = GetConnection("ogdbatest01")
        
        # Execute query dengan parameter
        if len(params) > 1:  # Jika ada parameter selain limit
            df = pd.read_sql(final_query, conn, params=params[1:])  # Skip limit parameter
        else:
            df = pd.read_sql(final_query, conn)
            
        conn.close()
        
        # Konversi DataFrame ke list of dictionaries untuk template
        backup_history = df.to_dict('records')
        
        return {
            "success": True,
            "data": backup_history,
            "count": len(backup_history),
            "filters": {
                "database_name": database_name,
                "backup_type": backup_type
            },
            "limit_reached": len(backup_history) == limit
        }
        
    except Exception as e:
        print(f"❌ Error in get_backup_history: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "data": [],
            "count": 0,
            "filters": {
                "database_name": database_name,
                "backup_type": backup_type
            },
            "limit_reached": False
        }

# ---------------------------------------------
# Fungsi untuk mengambil count total backup records (untuk info)
# ---------------------------------------------
def get_backup_count(database_name=None, backup_type=None):
    """
    Mengambil jumlah total records untuk informasi
    """
    try:
        query = """
        SELECT COUNT(*) as total_count
        FROM msdb.dbo.backupmediafamily bmf
        INNER JOIN msdb.dbo.backupset bs 
            ON bmf.media_set_id = bs.media_set_id 
        WHERE (CONVERT(datetime, bs.backup_start_date, 102) >= GETDATE() - 33)
        """
        
        params = []
        if database_name and database_name != "all":
            query += " AND bs.database_name = ?"
            params.append(database_name)
        
        if backup_type and backup_type != "all":
            type_mapping = {
                "database": "D",
                "differential": "I", 
                "log": "L"
            }
            if backup_type in type_mapping:
                query += " AND bs.type = ?"
                params.append(type_mapping[backup_type])
        
        conn = GetConnection("ogdbatest01")
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        row = cursor.fetchone()
        conn.close()
        
        return row[0] if row else 0
        
    except Exception as e:
        print(f"Error getting backup count: {str(e)}")
        return 0

# ---------------------------------------------
# Home
# ---------------------------------------------
@app.route("/")
def home():
    server_name = get_server_name()
    
    data = {
        "title": "Dashboard Monitoring",
        "server_status": [
            {"server": server_name, "status": "Database"}
        ],
        "databases": [],
        "show_validation_box": False,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": False,
        "show_history": False,
        "backup_history": None,
        "backup_databases": [],
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False
    }
    return render_template("index.html", data=data)

# ---------------------------------------------
# Ambil Daftar Database (Route untuk tombol Database)
# ---------------------------------------------
@app.route("/get_databases", methods=["POST"])
def show_databases():
    database_list = get_database_list()
    server_name = get_server_name()
    
    data = {
        "title": "Dashboard Monitoring",
        "server_status": [
            {"server": server_name, "status": "Database"}
        ],
        "databases": database_list,
        "show_validation_box": False,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": True,
        "show_history": False,
        "backup_history": None,
        "backup_databases": [],
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False
    }
    return render_template("index.html", data=data)

# ---------------------------------------------
# Tombol History Backup → tampilkan form filter
# ---------------------------------------------
@app.route("/history_backup", methods=["POST"])
def history_backup():
    server_name = get_server_name()
    
    # Ambil daftar database dari backup history
    backup_databases = get_backup_databases()
    
    data = {
        "title": "Dashboard Monitoring",
        "server_status": [{"server": server_name, "status": "Database"}],
        "databases": [],
        "show_validation_box": False,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": False,
        "show_history": True,
        "show_history_results": False,
        "backup_history": None,
        "backup_databases": backup_databases,
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False
    }
    return render_template("index.html", data=data)

# ---------------------------------------------
# PROSES HISTORY BACKUP - DIMODIFIKASI UNTUK ADVANCED IR
# ---------------------------------------------
@app.route("/proses_history_backup", methods=["POST"])
def proses_history_backup():
    database_name = request.form.get("databaseSelect")
    backup_type = request.form.get("backupTypeSelect")
    search_query = request.form.get("search_query")  # ADVANCED IR: Natural language query
    advanced_search = request.form.get("advanced_search")  # ADVANCED IR: Flag
    
    server_name = get_server_name()
    
    # Ambil daftar database dari backup history
    backup_databases = get_backup_databases()
    
    # Ambil data history backup dengan filter dan limit
    history_result = get_backup_history(database_name, backup_type, limit=1000)
    
    # ==================================================
    # ADVANCED IR: PROSES NATURAL LANGUAGE QUERY
    # ==================================================
    ranked_backups = None
    show_advanced_results = False
    
    if advanced_search and search_query and search_query.strip():
        print(f"🎯 ADVANCED IR: Memproses natural language query: '{search_query}'")
        
        if history_result["success"] and history_result["data"]:
            # Gunakan Advanced IR untuk ranking
            ranked_backups = ir_system.rank_backups(history_result["data"], search_query)
            show_advanced_results = True
            
            print(f"✅ ADVANCED IR: Menemukan {len(ranked_backups)} backup relevan")
        else:
            print("⚠️ ADVANCED IR: Tidak ada data backup untuk diproses")
    
    # Ambil total count untuk informasi
    total_count = get_backup_count(database_name, backup_type)
    
    data = {
        "title": "Dashboard Monitoring",
        "server_status": [{"server": server_name, "status": "Database"}],
        "databases": [],
        "show_validation_box": False,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": False,
        "show_history": True,
        "show_history_results": True,
        "backup_history": history_result,
        "backup_databases": backup_databases,
        "selected_database": database_name,
        "selected_backup_type": backup_type,
        "total_count": total_count,
        "restore_result": None,
        # ==================================================
        # ADVANCED IR: DATA BARU
        # ==================================================
        "ranked_backups": ranked_backups,
        "show_advanced_results": show_advanced_results,
        "search_query": search_query,
        "advanced_search_used": advanced_search
    }
    return render_template("index.html", data=data)

# ---------------------------------------------
# ADVANCED IR: ENDPOINT BARU UNTUK QUICK SEARCH
# ---------------------------------------------
@app.route("/quick_search", methods=["POST"])
def quick_search():
    """
    ADVANCED IR: Endpoint khusus untuk quick natural language search
    """
    search_query = request.form.get("quick_search_query")
    server_name = get_server_name()
    
    if not search_query or not search_query.strip():
        return render_template("index.html", data={
            "title": "Dashboard Monitoring",
            "server_status": [{"server": server_name, "status": "Database"}],
            "show_history": True,
            "show_advanced_results": False,
            "error_message": "Masukkan query pencarian"
        })
    
    print(f"🎯 ADVANCED IR: Quick search untuk: '{search_query}'")
    
    # Ambil semua backup data
    backup_result = get_backup_history(None, None, limit=500)
    
    if backup_result["success"] and backup_result["data"]:
        # Gunakan Advanced IR
        ranked_backups = ir_system.rank_backups(backup_result["data"], search_query)
        
        data = {
            "title": "Dashboard Monitoring",
            "server_status": [{"server": server_name, "status": "Database"}],
            "show_history": True,
            "show_history_results": True,
            "show_advanced_results": True,
            "ranked_backups": ranked_backups,
            "search_query": search_query,
            "advanced_search_used": True,
            "quick_search_mode": True
        }
        
        return render_template("index.html", data=data)
    else:
        return render_template("index.html", data={
            "title": "Dashboard Monitoring",
            "server_status": [{"server": server_name, "status": "Database"}],
            "show_history": True,
            "show_advanced_results": False,
            "error_message": "Tidak ada data backup yang ditemukan"
        })

# ---------------------------------------------
# Tombol Validasi Bak → tampilkan combo box
# ---------------------------------------------
@app.route("/validasi_bak", methods=["POST"])
def validasi_bak():
    server_name = get_server_name()
    data = {
        "title": "Dashboard Monitoring",
        "server_status": [{"server": server_name, "status": "Database"}],
        "databases": [],
        "show_validation_box": True,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": False,
        "show_history": False,
        "show_history_results": False,
        "backup_history": None,
        "backup_databases": [],
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False
    }
    return render_template("index.html", data=data)

# ---------------------------------------------
# Tombol Restore Database → tampilkan combo box restore
# ---------------------------------------------
@app.route("/restore_database", methods=["POST"])
def restore_database():
    server_name = get_server_name()
    data = {
        "title": "Dashboard Monitoring",
        "server_status": [{"server": server_name, "status": "Database"}],
        "databases": [],
        "show_validation_box": False,
        "show_restore_box": True,
        "restore_header": [],
        "show_databases": False,
        "show_history": False,
        "show_history_results": False,
        "backup_history": None,
        "backup_databases": [],
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False
    }
    return render_template("index.html", data=data)

# ---------------------------------------------
# PROSES VALIDASI (TOMBOL 1-5)
# ---------------------------------------------
@app.route("/proses_validasi", methods=["POST"])
def proses_validasi():
    pilihan = request.form.get("validationSelect")
    server_name = get_server_name()
    
    backup_files = {
        "1": "D:\\Dashboard_project\\DATA BACKUP\\DB_DistributionInventory\\Full_DB_DistributionInventory.Bak",
        "2": "D:\\Dashboard_project\\DATA BACKUP\\DB_LogisticsInventory\\Full_DB_LogisticsInventory.Bak",
        "3": "D:\\Dashboard_project\\DATA BACKUP\\DB_StockManagement\\Full_DB_StockManagement.Bak",
        "4": "D:\\Dashboard_project\\DATA BACKUP\\DB_Warehouse\\Full_DB_Warehouse.Bak",
        "5": "D:\\Dashboard_project\\DATA BACKUP\\DB_InventoriBarang\\Full_DB_InventoriBarang.Bak"
    }
    
    if pilihan in backup_files:
        backup_file_path = backup_files[pilihan]
        try:
            filtered_results = run_restore_headeronly(backup_file_path)
            data = {
                "title": "Dashboard Monitoring",
                "server_status": [{"server": server_name, "status": "Database"}],
                "databases": [],
                "show_validation_box": True,
                "show_restore_box": False,
                "restore_header": filtered_results,
                "show_databases": False,
                "show_history": False,
                "show_history_results": False,
                "backup_history": None,
                "backup_databases": [],
                "backup_file_used": backup_file_path,
                "restore_result": None,
                "ranked_backups": None,
                "show_advanced_results": False,
                "search_query": None,
                "advanced_search_used": False
            }
        except Exception as e:
            data = {
                "title": "Dashboard Monitoring",
                "server_status": [{"server": server_name, "status": "Database"}],
                "databases": [],
                "show_validation_box": True,
                "show_restore_box": False,
                "restore_header": [],
                "show_databases": False,
                "show_history": False,
                "show_history_results": False,
                "backup_history": None,
                "backup_databases": [],
                "error_message": f"Error: {str(e)}",
                "restore_result": None,
                "ranked_backups": None,
                "show_advanced_results": False,
                "search_query": None,
                "advanced_search_used": False
            }
    else:
        data = {
            "title": "Dashboard Monitoring",
            "server_status": [{"server": server_name, "status": "Database"}],
            "databases": [],
            "show_validation_box": True,
            "show_restore_box": False,
            "restore_header": [],
            "show_databases": False,
            "show_history": False,
            "show_history_results": False,
            "backup_history": None,
            "backup_databases": [],
            "restore_result": None,
            "ranked_backups": None,
            "show_advanced_results": False,
            "search_query": None,
            "advanced_search_used": False
        }
    
    return render_template("index.html", data=data)

# ---------------------------------------------
# PROSES RESTORE (TOMBOL 1-5) - JALANKAN SCRIPT PYTHON
# ---------------------------------------------
@app.route("/proses_restore", methods=["POST"])
def proses_restore():
    pilihan = request.form.get("restoreSelect")
    server_name = get_server_name()
    
    # Mapping pilihan ke script Python
    restore_mapping = {
        "1": {
            "name": "DB_DistributionInventory",
            "script": "restore_DB_DistributionInventory.py",
            "database_name": "DB_DistributionInventory"
        },
        "2": {
            "name": "DB_LogisticsInventory", 
            "script": "restore_DB_LogisticsInventory.py",
            "database_name": "DB_LogisticsInventory"
        },
        "3": {
            "name": "DB_StockManagement",
            "script": "restore_DB_StockManagement.py", 
            "database_name": "DB_StockManagement"
        },
        "4": {
            "name": "DB_Warehouse",
            "script": "restore_DB_Warehouse.py",
            "database_name": "DB_Warehouse"
        },
        "5": {
            "name": "DB_InventoriBarang",
            "script": "restore_DB_InventoriBarang.py",
            "database_name": "DB_InventoriBarang"
        }
    }
    
    if pilihan in restore_mapping:
        restore_info = restore_mapping[pilihan]
        print(f"🎯 Memulai proses restore untuk: {restore_info['name']}")
        print(f"📜 Menggunakan script: {restore_info['script']}")
        
        # Jalankan script Python untuk restore
        restore_result = run_restore_script(restore_info["script"])
        
        data = {
            "title": "Dashboard Monitoring",
            "server_status": [{"server": server_name, "status": "Database"}],
            "databases": [],
            "show_validation_box": False,
            "show_restore_box": True,
            "restore_header": [],
            "show_databases": False,
            "show_history": False,
            "show_history_results": False,
            "backup_history": None,
            "backup_databases": [],
            "restore_result": {
                "success": restore_result["success"],
                "message": restore_result["message"],
                "database_name": restore_info["name"],
                "script_used": restore_info["script"],
                "output": restore_result.get("output", ""),
                "error_output": restore_result.get("error_output", "")
            },
            "ranked_backups": None,
            "show_advanced_results": False,
            "search_query": None,
            "advanced_search_used": False
        }
    else:
        data = {
            "title": "Dashboard Monitoring",
            "server_status": [{"server": server_name, "status": "Database"}],
            "databases": [],
            "show_validation_box": False,
            "show_restore_box": True,
            "restore_header": [],
            "show_databases": False,
            "show_history": False,
            "show_history_results": False,
            "backup_history": None,
            "backup_databases": [],
            "restore_result": None,
            "ranked_backups": None,
            "show_advanced_results": False,
            "search_query": None,
            "advanced_search_used": False
        }
    
    return render_template("index.html", data=data)

# ---------------------------------------------
# Error Handlers
# ---------------------------------------------
@app.errorhandler(404)
def page_not_found(e):
    return render_template('index.html', data={
        "title": "Dashboard Monitoring - Page Not Found",
        "server_status": [{"server": get_server_name(), "status": "Database"}],
        "databases": [],
        "show_validation_box": False,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": False,
        "show_history": False,
        "show_history_results": False,
        "backup_history": None,
        "backup_databases": [],
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False,
        "error_message": "Halaman tidak ditemukan."
    }), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('index.html', data={
        "title": "Dashboard Monitoring - Server Error",
        "server_status": [{"server": get_server_name(), "status": "Database"}],
        "databases": [],
        "show_validation_box": False,
        "show_restore_box": False,
        "restore_header": [],
        "show_databases": False,
        "show_history": False,
        "show_history_results": False,
        "backup_history": None,
        "backup_databases": [],
        "restore_result": None,
        "ranked_backups": None,
        "show_advanced_results": False,
        "search_query": None,
        "advanced_search_used": False,
        "error_message": "Terjadi kesalahan internal server."
    }), 500

# ---------------------------------------------
# RUN
# ---------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Dashboard Monitoring Server Starting...")
    print("🖥️ Server: OGDBATEST01")
    print("🗄️ Database: Dashboard_project")
    print(f"⏰ Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("🤖 Advanced IR System: ACTIVE")
    print("=" * 50)
    
    # Cek apakah folder restore_db exists
    restore_folder = os.path.join(os.path.dirname(__file__), 'restore_db')
    if os.path.exists(restore_folder):
        print(f"✅ Folder restore_db ditemukan: {restore_folder}")
        # List semua file Python di folder restore_db
        python_files = [f for f in os.listdir(restore_folder) if f.endswith('.py')]
        print(f"✅ Script restore yang tersedia: {python_files}")
    else:
        print(f"❌ Folder restore_db tidak ditemukan: {restore_folder}")
    
    app.run(debug=True, host='0.0.0.0', port=5000)