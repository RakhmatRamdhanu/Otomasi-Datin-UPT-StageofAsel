import socketio
import sqlite3
import json
import time
from datetime import datetime

# --- KONFIGURASI ---
SERVER_URL = 'http://172.19.2.185:1880'  # Port 1880 Node-RED
DB_NAME = 'gempa_data.db'

# Auto reconnect 
sio = socketio.Client(reconnection=True, reconnection_attempts=0, reconnection_delay=1, logger=True)

def init_db():
    """Bikin database kalau belum ada"""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS gempa (
                        id TEXT PRIMARY KEY,
                        waktu_server DATETIME,
                        info_gempa TEXT,
                        lat REAL,
                        lon REAL,
                        mag REAL,
                        depth REAL,
                        place TEXT,
                        raw_json TEXT
                    )''')
        conn.commit()
        conn.close()
        print(f"[*] Database {DB_NAME} siap.")
    except Exception as e:
        print(f"[!] Gagal Init DB: {e}")

def simpan_ke_db(data):
    """Simpan data json ke SQLite"""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Mapping data aman (pakai .get biar gak error)
        p_id = data.get('id', f"unknown_{int(time.time())}")
        
        c.execute('''INSERT OR IGNORE INTO gempa VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (p_id, 
                   data.get('created_at'), 
                   data.get('info_gempa'),
                   data.get('latitude'), 
                   data.get('longitude'), 
                   data.get('mag'), 
                   data.get('depth'), 
                   data.get('place'), 
                   json.dumps(data)))
        
        if conn.total_changes > 0:
            print(f"\n[+] DATA BARU: {data.get('place')} | M{data.get('mag')}")
            
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[!] Error Simpan DB: {e}")

# --- EVENT HANDLER ---
@sio.event
def connect():
    print(f"\n[{datetime.now()}] >>> TERHUBUNG KE {SERVER_URL}")

@sio.event
def disconnect():
    print(f"\n[{datetime.now()}] !!! TERPUTUS")

@sio.on('update-value')
def on_message(data):
    payload = data.get('payload', [])
    if payload:
        for gempa in payload:
            simpan_ke_db(gempa)

if __name__ == '__main__':
    print("--- STARTING COLLECTOR ---")
    init_db()
    while True:
        try:
            sio.connect(SERVER_URL)
            sio.wait()
        except KeyboardInterrupt:
            break
        except Exception:
            print("Reconnect dalam 5 detik...")
            time.sleep(5)