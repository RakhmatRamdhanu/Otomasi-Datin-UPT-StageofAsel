import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = 'gempa_database.db'

def ambil_data(start_date, end_date):
    """
    Format tanggal input: 'YYYY-MM-DD' (Contoh: 2026-02-09)
    """
    conn = sqlite3.connect(DB_NAME)
    
    # Query SQL 
    query = f"""
    SELECT waktu_server, info_gempa, mag, place, lat, lon, depth 
    FROM gempa 
    WHERE date(waktu_server) BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY waktu_server DESC
    """
    
    # DataFrame Pandas
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def main():
    print("--- GENERATOR LAPORAN GEMPA ---")
    print("Format Tanggal: YYYY-MM-DD (Misal: 2026-02-01)")
    
    tgl_mulai = input("Masukkan Tanggal Awal : ")
    tgl_akhir = input("Masukkan Tanggal Akhir: ")
    
    print(f"\nSedang mencari data dari {tgl_mulai} s.d {tgl_akhir}...")
    
    df_gempa = ambil_data(tgl_mulai, tgl_akhir)
    
    if not df_gempa.empty:
        print(f"Ditemukan {len(df_gempa)} kejadian gempa.")
        
        # OUTPUT 1: Preview di Layar
        print(df_gempa[['waktu_server', 'place', 'mag']].head())
        
        # OUTPUT 2: Jadiin Excel
        nama_file = f"Laporan_Gempa_{tgl_mulai}_sd_{tgl_akhir}.xlsx"
        df_gempa.to_excel(nama_file, index=False)
        print(f"\n[SUKSES] File tersimpan: {nama_file}")
        
    else:
        print("\n[ZONK] Tidak ada data gempa pada periode tersebut.")

if __name__ == "__main__":
    main()