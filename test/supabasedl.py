import os
import json
import sqlite3
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

db_url = os.getenv("POSTGRES_URL")
BACKUP_DB = "backup.db"


def to_sqlite_value(value):
    """SQLiteで扱えない型を文字列化する。"""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value

def fetch_table(conn, table_name):
    """テーブルから全件取得"""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    columns = [col[0] for col in cur.fetchall()]
    
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    
    return columns, rows

def save_to_sqlite(postgres_conn):
    """PostgreSQL → SQLite"""
    # SQLite接続
    sqlite_conn = sqlite3.connect(BACKUP_DB)
    sqlite_cur = sqlite_conn.cursor()
    
    # バックアップテーブル
    tables = ["todos", "replies"]
    
    for table_name in tables:
        print(f"\n📌 Backing up {table_name}...")
        
        # PostgreSQL から取得
        columns, rows = fetch_table(postgres_conn, table_name)
        
        if not rows:
            print(f"  ✓ {table_name}: 0 records")
            continue
        
        # SQLite にテーブル作成（簡易版 - 全カラムTEXT）
        col_def = ", ".join([f'"{col}" TEXT' for col in columns])
        sqlite_cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        sqlite_cur.execute(f"CREATE TABLE {table_name} ({col_def})")
        
        # INSERT
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT INTO {table_name} VALUES ({str(placeholders)})"
        
        for row in rows:
            serialized_row = tuple(to_sqlite_value(v) for v in row)
            sqlite_cur.execute(insert_sql, serialized_row)
        
        print(f"  ✓ {table_name}: {len(rows)} records inserted")
    
    sqlite_conn.commit()
    sqlite_conn.close()