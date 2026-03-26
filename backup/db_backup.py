import os
import json
import sqlite3
from datetime import datetime, timezone, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
# 既定は artifacts/backup/backup.db 配下にまとめる
BACKUP_DB_PATH = os.getenv("BACKUP_DB_PATH", os.path.join("artifacts", "backup", "backup.db"))
DELETION_THRESHOLD_DAYS = int(os.getenv("BACKUP_THRESHOLD_DAYS", 90))


def to_sqlite_value(value):
    """SQLite で扱えない型を文字列化する"""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def fetch_rows_older_than(conn, table_name, threshold_dt):
    """指定テーブルから threshold_dt より古いレコードを取得"""
    cur = conn.cursor()
    # カラム一覧
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    columns = [row[0] for row in cur.fetchall()]

    # created_at を基準に 3か月以上前のみ取得
    if "created_at" not in columns:
        raise RuntimeError(f"{table_name} に created_at カラムがありません")

    cur.execute(
        f"SELECT * FROM {table_name} WHERE created_at < %s",
        (threshold_dt,),
    )
    rows = cur.fetchall()
    cur.close()
    return columns, rows


def ensure_sqlite_table(sqlite_cur, table_name, columns):
    """SQLite 側にテーブル&一意制約を用意（id で重複防止）"""
    col_defs = []
    for col in columns:
        # 型は全部 TEXT 扱い（PostgreSQL 型にあまり依存しない）
        col_defs.append(f'"{col}" TEXT')

    col_def_sql = ", ".join(col_defs)
    sqlite_cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_def_sql})")

    if "id" in columns:
        # 既存 DB に対しても安全に一意制約を追加（既に存在すれば何もしない）
        sqlite_cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(id)"
        )


def backup_old_rows_to_sqlite():
    """3か月以上前の todos/replies を backup.db に追記する"""
    if not POSTGRES_URL:
        raise RuntimeError("POSTGRES_URL が未設定です。.env を確認してください")

    threshold_dt = datetime.now(timezone.utc) - timedelta(days=DELETION_THRESHOLD_DAYS)
    print(f"📌 しきい値: {threshold_dt.isoformat()} より古いレコードをバックアップ対象とします")

    pg_conn = psycopg2.connect(POSTGRES_URL)

    # フォルダ付きパスの場合でも作成できるようにディレクトリを用意
    backup_dir = os.path.dirname(BACKUP_DB_PATH)
    if backup_dir:
        os.makedirs(backup_dir, exist_ok=True)

    sqlite_conn = sqlite3.connect(BACKUP_DB_PATH)
    sqlite_cur = sqlite_conn.cursor()

    try:
        for table_name in ["todos", "replies"]:
            print(f"\n📌 Backing up old rows from {table_name}...")
            columns, rows = fetch_rows_older_than(pg_conn, table_name, threshold_dt)

            if not rows:
                print(f"  ✓ {table_name}: バックアップ対象のレコードはありません")
                continue

            ensure_sqlite_table(sqlite_cur, table_name, columns)

            cols_quoted = ", ".join([f'"{c}"' for c in columns])
            placeholders = ", ".join(["?" for _ in columns])
            insert_sql = f"INSERT OR IGNORE INTO {table_name} ({cols_quoted}) VALUES ({placeholders})"

            inserted = 0
            for row in rows:
                serialized_row = tuple(to_sqlite_value(v) for v in row)
                sqlite_cur.execute(insert_sql, serialized_row)
                inserted += 1

            print(f"  ✓ {table_name}: {inserted} rows (INSERT OR IGNORE)")

        sqlite_conn.commit()
    finally:
        sqlite_cur.close()
        sqlite_conn.close()
        pg_conn.close()


__all__ = ["backup_old_rows_to_sqlite", "BACKUP_DB_PATH", "DELETION_THRESHOLD_DAYS"]
