import os
import re
import glob
import json
import sqlite3
from datetime import datetime, timezone, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
# BACKUP_DB_PATH は「テンプレート」として扱い、実体は *_YYYY.db を生成する
# 例: artifacts/backup/jyogi_sns_backup.db -> artifacts/backup/jyogi_sns_backup_2026.db
BACKUP_DB_PATH = os.getenv(
    "BACKUP_DB_PATH",
    os.path.join("artifacts", "backup", "jyogi_sns_backup.db"),
)
DELETION_THRESHOLD_DAYS = int(os.getenv("BACKUP_THRESHOLD_DAYS", 90))


def _backup_template_parts():
    template = BACKUP_DB_PATH
    backup_dir = os.path.dirname(template)
    base = os.path.basename(template)
    stem, ext = os.path.splitext(base)
    if not stem:
        stem = "jyogi_sns_backup"
    if not ext:
        ext = ".db"
    return backup_dir, stem, ext


def get_yearly_backup_db_path(year: int) -> str:
    """年別の SQLite バックアップファイルパスを返す。"""
    backup_dir, stem, ext = _backup_template_parts()
    filename = f"{stem}_{int(year)}{ext}"
    return os.path.abspath(os.path.join(backup_dir, filename))


def list_yearly_backup_db_paths():
    """既存の年別 SQLite バックアップファイルを年昇順で返す。"""
    backup_dir, stem, ext = _backup_template_parts()
    if not backup_dir:
        backup_dir = "."
    pattern = os.path.join(backup_dir, f"{stem}_*{ext}")
    candidates = glob.glob(pattern)

    year_file_pairs = []
    regex = re.compile(rf"^{re.escape(stem)}_(\d{{4}}){re.escape(ext)}$")
    for path in candidates:
        name = os.path.basename(path)
        m = regex.match(name)
        if not m:
            continue
        year_file_pairs.append((int(m.group(1)), os.path.abspath(path)))

    return [p for _, p in sorted(year_file_pairs, key=lambda x: x[0])]


def _extract_year_from_created_at(value) -> int:
    if isinstance(value, datetime):
        return value.year

    text = str(value)
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).year
    except Exception:
        pass

    # 末尾にタイムゾーンが付かない形式などにも緩く対応
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").year
    except Exception:
        raise RuntimeError(f"created_at から年を抽出できません: {value}")


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
    """3か月以上前の todos/replies を年別の SQLite(*_YYYY.db) に追記する"""
    if not POSTGRES_URL:
        raise RuntimeError("POSTGRES_URL が未設定です。.env を確認してください")

    threshold_dt = datetime.now(timezone.utc) - timedelta(days=DELETION_THRESHOLD_DAYS)
    print(f"📌 しきい値: {threshold_dt.isoformat()} より古いレコードをバックアップ対象とします")

    pg_conn = psycopg2.connect(POSTGRES_URL)

    backup_dir, _, _ = _backup_template_parts()
    if backup_dir:
        os.makedirs(backup_dir, exist_ok=True)

    sqlite_resources = {}

    def get_sqlite_cursor_for_year(year: int):
        db_path = get_yearly_backup_db_path(year)
        if db_path not in sqlite_resources:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            sqlite_resources[db_path] = (conn, cur)
        return db_path, sqlite_resources[db_path][0], sqlite_resources[db_path][1]

    try:
        for table_name in ["todos", "replies"]:
            print(f"\n📌 Backing up old rows from {table_name}...")
            columns, rows = fetch_rows_older_than(pg_conn, table_name, threshold_dt)

            if not rows:
                print(f"  ✓ {table_name}: バックアップ対象のレコードはありません")
                continue

            created_at_index = columns.index("created_at")

            rows_by_year = {}
            for row in rows:
                year = _extract_year_from_created_at(row[created_at_index])
                rows_by_year.setdefault(year, []).append(row)

            cols_quoted = ", ".join([f'"{c}"' for c in columns])
            placeholders = ", ".join(["?" for _ in columns])
            insert_sql = f"INSERT OR IGNORE INTO {table_name} ({cols_quoted}) VALUES ({placeholders})"

            for year, year_rows in sorted(rows_by_year.items()):
                db_path, _, sqlite_cur = get_sqlite_cursor_for_year(year)
                ensure_sqlite_table(sqlite_cur, table_name, columns)

                inserted = 0
                for row in year_rows:
                    serialized_row = tuple(to_sqlite_value(v) for v in row)
                    sqlite_cur.execute(insert_sql, serialized_row)
                    inserted += 1

                print(f"  ✓ {table_name}/{year}: {inserted} rows -> {db_path}")

        for conn, _ in sqlite_resources.values():
            conn.commit()
    finally:
        for conn, cur in sqlite_resources.values():
            cur.close()
            conn.close()
        pg_conn.close()

    return list_yearly_backup_db_paths()


__all__ = [
    "backup_old_rows_to_sqlite",
    "list_yearly_backup_db_paths",
    "get_yearly_backup_db_path",
    "BACKUP_DB_PATH",
    "DELETION_THRESHOLD_DAYS",
]
