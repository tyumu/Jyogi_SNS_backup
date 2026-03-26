import hashlib
import json
import os
import sqlite3
from datetime import datetime
from typing import Iterable, List, Optional

from dotenv import load_dotenv

load_dotenv()

MANIFEST_PATH = os.getenv("BACKUP_MANIFEST_PATH", os.path.join("artifacts", "backup_manifest.json"))


def calculate_sha256(file_path):
    if not os.path.exists(file_path):
        return None
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def create_manifest(db_path: str, zip_paths: Iterable[str], max_todo_id_zipped: Optional[int] = None) -> str:
    """backup.db と複数 ZIP をまとめたマニフェストを BACKUP_MANIFEST_PATH（既定: artifacts/backup_manifest.json）に生成

    max_todo_id_zipped: 画像ZIP生成時点で "ここまでの todos.id は ZIP 済み" という境界
    """
    print("\n📌 マニフェスト作成...")

    db_abs = os.path.abspath(db_path)
    zip_abs_list = [os.path.abspath(p) for p in zip_paths]

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "counts": {
            "todos": 0,
            "replies": 0,
            "image_urls": 0,
        },
        "hashes": {},
        "zip_files": [],  # 追跡しておく（絶対パス）
        "image_max_todo_id_zipped": int(max_todo_id_zipped or 0),
    }

    # 1. SQLite の件数
    if os.path.exists(db_abs):
        conn = sqlite3.connect(db_abs)
        cur = conn.cursor()
        try:
            for table in ["todos", "replies"]:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    manifest["counts"][table] = cur.fetchone()[0]
                except sqlite3.OperationalError:
                    pass

            try:
                cur.execute(
                    """
                    SELECT COUNT(image_url)
                    FROM todos
                    WHERE image_url IS NOT NULL AND TRIM(image_url) != ''
                    """
                )
                manifest["counts"]["image_urls"] = cur.fetchone()[0]
            except sqlite3.OperationalError:
                pass
        finally:
            cur.close()
            conn.close()

    # 2. ハッシュ値
    files_to_hash: List[str] = [db_abs] + list(zip_abs_list)
    for file_path in files_to_hash:
        if os.path.exists(file_path):
            manifest["hashes"][file_path] = calculate_sha256(file_path)
            if file_path != db_abs:
                manifest["zip_files"].append(file_path)

    # 出力先は環境変数 BACKUP_MANIFEST_PATH（既定: artifacts/backup_manifest.json）
    manifest_path = os.path.abspath(MANIFEST_PATH)
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4, ensure_ascii=False)

    print(f"  ✓ マニフェストを {manifest_path} に保存しました。")
    return manifest_path


__all__ = ["create_manifest"]
