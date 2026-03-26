import hashlib
from datetime import datetime
import os
import sqlite3
import json

def calculate_sha256(file_path):
    if not os.path.exists(file_path):
        return None
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # メモリを食い潰さないように少しずつ読み込む
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def create_manifest(db_path, zip_path="images_backup.zip"):
    print("\n📌 マニフェスト作成...")
    
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "counts": {
            "todos": 0,
            "replies": 0,
            "image_urls": 0
        },
        "hashes": {}
    }

    # 1. 出来上がったSQLiteデータベースから直接件数を数え直し
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # todos, repliesの件数
        for table in ["todos", "replies"]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                manifest["counts"][table] = cur.fetchone()[0]
            except sqlite3.OperationalError:
                pass
        
        # image_urlの件数（todosテーブルに存在すると仮定）
        try:
            cur.execute("SELECT COUNT(image_url) FROM todos WHERE image_url IS NOT NULL AND TRIM(image_url) != ''")
            manifest["counts"]["image_urls"] = cur.fetchone()[0]
        except sqlite3.OperationalError:
            pass
            
        conn.close()

    # 2. ハッシュ値の計算（改ざんチェック用）
    for file_path in [db_path, zip_path]:
        if os.path.exists(file_path):
            manifest["hashes"][os.path.basename(file_path)] = calculate_sha256(file_path)

    # 3. artifactsディレクトリを作って保存
    os.makedirs("artifacts", exist_ok=True)
    manifest_path = "artifacts/backup_manifest.json"
    
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4, ensure_ascii=False)
        
    print(f"  ✓ マニフェストを {manifest_path} に保存しました。")