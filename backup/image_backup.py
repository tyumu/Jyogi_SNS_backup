import os
import sqlite3
from datetime import datetime
from pathlib import Path

import boto3
from dotenv import load_dotenv

from infra_logging import log_infra

load_dotenv()

R2_ENDPOINT = os.getenv("R2_TEMP_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_TEMP_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_TEMP_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_TEMP_BUCKET_NAME")

# 既定は artifacts/images 配下にまとめる
IMAGES_DIR = os.getenv("BACKUP_IMAGES_DIR", os.path.join("artifacts", "images"))
MANIFEST_PATH = os.getenv("BACKUP_MANIFEST_PATH", os.path.join("artifacts", "backup_manifest.json"))


def _ensure_r2_client():
    if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
        raise RuntimeError("R2 の接続情報(R2_TEMP_*) が不足しています")
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def _load_last_max_id_from_manifest() -> int:
    """マニフェストから、すでに ZIP 済みの max(id) を取得（なければ 0）"""
    manifest_abspath = os.path.abspath(MANIFEST_PATH)
    if not os.path.exists(manifest_abspath):
        return 0
    try:
        import json

        with open(manifest_abspath, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        return int(manifest.get("image_max_todo_id_zipped", 0) or 0)
    except Exception:
        # 壊れている・旧形式などの場合はフルスキャンにフォールバック
        return 0


def iter_image_records(sqlite_path, min_id_exclusive: int = 0):
    """SQLite の todos から (id, filename, month_key) を列挙（id > min_id_exclusive のみ）"""
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, image_url, created_at
            FROM todos
            WHERE image_url IS NOT NULL
              AND TRIM(image_url) != ''
              AND id > ?
            ORDER BY id ASC
            """,
            (min_id_exclusive,),
        )
        for todo_id, url, created_at in cur.fetchall():
            if not url:
                continue
            # id は TEXT で入っているので Python 側で int に揃える
            try:
                todo_id_int = int(todo_id)
            except (TypeError, ValueError):
                log_infra(
                    source="image_backup.iter_image_records",
                    log_level="warn",
                    event_type="image_backup_invalid_todo_id",
                    message="非数値の todo_id がスキップされました",
                    detail={"raw_todo_id": todo_id, "url": url},
                )
                # 数値に変換できない id はスキップ
                continue

            filename = url.split("/")[-1]
            try:
                dt = datetime.fromisoformat(str(created_at))
            except Exception:
                log_infra(
                    source="image_backup.iter_image_records",
                    log_level="warn",
                    event_type="image_backup_invalid_created_at",
                    message="created_at のフォーマット不明のためスキップされました",
                    detail={
                        "raw_created_at": created_at,
                        "todo_id": todo_id,
                        "url": url,
                    },
                )
                # フォーマット不明な場合はスキップ
                continue
            month_key = dt.strftime("%Y%m")  # 例: 202603
            yield todo_id_int, filename, month_key
    finally:
        cur.close()
        conn.close()


def download_and_zip_images_by_month(sqlite_path):
    """backup.db をもとに R2 から画像をダウンロードし、月別 ZIP を作成/追記する

    戻り値: (更新した ZIP ファイルの絶対パスリスト, 今回までに ZIP 済みとなった max(id))
    """
    r2 = _ensure_r2_client()
    # 親ディレクトリごと作成（artifacts/images など）
    Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)

    last_max_id = _load_last_max_id_from_manifest()
    print(f"📌 画像バックアップ: 直近 ZIP 済み max(id) = {last_max_id}")

    # month_key -> set(filenames)
    monthly_files = {}
    processed_max_id = last_max_id

    for todo_id, filename, month_key in iter_image_records(sqlite_path, min_id_exclusive=last_max_id):
        monthly_files.setdefault(month_key, set()).add(filename)
        if todo_id > processed_max_id:
            processed_max_id = todo_id

    updated_zip_paths = []

    import zipfile

    for month_key, filenames in sorted(monthly_files.items()):
        zip_name = f"images_backup_{month_key}.zip"
        zip_path = os.path.abspath(os.path.join(IMAGES_DIR, zip_name))
        print(f"\n📌 月別ZIP処理: {zip_path} ({len(filenames)} files)")

        # 既存 ZIP 内のファイル一覧
        existing = set()
        if os.path.exists(zip_path):
            with zipfile.ZipFile(zip_path, "r") as zf:
                existing.update(zf.namelist())

        # 'a' モードで追記
        with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
            for filename in filenames:
                if filename in existing:
                    continue
                local_path = os.path.join(IMAGES_DIR, filename)
                if not os.path.exists(local_path):
                    try:
                        r2.download_file(R2_BUCKET_NAME, filename, local_path)
                        print(f"  ✓ downloaded {filename} from R2")
                    except Exception as e:
                        print(f"  ✗ failed to download {filename}: {e}")
                        try:
                            log_infra(
                                source="image_backup",
                                log_level="error",
                                event_type="image_download_failed",
                                message="R2 からの画像ダウンロードに失敗しました",
                                detail={
                                    "bucket": R2_BUCKET_NAME,
                                    "filename": filename,
                                    "error": str(e),
                                },
                                resolved=False,
                            )
                        except Exception:
                            pass
                        continue
                zf.write(local_path, arcname=filename)
                print(f"  ✓ added {filename} to {zip_path}")

        size_mb = os.path.getsize(zip_path) / (1024 ** 2)
        print(f"  ✓ {zip_path} updated ({size_mb:.2f} MB)")
        updated_zip_paths.append(zip_path)

    return updated_zip_paths, processed_max_id


__all__ = ["download_and_zip_images_by_month"]
