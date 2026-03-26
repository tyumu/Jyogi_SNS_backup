import hashlib
import io
import json
import os
import random
import sqlite3
import zipfile
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv
from PIL import Image, UnidentifiedImageError

load_dotenv()

DB_FILE = os.getenv("BACKUP_DB_PATH", os.path.join("artifacts", "backup", "backup.db"))
MANIFEST_FILE = os.getenv("BACKUP_MANIFEST_PATH", os.path.join("artifacts", "backup_manifest.json"))
RESTORE_OK_FILE = os.getenv("RESTORE_OK_MARKER", os.path.join("artifacts", "restore_smoke_test.ok"))


def calculate_sha256(file_path):
    if not os.path.exists(file_path):
        return None
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


@dataclass
class ManifestData:
    counts: dict
    hashes: dict
    zip_files: List[str]
    image_max_todo_id_zipped: int


class ManifestAdapter:
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path

    def load(self):
        if not os.path.exists(self.manifest_path):
            raise FileNotFoundError(f"'{self.manifest_path}' が見当たりません。")
        with open(self.manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        return ManifestData(
            counts=manifest.get("counts", {}),
            hashes=manifest.get("hashes", {}),
            zip_files=manifest.get("zip_files", []),
            image_max_todo_id_zipped=int(manifest.get("image_max_todo_id_zipped", 0) or 0),
        )


class SQLiteBackupAdapter:
    def __init__(self, db_path):
        self.db_path = db_path

    def exists(self):
        return os.path.exists(self.db_path)

    def sha256(self):
        return calculate_sha256(self.db_path)

    def count_table_rows(self, table_name):
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0]
        finally:
            conn.close()


class ZipImageBackupAdapter:
    def __init__(self, zip_path):
        self.zip_path = zip_path

    def exists(self):
        return os.path.exists(self.zip_path)

    def sha256(self):
        return calculate_sha256(self.zip_path)

    def list_image_files(self):
        with zipfile.ZipFile(self.zip_path, "r") as zf:
            return [
                name
                for name in zf.namelist()
                if name.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
            ]

    def verify_image_payload(self, image_name):
        with zipfile.ZipFile(self.zip_path, "r") as zf:
            with zf.open(image_name) as f:
                img_data = f.read()
        img = Image.open(io.BytesIO(img_data))
        img.verify()


def write_restore_ok_marker(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("OK\n")


def verify_backup():
    print("バックアップ審査を開始...\n")
    all_ok = True

    manifest_adapter = ManifestAdapter(MANIFEST_FILE)
    sqlite_adapter = SQLiteBackupAdapter(DB_FILE)

    print("📌 マニフェストの確認")
    try:
        manifest = manifest_adapter.load()
        print("  ✓ マニフェストを読み込みました。")
    except Exception as e:
        print(f"  ✗ マニフェストファイルの読み込みエラーです: {e}")
        return

    print("\n📌 データベースの確認")
    if not sqlite_adapter.exists():
        print(f"  ✗ '{DB_FILE}' が見当たりません。")
        all_ok = False
    else:
        actual_db_hash = sqlite_adapter.sha256()
        db_key_abs = os.path.abspath(DB_FILE)
        expected_db_hash = manifest.hashes.get(db_key_abs) or manifest.hashes.get(os.path.basename(DB_FILE))
        if actual_db_hash == expected_db_hash:
            print(f"  ✓ ハッシュ値一致: {DB_FILE} は改ざんされていません。")
        else:
            print(f"  ✗ ハッシュ値不一致: {DB_FILE} が作成時から変化しているか、壊れています。")
            all_ok = False

        for table in ["todos", "replies"]:
            try:
                count = sqlite_adapter.count_table_rows(table)
                expected = manifest.counts.get(table, 0)
                if count == expected:
                    print(f"  ✓ {table} テーブル: {count} 件（マニフェストと完全に一致）")
                else:
                    print(f"  ✗ {table} テーブル: 件数不一致 (実体:{count}件 vs マニフェスト:{expected}件)")
                    all_ok = False
            except sqlite3.OperationalError:
                print(f"  ✗ {table} テーブルが存在しません。")
                all_ok = False
            except Exception as e:
                print(f"  ✗ {table} テーブル確認中にエラーです: {e}")
                all_ok = False

    print("\n📌 画像ZIPの確認")
    total_img_count = 0
    expected_img_count = manifest.counts.get("image_urls", 0)

    zip_files = manifest.zip_files or []
    if not zip_files:
        print("  ⚠ マニフェストに zip_files 情報がありません。古い形式の可能性があります。")

    for zip_entry in zip_files:
        # マニフェストには絶対パスを記録しているが、旧形式との互換も考慮
        if os.path.isabs(zip_entry):
            zip_path = zip_entry
        else:
            zip_path = os.path.abspath(zip_entry)
        adapter = ZipImageBackupAdapter(zip_path)

        if not adapter.exists():
            print(f"  ✗ '{zip_path}' が見当たりません。")
            all_ok = False
            continue

        actual_zip_hash = adapter.sha256()
        expected_zip_hash = manifest.hashes.get(zip_path) or manifest.hashes.get(os.path.basename(zip_path))
        if actual_zip_hash == expected_zip_hash:
            print(f"  ✓ ハッシュ値一致: {zip_path} は改ざんされていません。")
        else:
            print(f"  ✗ ハッシュ値不一致: {zip_path} はマニフェスト作成時と異なります。")
            all_ok = False

        try:
            image_files = adapter.list_image_files()
            img_count = len(image_files)
            total_img_count += img_count
            print(f"  ✓ {zip_path} 内の画像数: {img_count} 枚")

            if img_count > 0:
                sample_size = min(1, img_count)
                samples = random.sample(image_files, sample_size)
                print(f"    ランダムに {sample_size} 枚抽出してデータ破損を検査します...")
                for img_name in samples:
                    try:
                        adapter.verify_image_payload(img_name)
                        print(f"      ✓ {img_name}: 正常な画像データです。")
                    except UnidentifiedImageError:
                        print(f"      ✗ {img_name}: 画像として読み込めません。中身が壊れています。")
                        all_ok = False
                    except Exception as e:
                        print(f"      ✗ {img_name}: 検査中エラーです: {e}")
                        all_ok = False
        except zipfile.BadZipFile:
            print(f"  ✗ {zip_path}: ZIPファイル自体が壊れています。")
            all_ok = False
        except Exception as e:
            print(f"  ✗ {zip_path}: 画像の確認中に未知のエラーです: {e}")
            all_ok = False

    if zip_files:
        if total_img_count == expected_img_count:
            print(f"  ✓ 全 ZIP 合計画像数: {total_img_count} 枚（マニフェストと完全に一致）")
        else:
            print(f"  ⚠ 全 ZIP 合計画像数不一致 (実体:{total_img_count}枚 vs マニフェスト:{expected_img_count}件)")
            all_ok = False

    print("\n" + "=" * 40)
    print("【最終判定】")
    if all_ok:
        print("  ==> [ OK ] 完璧なバックアップです。")
        try:
            write_restore_ok_marker(RESTORE_OK_FILE)
            print(f"  ✓ OKファイルを生成: {RESTORE_OK_FILE}")
        except Exception as e:
            print(f"  ✗ OKファイル生成エラー: {e}")
    else:
        print("  ==> [ NG ] どこかに不整合やエラー")
    print("=" * 40)


if __name__ == "__main__":
    verify_backup()