import hashlib
import glob
import io
import json
import os
import random
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv
from PIL import Image, UnidentifiedImageError

from infra_logging import log_infra

load_dotenv()

DB_FILE = os.getenv("BACKUP_DB_PATH", os.path.join("artifacts", "backup", "jyogi_sns_backup.db"))
MANIFEST_FILE = os.getenv("BACKUP_MANIFEST_PATH", os.path.join("artifacts", "backup_manifest.json"))
RESTORE_OK_FILE = os.getenv("RESTORE_OK_MARKER", os.path.join("artifacts", "restore_smoke_test.ok"))


def list_yearly_backup_db_paths_from_template(path_template: str) -> List[str]:
    backup_dir = os.path.dirname(path_template) or "."
    base = os.path.basename(path_template)
    stem, ext = os.path.splitext(base)
    if not ext:
        ext = ".db"

    pattern = os.path.join(backup_dir, f"{stem}_*{ext}")
    regex = re.compile(rf"^{re.escape(stem)}_(\d{{4}}){re.escape(ext)}$")

    year_file_pairs = []
    for path in glob.glob(pattern):
        name = os.path.basename(path)
        match = regex.match(name)
        if not match:
            continue
        year_file_pairs.append((int(match.group(1)), os.path.abspath(path)))

    return [p for _, p in sorted(year_file_pairs, key=lambda x: x[0])]


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
    db_files: List[str]
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
            db_files=manifest.get("db_files", []),
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
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cur.fetchone()[0]
            except sqlite3.OperationalError:
                # 一部年のDBでテーブルが存在しないのは許容（0件として扱う）
                return 0
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

    issues = []  # インフラログ用: 人手対応が必要な問題の概要を溜めておく

    manifest_adapter = ManifestAdapter(MANIFEST_FILE)
    print("📌 マニフェストの確認")
    try:
        manifest = manifest_adapter.load()
        print("  ✓ マニフェストを読み込みました。")
    except Exception as e:
        print(f"  ✗ マニフェストファイルの読み込みエラーです: {e}")
        return

    print("\n📌 データベース(年別)の確認")
    db_files = [os.path.abspath(p) for p in (manifest.db_files or [])]

    # 旧マニフェスト互換: db_files が無い場合は既存の年別DB一覧、最後に単一DBパスへフォールバック
    if not db_files:
        db_files = list_yearly_backup_db_paths_from_template(DB_FILE)
    if not db_files:
        fallback_db = os.path.abspath(DB_FILE)
        if os.path.exists(fallback_db):
            db_files = [fallback_db]

    if not db_files:
        print("  ⚠ 確認対象のDBファイルがありません。")
    else:
        total_counts = {"todos": 0, "replies": 0}

        for db_path in db_files:
            sqlite_adapter = SQLiteBackupAdapter(db_path)

            if not sqlite_adapter.exists():
                print(f"  ✗ '{db_path}' が見当たりません。")
                all_ok = False
                issues.append({"type": "db_missing", "db_path": db_path})
                continue

            actual_db_hash = sqlite_adapter.sha256()
            expected_db_hash = manifest.hashes.get(db_path) or manifest.hashes.get(os.path.basename(db_path))
            if actual_db_hash == expected_db_hash:
                print(f"  ✓ ハッシュ値一致: {db_path} は改ざんされていません。")
            else:
                print(f"  ✗ ハッシュ値不一致: {db_path} が作成時から変化しているか、壊れています。")
                all_ok = False
                issues.append({
                    "type": "db_hash_mismatch",
                    "db_path": db_path,
                    "actual_hash": actual_db_hash,
                    "expected_hash": expected_db_hash,
                })

            for table in ["todos", "replies"]:
                try:
                    count = sqlite_adapter.count_table_rows(table)
                    total_counts[table] += count
                except Exception as e:
                    print(f"  ✗ {db_path} の {table} テーブル確認中にエラーです: {e}")
                    all_ok = False
                    issues.append({
                        "type": "table_check_error",
                        "db_path": db_path,
                        "table": table,
                        "error": str(e),
                    })

        for table in ["todos", "replies"]:
            actual_total = total_counts[table]
            expected = manifest.counts.get(table, 0)
            if actual_total == expected:
                print(f"  ✓ {table} 合計: {actual_total} 件（マニフェストと完全に一致）")
            else:
                print(f"  ✗ {table} 合計: 件数不一致 (実体:{actual_total}件 vs マニフェスト:{expected}件)")
                all_ok = False
                issues.append({
                    "type": "table_count_mismatch",
                    "table": table,
                    "actual": actual_total,
                    "expected": expected,
                })

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
            issues.append({"type": "zip_missing", "zip_path": zip_path})
            continue

        actual_zip_hash = adapter.sha256()
        expected_zip_hash = manifest.hashes.get(zip_path) or manifest.hashes.get(os.path.basename(zip_path))
        if actual_zip_hash == expected_zip_hash:
            print(f"  ✓ ハッシュ値一致: {zip_path} は改ざんされていません。")
        else:
            print(f"  ✗ ハッシュ値不一致: {zip_path} はマニフェスト作成時と異なります。")
            all_ok = False
            issues.append({
                "type": "zip_hash_mismatch",
                "zip_path": zip_path,
                "actual_hash": actual_zip_hash,
                "expected_hash": expected_zip_hash,
            })

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
                        issues.append({
                            "type": "image_invalid",
                            "zip_path": zip_path,
                            "image_name": img_name,
                            "error": "UnidentifiedImageError",
                        })
                    except Exception as e:
                        print(f"      ✗ {img_name}: 検査中エラーです: {e}")
                        all_ok = False
                        issues.append({
                            "type": "image_check_error",
                            "zip_path": zip_path,
                            "image_name": img_name,
                            "error": str(e),
                        })
        except zipfile.BadZipFile:
            print(f"  ✗ {zip_path}: ZIPファイル自体が壊れています。")
            all_ok = False
            issues.append({"type": "zip_corrupted", "zip_path": zip_path})
        except Exception as e:
            print(f"  ✗ {zip_path}: 画像の確認中に未知のエラーです: {e}")
            all_ok = False
            issues.append({"type": "zip_check_error", "zip_path": zip_path, "error": str(e)})

    if zip_files:
        if total_img_count == expected_img_count:
            print(f"  ✓ 全 ZIP 合計画像数: {total_img_count} 枚（マニフェストと完全に一致）")
        else:
            print(f"  ⚠ 全 ZIP 合計画像数不一致 (実体:{total_img_count}枚 vs マニフェスト:{expected_img_count}件)")
            all_ok = False
            issues.append({
                "type": "image_count_mismatch",
                "actual_total": total_img_count,
                "expected_total": expected_img_count,
            })

    print("\n" + "=" * 40)
    print("【最終判定】")
    if all_ok:
        print("  ==> [ OK ] 完璧なバックアップです。")
        try:
            write_restore_ok_marker(RESTORE_OK_FILE)
            print(f"  ✓ OKファイルを生成: {RESTORE_OK_FILE}")
            try:
                log_infra(
                    source="restore_smoke_test",
                    log_level="info",
                    event_type="backup_verification_ok",
                    message="バックアップ整合性チェックに成功しました",
                    detail={
                        "db_files": db_files,
                        "manifest_path": MANIFEST_FILE,
                        "restore_ok_marker": RESTORE_OK_FILE,
                    },
                    resolved=True,
                )
            except Exception:
                # ログ出力で本処理を止めない
                pass
        except Exception as e:
            print(f"  ✗ OKファイル生成エラー: {e}")
            try:
                log_infra(
                    source="restore_smoke_test",
                    log_level="error",
                    event_type="restore_ok_marker_write_failed",
                    message="restore_smoke_test.ok の作成に失敗しました",
                    detail={
                        "marker_path": RESTORE_OK_FILE,
                        "error": str(e),
                    },
                    resolved=False,
                )
            except Exception:
                pass
    else:
        print("  ==> [ NG ] どこかに不整合やエラー")
        try:
            log_infra(
                source="restore_smoke_test",
                log_level="error",
                event_type="backup_verification_failed",
                message="バックアップ整合性チェックに失敗しました",
                detail={
                    "db_files": db_files,
                    "manifest_path": MANIFEST_FILE,
                    "issues": issues,
                },
                resolved=False,
            )
        except Exception:
            pass
    print("=" * 40)


if __name__ == "__main__":
    verify_backup()