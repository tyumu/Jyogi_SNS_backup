import sqlite3
import zipfile
import random
import io
import os
import json
import hashlib
from PIL import Image, UnidentifiedImageError

# 確認するファイル群
DB_FILE = "backup.db"
ZIP_FILE = "images_backup.zip"
MANIFEST_FILE = "artifacts/backup_manifest.json"

def calculate_sha256(file_path):
    if not os.path.exists(file_path):
        return None
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def verify_backup():
    print("バックアップ審査を開始...\n")
    all_ok = True

    # ---------------------------------------------------------
    # 0. マニフェストの読み込み
    # ---------------------------------------------------------
    print("📌 マニフェストの確認")
    if not os.path.exists(MANIFEST_FILE):
        print(f"  ✗ '{MANIFEST_FILE}' が見当たりません。")
        return  # 基準がないのでここで打ち切ります
    
    try:
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        print("  ✓ マニフェストを読み込みました。")
    except Exception as e:
        print(f"  ✗ マニフェストファイルの読み込みエラーです: {e}")
        return

    expected_counts = manifest.get("counts", {})
    expected_hashes = manifest.get("hashes", {})

    # ---------------------------------------------------------
    # 1. SQLiteのデータ件数＆ハッシュチェック
    # ---------------------------------------------------------
    print("\n📌 データベースの確認")
    if not os.path.exists(DB_FILE):
        print(f"  ✗ '{DB_FILE}' が見当たりません。")
        all_ok = False
    else:
        # ハッシュ値の照合
        actual_db_hash = calculate_sha256(DB_FILE)
        if actual_db_hash == expected_hashes.get(DB_FILE):
            print(f"  ✓ ハッシュ値一致: {DB_FILE} は改ざんされていません。")
        else:
            print(f"  ✗ ハッシュ値不一致: {DB_FILE} が作成時から変化しているか、壊れています。")
            all_ok = False

        # 件数の照合
        try:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            for table in ["todos", "replies"]:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    expected = expected_counts.get(table, 0)
                    
                    if count == expected:
                        print(f"  ✓ {table} テーブル: {count} 件（マニフェストと完全に一致）")
                    else:
                        print(f"  ✗ {table} テーブル: 件数不一致 (実体:{count}件 vs マニフェスト:{expected}件)")
                        all_ok = False
                except sqlite3.OperationalError:
                    print(f"  ✗ {table} テーブルが存在しません。")
                    all_ok = False
            conn.close()
        except Exception as e:
            print(f"  ✗ データベースへの接続でエラーです: {e}")
            all_ok = False

    # ---------------------------------------------------------
    # 2 & 3. ZIP内のファイル数確認 ＆ ランダム画像チェック
    # ---------------------------------------------------------
    print("\n📌 画像ZIPの確認")
    if not os.path.exists(ZIP_FILE):
        print(f"  ✗ '{ZIP_FILE}' が見当たりません。")
        all_ok = False
    else:
        # ハッシュ値の照合
        actual_zip_hash = calculate_sha256(ZIP_FILE)
        if actual_zip_hash == expected_hashes.get(ZIP_FILE):
            print(f"  ✓ ハッシュ値一致: {ZIP_FILE} は改ざんされていません。")
        else:
            print(f"  ✗ ハッシュ値不一致: {ZIP_FILE} はマニフェスト作成時と異なります。")
            all_ok = False

        try:
            with zipfile.ZipFile(ZIP_FILE, "r") as zf:
                image_files = [f for f in zf.namelist() if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))]
                actual_img_count = len(image_files)
                expected_img_count = expected_counts.get("image_urls", 0)

                # 件数の照合
                if actual_img_count == expected_img_count:
                    print(f"  ✓ ZIP内の画像数: {actual_img_count} 枚（マニフェストと完全に一致）")
                else:
                    print(f"  ⚠ ZIP内の画像数不一致 (実体:{actual_img_count}枚 vs マニフェスト:{expected_img_count}件)")
                    print("    ※ダウンロードに失敗した画像がある可能性があります。")
                    # これは致命的ではないかもしれませんが、一応オールグリーンは剥奪
                    all_ok = False

                # ランダム画像検査
                if actual_img_count > 0:
                    sample_size = min(3, actual_img_count)
                    samples = random.sample(image_files, sample_size)
                    print(f"  ランダムに {sample_size} 枚抽出してデータ破損を検査します...")

                    for img_name in samples:
                        with zf.open(img_name) as f:
                            img_data = f.read()
                            try:
                                img = Image.open(io.BytesIO(img_data))
                                img.verify()
                                print(f"    ✓ {img_name}: 正常な画像データです。")
                            except UnidentifiedImageError:
                                print(f"    ✗ {img_name}: 画像として読み込めません。中身が壊れています。")
                                all_ok = False
                else:
                    print("  ⚠ ZIPの中に画像が1枚もありません。")

        except zipfile.BadZipFile:
            print("  ✗ ZIPファイル自体が壊れています。")
            all_ok = False
        except Exception as e:
            print(f"  ✗ 画像の確認中に未知のエラーです: {e}")
            all_ok = False

    # ---------------------------------------------------------
    # 4. 最終判定 (OK/NG)
    # ---------------------------------------------------------
    print("\n" + "="*40)
    print("【最終判定】")
    if all_ok:
        print("  ==> [ OK ] 完璧なバックアップです。")
        # OKファイルを生成
        try:
            ok_path = os.path.join("artifacts", "restore_smoke_test.ok")
            with open(ok_path, "w", encoding="utf-8") as f:
                f.write("OK\n")
            print(f"  ✓ OKファイルを生成: {ok_path}")
        except Exception as e:
            print(f"  ✗ OKファイル生成エラー: {e}")
    else:
        print("  ==> [ NG ] どこかに不整合やエラー")
    print("="*40)

if __name__ == "__main__":
    verify_backup()