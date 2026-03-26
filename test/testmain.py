import psycopg2
import sqlite3
import os
from dotenv import load_dotenv
from R2dl import extract_image_urls, download_from_r2, create_zip
from scriptsTest.manifest_generate import create_manifest
from supabasedl import save_to_sqlite
from scriptsTest.restore_smoke_test import verify_backup

# .envファイルを読み込む
load_dotenv()

db_url = os.getenv("POSTGRES_URL")
BACKUP_DB = "backup.db"


def main():
    try:
        print("🔄 PostgreSQL接続...")
        conn = psycopg2.connect(db_url)
        print("✓ 接続成功")
        
        print("\n🔄 SQLiteにバックアップ中...")
        save_to_sqlite(conn)
        
        print("\n🔄 SQLiteから画像URL抽出...")
        sqlite_conn = sqlite3.connect(BACKUP_DB)
        image_urls = extract_image_urls(sqlite_conn)
        sqlite_conn.close()
        
        if image_urls:
            print(f"✓ Found {len(image_urls)} unique image URLs")
            
            print("\n🔄 R2からダウンロード...")
            image_dir = download_from_r2(image_urls)
            
            print("\n🔄 ZIP作成...")
            create_zip(image_dir)
        else:
            print("ℹ️ No images to backup")

        
        # ファイルサイズ確認
        size_mb = os.path.getsize(BACKUP_DB) / (1024 ** 2)
        print(f"\n✅ Backup complete: {BACKUP_DB} ({size_mb:.2f} MB)")
        
        # マニフェスト生成
        create_manifest(BACKUP_DB, "images_backup.zip")
        
        # バックアップ審査
        verify_backup()

        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()