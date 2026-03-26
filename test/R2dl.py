import boto3
import os
from pathlib import Path

def column_exists(sqlite_conn, table_name, column_name):
    cur = sqlite_conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]  # row[1] = column name
    return column_name in cols

def extract_image_urls(sqlite_conn):
    """SQLiteから画像URLを抽出（存在するカラムのみ）"""
    cur = sqlite_conn.cursor()
    urls = set()

    # todos だけが image_url を持つ前提
    if column_exists(sqlite_conn, "todos", "image_url"):
        cur.execute("""
            SELECT image_url
            FROM todos
            WHERE image_url IS NOT NULL
              AND TRIM(image_url) != ''
        """)
        for (url,) in cur.fetchall():
            urls.add(url.strip())

    return list(urls)

def download_from_r2(image_urls, output_dir="images"):
    """R2から画像ダウンロード"""
    # R2 設定
    r2 = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_TEMP_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_TEMP_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_TEMP_SECRET_ACCESS_KEY"),
    )
    
    bucket = os.getenv("R2_TEMP_BUCKET_NAME")
    
    # 出力ディレクトリ作成
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"\n📌 Downloading {len(image_urls)} images from R2...")
    
    for i, url in enumerate(image_urls, 1):
        try:
            # URL から ファイル名抽出
            filename = url.split("/")[-1]
            filepath = os.path.join(output_dir, filename)
            
            # R2 からダウンロード
            r2.download_file(bucket, filename, filepath)
            
            print(f"  ✓ [{i}/{len(image_urls)}] {filename}")
        except Exception as e:
            print(f"  ✗ [{i}/{len(image_urls)}] {url} - Error: {e}")
    
    print(f"✅ Downloaded {len(image_urls)} images to {output_dir}/")
    return output_dir

def create_zip(image_dir, output_zip="images_backup.zip"):
    """画像を ZIP に詰める"""
    import zipfile
    
    print(f"\n📌 Creating ZIP: {output_zip}...")
    
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename in os.listdir(image_dir):
            filepath = os.path.join(image_dir, filename)
            if os.path.isfile(filepath):
                zf.write(filepath, arcname=filename)
    
    size_mb = os.path.getsize(output_zip) / (1024 ** 2)
    print(f"✅ ZIP created: {output_zip} ({size_mb:.2f} MB)")