import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("POSTGRES_URL")

try:
    print("わざとテーブル作成を試し中...")
    conn = psycopg2.connect(db_url)
    # エラーをすぐに発生させるための設定
    conn.autocommit = True
    cur = conn.cursor()

    # わざと書き込み（テーブル作成）を行ってみる
    cur.execute("CREATE TABLE yukari_test_table (id serial PRIMARY KEY, note text);")

    print("なんと、テーブルが作れてしまいました。設定ミスです。どこかで全権限を持つパスワードを使っていませんか？")
    
    # 散らかしたので一応お掃除しておきます
    cur.execute("DROP TABLE yukari_test_table;")

    cur.close()
    conn.close()

except Exception as e:
    print("素晴らしい！データベースから見事に怒られました。")
    print("つまり、読み取り専用の設定は完璧に機能しています")
    print(f"【エラーの証拠】: {e}")

import psycopg2
import os
from dotenv import load_dotenv

try:
    print("データの読み取りを開始します...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    cur.execute("SELECT * FROM todos LIMIT 5;")

    # fetchall()で結果を全部（今回はLIMIT 5なので最大5件）受け取ります
    rows = cur.fetchall()

    if not rows:
        print("データが空っぽです。RTLが正しく設定されているか、テーブルにデータが入っているか確認してください。")
    else:
        print("取ってきたデータ：")
        for row in rows:
            print(row)

    cur.close()
    conn.close()

except Exception as e:
    print("残念な結果です。")
    print(f"【エラーの証拠】: {e}")