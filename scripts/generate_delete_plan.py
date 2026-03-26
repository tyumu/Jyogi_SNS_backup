import os
import json
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")

# 3ヶ月前より古いデータを削除対象
DELETION_THRESHOLD_DAYS = 90


def get_todos_older_than(conn, days=DELETION_THRESHOLD_DAYS):
    """指定日数より古い todos を取得"""
    cur = conn.cursor()
    
    threshold_date = datetime.now(timezone.utc) - timedelta(days=days)
    
    cur.execute("""
        SELECT id, image_url, created_at
        FROM todos
        WHERE created_at < %s
        ORDER BY created_at ASC
    """, (threshold_date,))
    
    todos = cur.fetchall()
    cur.close()
    return todos


def get_replies_for_todos(conn, todo_ids):
    """削除対象の todos に紐付く replies を取得"""
    if not todo_ids:
        return []
    
    cur = conn.cursor()
    cur.execute("""
        SELECT id, post_id
        FROM replies
        WHERE post_id = ANY(%s)
    """, (todo_ids,))
    
    replies = cur.fetchall()
    cur.close()
    return replies


def generate_delete_plan():
    """3ヶ月前より古い todos/replies を一括削除する計画を生成"""
    
    conn = psycopg2.connect(POSTGRES_URL)
    
    # 古い todos を取得
    todos = get_todos_older_than(conn, DELETION_THRESHOLD_DAYS)
    todo_ids = [t[0] for t in todos]
    
    # 画像URL抽出
    image_keys = [t[1] for t in todos if t[1]]
    
    # replies を取得
    replies = get_replies_for_todos(conn, todo_ids)
    reply_ids = [r[0] for r in replies]
    
    conn.close()
    
    plan = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "threshold_days": DELETION_THRESHOLD_DAYS,
        "todo_ids": todo_ids,
        "reply_ids": reply_ids,
        "image_keys": image_keys,
    }
    
    return plan


def main():
    plan = generate_delete_plan()
    
    os.makedirs("artifacts", exist_ok=True)
    out = "artifacts/delete_plan.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ 削除計画を生成: {out}")
    print(f"  threshold: {DELETION_THRESHOLD_DAYS}日以上前")
    print(f"  todos_count: {len(plan['todo_ids'])}")
    print(f"  replies_count: {len(plan['reply_ids'])}")
    print(f"  image_count: {len(plan['image_keys'])}")


if __name__ == "__main__":
    main()