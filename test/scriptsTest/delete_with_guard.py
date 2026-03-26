import os
import json
import psycopg2
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
R2_ENDPOINT = os.getenv("R2_TEMP_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_TEMP_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_TEMP_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_TEMP_BUCKET_NAME")

VERIFY_ONLY = os.getenv("VERIFY_ONLY", "true").lower() == "true"
DELETE_ENABLED = os.getenv("DELETE_ENABLED", "false").lower() == "true"
RESTORE_OK_MARKER = os.getenv("RESTORE_OK_MARKER", "artifacts/restore_smoke_test.ok")
DELETE_PLAN_PATH = os.getenv("DELETE_PLAN_PATH", "artifacts/delete_plan.json")


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def load_delete_plan(path):
    with open(path, "r", encoding="utf-8") as f:
        plan = json.load(f)
    return plan


def require_restore_ok():
    if not os.path.exists(RESTORE_OK_MARKER):
        raise RuntimeError(
            f"復元テスト成功マーカーがありません: {RESTORE_OK_MARKER}\n"
            "先に復元テストを通してから削除してください。"
        )


def precheck_counts(conn, todo_ids, reply_ids):
    cur = conn.cursor()

    todo_count = 0
    reply_count = 0

    if todo_ids:
        cur.execute("SELECT COUNT(*) FROM todos WHERE id = ANY(%s)", (todo_ids,))
        todo_count = cur.fetchone()[0]

    if reply_ids:
        cur.execute("SELECT COUNT(*) FROM replies WHERE id = ANY(%s)", (reply_ids,))
        reply_count = cur.fetchone()[0]

    cur.close()
    return todo_count, reply_count


def delete_db_rows(conn, todo_ids, reply_ids):
    cur = conn.cursor()
    deleted_replies = 0
    deleted_todos = 0

    # FK都合で replies -> todos の順
    if reply_ids:
        cur.execute("DELETE FROM replies WHERE id = ANY(%s)", (reply_ids,))
        deleted_replies = cur.rowcount

    if todo_ids:
        cur.execute("DELETE FROM todos WHERE id = ANY(%s)", (todo_ids,))
        deleted_todos = cur.rowcount

    cur.close()
    return deleted_todos, deleted_replies


def delete_r2_objects(image_keys):
    if not image_keys:
        return 0, []

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto"
    )

    deleted = 0
    failed = []

    for batch in chunked(image_keys, 1000):
        payload = {"Objects": [{"Key": k} for k in batch], "Quiet": True}
        resp = s3.delete_objects(Bucket=R2_BUCKET_NAME, Delete=payload)

        # 削除成功
        deleted += len(resp.get("Deleted", []))

        # 削除失敗
        for err in resp.get("Errors", []):
            failed.append({"key": err.get("Key"), "code": err.get("Code"), "message": err.get("Message")})

    return deleted, failed


def write_result(result):
    os.makedirs("artifacts", exist_ok=True)
    out = "artifacts/delete_result.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"結果を保存しました: {out}")


def main():
    require_restore_ok()
    plan = load_delete_plan(DELETE_PLAN_PATH)

    todo_ids = plan.get("todo_ids", [])
    reply_ids = plan.get("reply_ids", [])
    image_keys = plan.get("image_keys", [])
    threshold_days = plan.get("threshold_days", "N/A")
    created_at = plan.get("created_at", "N/A")

    print("=== DELETE PLAN ===")
    print(f"created_at     : {created_at}")
    print(f"threshold_days : {threshold_days}日以上前")
    print(f"todos : {len(todo_ids)}")
    print(f"replies: {len(reply_ids)}")
    print(f"images : {len(image_keys)}")

    conn = psycopg2.connect(POSTGRES_URL)
    conn.autocommit = False

    try:
        before_todos, before_replies = precheck_counts(conn, todo_ids, reply_ids)
        print("\n=== PRECHECK ===")
        print(f"DB存在 todos : {before_todos}/{len(todo_ids)}")
        print(f"DB存在 replies: {before_replies}/{len(reply_ids)}")

        # 安全デフォルト: 検証のみ
        if VERIFY_ONLY or not DELETE_ENABLED:
            print("\n[DRY-RUN] 削除は実行しません。")
            result = {
                "mode": "dry-run",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "todos_planned": len(todo_ids),
                "replies_planned": len(reply_ids),
                "images_planned": len(image_keys),
                "todos_found_before_delete": before_todos,
                "replies_found_before_delete": before_replies
            }
            write_result(result)
            conn.rollback()
            return

        print("\n[EXECUTE] 削除を実行します...")
        deleted_todos, deleted_replies = delete_db_rows(conn, todo_ids, reply_ids)
        conn.commit()
        print(f"DB削除 todos={deleted_todos}, replies={deleted_replies}")

        # 画像は後段（DBが先、画像が後）
        deleted_images, image_errors = delete_r2_objects(image_keys)
        print(f"R2削除 images={deleted_images}, errors={len(image_errors)}")

        result = {
            "mode": "execute",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "todos_deleted": deleted_todos,
            "replies_deleted": deleted_replies,
            "images_deleted": deleted_images,
            "image_delete_errors": image_errors
        }
        write_result(result)

    except Exception as e:
        conn.rollback()
        print("削除中にエラー。ロールバックしました。")
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    main()