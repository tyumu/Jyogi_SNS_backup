import os
import json
import psycopg2
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

from infra_logging import log_infra

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


def clear_restore_ok_marker():
    try:
        if os.path.exists(RESTORE_OK_MARKER):
            os.remove(RESTORE_OK_MARKER)
            print(f"復元テスト成功マーカーを削除しました: {RESTORE_OK_MARKER}")
    except Exception as e:
        # マーカー削除に失敗しても本処理は成功扱いとし、警告だけ出す
        msg = f"復元テスト成功マーカー削除に失敗しました: {e}"
        print(msg)
        try:
            log_infra(
                source="delete_with_guard",
                log_level="WARN",
                event_type="restore_ok_marker_delete_failed",
                message="restore_smoke_test.ok の削除に失敗しました",
                detail={
                    "marker_path": RESTORE_OK_MARKER,
                    "error": str(e),
                },
                resolved=False,
            )
        except Exception:
            # ログ出力自体が失敗しても、削除処理は続行する
            pass


def clear_delete_plan_file():
    try:
        if os.path.exists(DELETE_PLAN_PATH):
            os.remove(DELETE_PLAN_PATH)
            print(f"削除計画ファイルを削除しました: {DELETE_PLAN_PATH}")
    except Exception as e:
        msg = f"削除計画ファイルの削除に失敗しました: {e}"
        print(msg)
        try:
            log_infra(
                source="delete_with_guard",
                log_level="WARN",
                event_type="delete_plan_delete_failed",
                message="delete_plan.json の削除に失敗しました",
                detail={
                    "plan_path": DELETE_PLAN_PATH,
                    "error": str(e),
                },
                resolved=False,
            )
        except Exception:
            pass


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
            try:
                log_infra(
                    source="delete_with_guard",
                    log_level="info",
                    event_type="delete_dry_run_completed",
                    message="削除DRY-RUNが完了しました",
                    detail={
                        "threshold_days": threshold_days,
                        "todos_planned": len(todo_ids),
                        "replies_planned": len(reply_ids),
                        "images_planned": len(image_keys),
                        "todos_found_before_delete": before_todos,
                        "replies_found_before_delete": before_replies,
                    },
                    resolved=True,
                )
            except Exception:
                pass
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

        try:
            log_infra(
                source="delete_with_guard",
                log_level="info",
                event_type="delete_execute_completed",
                message="削除処理が正常終了しました",
                detail={
                    "threshold_days": threshold_days,
                    "todos_planned": len(todo_ids),
                    "replies_planned": len(reply_ids),
                    "images_planned": len(image_keys),
                    "todos_deleted": deleted_todos,
                    "replies_deleted": deleted_replies,
                    "images_deleted": deleted_images,
                    "image_delete_errors_count": len(image_errors),
                },
                resolved=True,
            )
        except Exception:
            pass

        # 削除が正常終了したら、次回以降のためにOKマーカーを消す
        clear_restore_ok_marker()

        # 削除計画ファイルも使い捨てにしておくことで、
        # 古い delete_plan.json を再利用した 2 回目以降実行のズレを防ぐ
        clear_delete_plan_file()

    except Exception as e:
        conn.rollback()
        print("削除中にエラー。ロールバックしました。")
        try:
            log_infra(
                source="delete_with_guard",
                log_level="error",
                event_type="delete_execute_failed",
                message="削除処理中にエラーが発生しました",
                detail={
                    "threshold_days": threshold_days,
                    "todos_planned": len(todo_ids),
                    "replies_planned": len(reply_ids),
                    "images_planned": len(image_keys),
                    "error": str(e),
                },
                resolved=False,
            )
        except Exception:
            pass
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    main()