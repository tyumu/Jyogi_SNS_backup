import json
import os
import uuid
from typing import Any, Dict, Optional

import psycopg2
from dotenv import load_dotenv


load_dotenv()

# 専用の接続先があれば INFRA_LOG_DB_URL を使い、無ければ通常の POSTGRES_URL を使う
INFRA_LOG_DB_URL = os.getenv("INFRA_LOG_DB_URL") or os.getenv("POSTGRES_URL")
# 本番 / ステージング / ローカル などの環境識別子
INFRA_ENVIRONMENT = os.getenv("INFRA_ENVIRONMENT", "prod")

# 1回の main.py 実行単位で共有する run_id
_RUN_ID: Optional[str] = None


def get_run_id() -> str:
    global _RUN_ID
    if _RUN_ID is None:
        _RUN_ID = str(uuid.uuid4())
    return _RUN_ID


def log_infra(
    *,
    source: str,
    log_level: str,
    event_type: str,
    message: str,
    detail: Optional[Dict[str, Any]] = None,
    resolved: bool = False,
) -> None:
    """infrastructure_logs テーブルへの簡易ロガー

    ログ保存に失敗しても、バックアップ処理自体は止めない（標準出力に warning を出すだけ）。
    """

    if not INFRA_LOG_DB_URL:
        # 接続先が未設定なら何もしない（開発環境など）
        print(f"[infra-log skipped] {log_level} {source} {event_type}: {message}")
        return

    try:
        conn = psycopg2.connect(INFRA_LOG_DB_URL)
        cur = conn.cursor()

        run_id = get_run_id()
        env = INFRA_ENVIRONMENT

        # detail は JSONB カラムに詰める（None の場合は NULL）
        detail_json: Optional[str] = None
        if detail is not None:
            try:
                detail_json = json.dumps(detail, ensure_ascii=False)
            except TypeError:
                # シリアライズできない場合は文字列にフォールバック
                detail_json = json.dumps({"raw": str(detail)}, ensure_ascii=False)

        cur.execute(
            """
            INSERT INTO public.infrastructure_logs
                (id, created_at, run_id, environment, source, log_level,
                 event_type, message, detail, resolved)
            VALUES (
                gen_random_uuid(),
                NOW(),
                %s,  -- run_id
                %s,  -- environment
                %s,  -- source
                %s,  -- log_level
                %s,  -- event_type
                %s,  -- message
                %s::jsonb,  -- detail
                %s   -- resolved
            )
            """,
            (
                run_id,
                env,
                source,
                log_level,
                event_type,
                message,
                detail_json,
                resolved,
            ),
        )
        conn.commit()
    except Exception as e:  # ログ失敗で本処理を止めない
        print(f"[infra-log error] {e}")
    finally:
        try:
            cur.close()  # type: ignore[name-defined]
        except Exception:
            pass
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass
