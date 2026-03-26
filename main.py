import os
import sys

from dotenv import load_dotenv

from backup.db_backup import backup_old_rows_to_sqlite, BACKUP_DB_PATH, DELETION_THRESHOLD_DAYS
from backup.image_backup import download_and_zip_images_by_month
from backup.manifest import create_manifest
from infra_logging import log_infra
from scripts import restore_smoke_test as restore_smoke_test_mod
from scripts import generate_delete_plan as generate_delete_plan_mod
from scripts import delete_with_guard as delete_with_guard_mod


def main():
    """夜間バッチ: 3か月以上前の投稿だけバックアップ & 画像を月別ZIP化"""

    # .env 読み込み（DB/R2 設定）
    load_dotenv()

    try:
        print("=== Nightly backup start ===")
        log_infra(
            source="raspberry_pi",
            log_level="info",
            event_type="nightly_backup_start",
            message="Nightly backup started.",
        )

        # 1) PostgreSQL -> SQLite (3か月以上前のみ追記)
        backup_old_rows_to_sqlite()

        # 2) R2 画像を月別 ZIP に分割してバックアップ（インクリメンタル）
        zip_paths, max_todo_id_zipped = download_and_zip_images_by_month(BACKUP_DB_PATH)

        # 3) マニフェスト生成（絶対パス & max(id) 記録）
        manifest_path = create_manifest(BACKUP_DB_PATH, zip_paths, max_todo_id_zipped)

        # 4) 復元スモークテスト（backup.db と ZIP 群の整合性チェック）
        restore_smoke_test_mod.verify_backup()

        # 5) 削除計画生成（3か月以上前の todos/replies を delete_plan.json に出力）
        generate_delete_plan_mod.main()

        # 6) ガード付き削除
        #    VERIFY_ONLY / DELETE_ENABLED / RESTORE_OK_MARKER / DELETE_PLAN_PATH で挙動制御
        delete_with_guard_mod.main()

        print("\n=== Nightly backup done ===")
        print(f"  backup.db          : {os.path.abspath(BACKUP_DB_PATH)}")
        print(f"  zip files (monthly):")
        for zp in zip_paths:
            print(f"    - {os.path.abspath(zp)}")
        print(f"  manifest           : {os.path.abspath(manifest_path)}")
        print(f"  threshold_days     : {DELETION_THRESHOLD_DAYS}")

        log_infra(
            source="raspberry_pi",
            log_level="info",
            event_type="nightly_backup_success",
            message="Nightly backup completed successfully.",
            detail={
                "threshold_days": DELETION_THRESHOLD_DAYS,
                "zip_file_count": len(zip_paths),
            },
        )
        return 0
    except Exception as e:
        print("[ERROR] Nightly backup failed.")
        print(e)
        log_infra(
            source="raspberry_pi",
            log_level="error",
            event_type="nightly_backup_failed",
            message=str(e),
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())