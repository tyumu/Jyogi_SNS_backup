# Jyogi SNS Backup / Cleanup Docs

このドキュメントは、このリポジトリを触る共同開発者向けのまとめです。

- 何をしているか
- どのスクリプトが何を担当しているか
- どうやってローカル / ラズパイで動かすか
- 削除が走る条件と安全装置

をざっくり把握できるように書いています。

---

## 全体像

### バックアップと削除のフロー

1. 毎晩 `main.py` を実行（cron など）
2. `main.py` がやること
   1. 3か月以上前の `todos` / `replies` を PostgreSQL から `backup.db` に追記
   2. `backup.db` を元に、画像を月別 ZIP (`images_backup_YYYYMM.zip`) に追記
   3. `backup.db` と ZIP 群のハッシュ・件数・"ZIP 済み max(id)" を `artifacts/backup_manifest.json` に出力
   4. `scripts/restore_smoke_test.py` でバックアップ整合性チェック → OK なら `artifacts/restore_smoke_test.ok` を作成
   5. `scripts/generate_delete_plan.py` で「3か月前より古い投稿」の削除計画 (`artifacts/delete_plan.json`) を生成
   6. `scripts/delete_with_guard.py` でガード付き削除（`.env` のフラグ次第で DRY-RUN or 本削除）

※ 実際の削除は `.env` の `VERIFY_ONLY` / `DELETE_ENABLED` によって制御されます（デフォルトは DRY-RUN）。

---

## ディレクトリ構成（ざっくり）

- [main.py](main.py)
  - 夜間バッチのエントリポイント（cron から呼ぶスクリプト）
  - 実行開始/成功/失敗を Supabase の `infrastructure_logs` テーブルに記録する（後述の infra ログ）

- [backup/](backup)
  - [db_backup.py](backup/db_backup.py)
    - `backup_old_rows_to_sqlite()`
    - 3か月以上前の `todos` / `replies` を Supabase(PostgreSQL) から [artifacts/backup/backup.db](artifacts/backup/backup.db) に追記（`INSERT OR IGNORE`）
  - [image_backup.py](backup/image_backup.py)
    - `download_and_zip_images_by_month(sqlite_path)`
    - `backup.db` の `todos` から `image_url` / `created_at` を見て、R2 から画像を取得し、[artifacts/images/](artifacts/images) 配下の `images_backup_YYYYMM.zip` に月別で追記
    - マニフェストの `image_max_todo_id_zipped` を見て、前回までに ZIP 済みの `id` より大きい行だけを対象にする（インクリメンタル）
  - [manifest.py](backup/manifest.py)
    - `create_manifest(db_path, zip_paths, max_todo_id_zipped)`
    - `backup.db` と ZIP 群について
      - 件数 (`todos` / `replies` / `image_urls`)
      - 各ファイルの SHA256 ハッシュ
      - ZIP ファイルの絶対パス一覧 (`zip_files`)
      - 画像として ZIP 済みとみなす最大 `todos.id` (`image_max_todo_id_zipped`)
    - を [artifacts/backup_manifest.json](artifacts/backup_manifest.json) に出力

- [scripts/](scripts)
  - [restore_smoke_test.py](scripts/restore_smoke_test.py)
    - `artifacts/backup_manifest.json` と [artifacts/backup/backup.db](artifacts/backup/backup.db) / [artifacts/images/](artifacts/images) 配下の `images_backup_YYYYMM.zip` 群を使って、
      - DB ハッシュ・件数
      - 各 ZIP のハッシュ・画像枚数
      - サンプル画像の整合性
    - をチェックし、全て OK なら [artifacts/restore_smoke_test.ok](artifacts/restore_smoke_test.ok) を作成
  - [generate_delete_plan.py](scripts/generate_delete_plan.py)
    - PostgreSQL から「3か月以上前の `todos`」を取得し、その id に紐づく `replies` と `image_url` を集めて
      [artifacts/delete_plan.json](artifacts/delete_plan.json) を生成
  - [delete_with_guard.py](scripts/delete_with_guard.py)
    - [artifacts/restore_smoke_test.ok](artifacts/restore_smoke_test.ok) が無ければ即中断
    - `DELETE_PLAN_PATH` の `delete_plan.json` を読み込み、
      - まず DB (`replies` → `todos` の順) を削除
      - 次に R2 の画像オブジェクトを削除
    - `.env` の `VERIFY_ONLY` / `DELETE_ENABLED` で挙動を制御（後述）
  - [run_main_nightly.sh](scripts/run_main_nightly.sh)
    - プロジェクトルートに移動して `venv` か `python3` で `main.py` を実行（ログは [artifacts/logs/main_nightly.log](artifacts/logs/main_nightly.log)）
  - [install_nightly_cron.sh](scripts/install_nightly_cron.sh)
    - 上記シェルをラズパイの `crontab` に登録するヘルパースクリプト（毎晩 02:00 実行）

- [infra_logging.py](infra_logging.py)
  - Supabase の `public.infrastructure_logs` テーブルに、
    - `nightly_backup_start` / `nightly_backup_success` / `nightly_backup_failed` などのイベントを INSERT する薄いロガー
  - ログ保存に失敗してもバックアップ処理自体は止めない設計

---

## 必要な環境変数 (.env)

サンプルは [.env.example](.env.example) を参照。主なものだけ抜粋します。

### DB 接続

- `POSTGRES_URL`
  - Supabase の接続文字列 (PostgreSQL URI)
  - 例: `postgresql://backup_readonly:YOUR_PASSWORD@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres?sslmode=require`

### R2 (画像ストレージ)

- `R2_TEMP_BUCKET_NAME`
- `R2_TEMP_ENDPOINT`
- `R2_TEMP_ACCESS_KEY_ID`
- `R2_TEMP_SECRET_ACCESS_KEY`

### バックアップ関連

- `BACKUP_DB_PATH` (任意, 既定: `artifacts/backup/backup.db`)
- `BACKUP_THRESHOLD_DAYS` (任意, 既定: `90`)
  - 何日前より前を「古い投稿」とみなすか
- `BACKUP_IMAGES_DIR` (任意, 既定: `artifacts/images`)
- `BACKUP_MANIFEST_PATH` (任意, 既定: `artifacts/backup_manifest.json`)

### インフラログ関連（任意）

- `INFRA_LOG_DB_URL` (任意)
  - インフラログを書き込むための接続文字列。
  - 未設定の場合は `POSTGRES_URL` をそのまま使う。
- `INFRA_ENVIRONMENT` (任意, 既定: `prod`)
  - `infrastructure_logs.environment` に入る環境名。
  - 例: `prod` / `staging` / `dev` など。

### 削除ガード

- `VERIFY_ONLY` (既定: `true`)
  - `true` の場合: **常に DRY-RUN**（削除 SQL は実行するが最後に `ROLLBACK` する）
- `DELETE_ENABLED` (既定: `false`)
  - `true` かつ `VERIFY_ONLY=false` のときだけ、本番削除が有効になる
- `RESTORE_OK_MARKER` (既定: `artifacts/restore_smoke_test.ok`)
  - このファイルが存在しないと `delete_with_guard.py` はエラーにして止まる
- `DELETE_PLAN_PATH` (既定: `artifacts/delete_plan.json`)
  - 削除計画 JSON のパス

---

## ローカルでの動かし方

### 1. 仮想環境と依存パッケージ

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\\Scripts\\activate
pip install -r requirements.txt
```

### 2. .env を用意

- [.env.example](.env.example) をコピーして `.env` を作成し、実環境に合わせて値を埋める

```bash
cp .env.example .env
# 中身を編集して POSTGRES_URL / R2_TEMP_* などを実値にする
```

### 3. 手動で一通りのフローを確認

```bash
# バックアップ + 復元テスト + （デフォルトでは DRY-RUN 削除）
python main.py

# バックアップの整合性だけ確認したい場合
python scripts/restore_smoke_test.py

# 削除計画だけ生成したい場合
python scripts/generate_delete_plan.py

# 削除だけ試したい場合（.env の VERIFY_ONLY/DELETE_ENABLED に注意）
python scripts/delete_with_guard.py
```

---

## ラズパイでの夜間自動実行
注意：大量のデータを読み書きするので外付けSSD(usb)を付けないとラズパイのメインのsdカードが数か月かそこらで死にます
### 1. リポジトリを配置

ラズパイにこのリポジトリを配置し、仮想環境と `.env` をローカルと同様に用意します。

### 2. cron に登録

```bash
chmod +x scripts/run_main_nightly.sh scripts/install_nightly_cron.sh
./scripts/install_nightly_cron.sh
# 確認
crontab -l
```

- 既定では毎晩 02:00 に `scripts/run_main_nightly.sh` → `python main.py` が実行されます。
  - `INFRA_LOG_DB_URL` / `INFRA_ENVIRONMENT` を設定しておくと、この実行ごとに Supabase の `infrastructure_logs` に 1 レコードずつ蓄積されます。

  例: `source='raspberry_pi'`, `event_type='nightly_backup_success'` / `'nightly_backup_failed'` など。

---

## 削除フローと安全装置

### 削除が実際に走る条件

1. `.env` で:
   - `VERIFY_ONLY=false`
   - `DELETE_ENABLED=true`
2. [scripts/restore_smoke_test.py](scripts/restore_smoke_test.py) が成功し、
   - [artifacts/restore_smoke_test.ok](artifacts/restore_smoke_test.ok) が存在する
3. [scripts/generate_delete_plan.py](scripts/generate_delete_plan.py) が正常に動き、
   - [artifacts/delete_plan.json](artifacts/delete_plan.json) がある

この状態で `python main.py` または `python scripts/delete_with_guard.py` を実行すると、本番削除が行われます。

### 失敗時の挙動

- DB削除中にエラーが起きた場合
  - トランザクションを `ROLLBACK` してから例外を再送出するので、「一部だけ削れた中途半端な状態」は残りません。
- R2 画像削除で一部失敗した場合
  - エラーになったキーは `image_delete_errors` として [artifacts/delete_result.json](artifacts/delete_result.json) に書き出されます（自動再試行まではしていません）。
- `main.py` 実行中にどこかで例外が出た場合
  - `[ERROR] Nightly backup failed.` とスタックトレースを出しつつ `return 1` するので、cron などから「失敗検知」できます。

---

## 開発者向けメモ

- `test/` フォルダはローカル検証用スクリプト群であり、本番デプロイには不要です。
- 新しいバックアップ方式（インクリメンタル画像ZIP + max(id) 管理）は
  - `backup/image_backup.py` と `backup/manifest.py` に閉じているので、将来ここだけ差し替えるのも比較的容易です。
- 削除条件やしきい値を変更したい場合は、
  - 日数: `BACKUP_THRESHOLD_DAYS`
  - 対象の `SELECT` ロジック: [backup/db_backup.py](backup/db_backup.py) と [scripts/generate_delete_plan.py](scripts/generate_delete_plan.py)
  を見ると理解しやすいです。

---

## 将来: SNS本体リポジトリ側でやること案

ざっくり案:

1. backup.db を読む専用 API / サービス
  - ローカルの`backup.db` を読み込み専用でマウントする（File API使用）。
  - この DB には「3か月以上前で、本番DBから削除済みの履歴」だけが入っている前提。

2. 「アーカイブ専用」検索＆閲覧ページ
  - 通常のタイムラインとは別に、メニューから遷移できる「アーカイブ閲覧ページ」を用意する。
  - ここでは `backup.db` だけを検索対象にし、
    - ユーザーID / 日付 / キーワード などで `todos` / `replies` を検索
    - 結果をテキストのみで一覧表示

3. 画像の扱い: 最初は「ZIP をアップロードしてもらう」方式
  - アーカイブ閲覧ページでは、最初は画像の代わりに「この月の画像ZIPをアップロードしてください」というボタンを表示する。
  - 例: 2025年09月の投稿を閲覧する際は、`images_backup_202509.zip` をローカルから選択してアップロードさせる UI。
  - フロー案:
    1. ユーザーがブラウザ上で `images_backup_YYYYMM.zip` を選択
    2. フロントエンド側で ZIP を解凍し、Blob URL (`URL.createObjectURL`) に変換
    3. `backup.db` 内の `image_url` のファイル名と ZIP 内のファイル名を突き合わせ、該当する Blob URL を `<img src=...>` として表示
  - サーバー側に ZIP を永続保存しない（フロント側だけで展開して表示）ことで、R2 や別ストレージへの再アップロードを避けつつ、必要なときだけユーザー側から持ち込んでもらう構成にできる。

4. 設計上のポイント
  - 「バックアップ分はバックアップ専用ページでしか見ない」ルールにしておくと、本番DB側のロジックを汚さずに済む。
  - `backup.db` / `images_backup_YYYYMM.zip` はあくまで「外部アーカイブ」として扱い、アプリ本体 DB とは疎結合に保つ。
  - 画像周りは、将来必要になった時点で「サーバー側に再アップロードしてキャッシュする」等の拡張も可能だが、最初は「ローカル ZIP をブラウザに読み込ませるだけ」の構成が安全でシンプル。

---

## 将来：このバックアップリポジトリ自体の発展形

- 運用まわりの強化
  - `main.py` の実行結果を HTML レポートや Slack / Discord 通知として飛ばす。
  - `artifacts/` 以下のログやマニフェストを一覧できる「運用レポート生成スクリプト」を追加する。
  - R2 ダウンロード/削除失敗・Supabase 接続エラーなども、`log_infra(source='r2' | 'supabase', log_level='error', ...)` で Supabase 側に集約し、GUI から追えるようにする。

- 失敗時リカバリの自動化
  - [artifacts/delete_result.json](artifacts/delete_result.json) に残った R2 削除エラーを次回の `delete_plan.json` に自動で取り込むリトライ機構。
  - バックアップ失敗時に自動でリトライする簡易ジョブランナー（`max_retries` / `backoff` 付き）を用意する。

- ログの整備と可視化
  - `infrastructure_logs` の `run_id` 単位で「1回の nightly がどこでコケたか」をダッシュボード表示する（例えば Supabase のダッシュボードや将来の管理画面から）。
  - `source` / `log_level` / `event_type` によるフィルタ（例: `source='r2' AND log_level='error'`）で、特定インフラのエラー傾向をすぐ追えるようにする。
  - 将来、アプリ本体リポジトリ側にも簡易ビューを用意して、「最近 N 回分のバックアップ状況」を一覧できるようにする。

- セキュリティ / プライバシー
  - `backup.db` や `images_backup_YYYYMM.zip` を暗号化して保存するオプション（例: OpenSSL / age / GPG など）。
  - 暗号鍵の管理を `.env` ではなく、将来的には別のシークレットストアに移せるように抽象化する。

- パフォーマンス / スケール
  - 画像ダウンロードや ZIP 生成を並列化（スレッド / プロセス / asyncio）し、大量データでも時間内に収まるようにする。
  - 進捗率や推定残り時間をログに出すことで、夜間バッチの「終わりそう感」を可視化する。

- ツール化 / ライブラリ化
  - `backup/` 以下をパッケージ化し、`pip install` して他プロジェクトからも使えるようにする（例: `jyogi-backup`）。
  - `python -m jyogi_backup backup-once` のような CLI エントリポイントを用意し、サブコマンド（`backup-db` / `backup-images` / `verify` / `cleanup` など）を切る。

- テストと CI
  - SQLite / ローカル R2 モックを使った自動テストを追加し、GitHub Actions などで `pytest` を回す。
  - 将来的なリファクタや依存ライブラリ更新時にも、バックアップの整合性が壊れていないことを自動で検証できるようにする。