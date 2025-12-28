# GPT-5.1-Codex カスタム指示書 (databricks_apps)

本プロジェクトは Databricks Apps 上で動作する Python ベースの Web アプリです。技術スタックは Flask + HTMX + Tailwind CSS + DaisyUI + Alpine.js。MCP ツールとして Serena と spec-workflow を使い、Spec 駆動・テスト駆動で進めます。

## 目的と原則
- ビジネス要件を Spec 化してから実装する。
- 可能な限り TDD で実装し、短いフィードバックループを維持する。
- Databricks 環境依存の箇所を明示し、ローカルと本番の差異を最小化する。
- セキュリティ(認証/認可・秘密管理)と可観測性(ログ/メトリクス)を常に考慮する。
- Serena を常時活用し、リポジトリ調査・既存資産の再利用・重複防止を徹底する。

## ワークフロー (Spec → 実装 → テスト)
1) spec-workflow を必ず起動
   - 仕様作業を始めるときは `spec-workflow_spec-workflow-guide` を呼び、Requirements → Design → Tasks → Implementation の流れを確認。
   - Serena でリポジトリ/記憶を検索し、関連実装やログを事前に把握する。
   - セッション開始時に Serena の記憶を確認し、当日の作業方針を同期する。作業中に得た知見や決定はセッション内で必ずメモリへ追記する。
2) 仕様策定
   - Requirements: ビジネスゴール・制約・非機能を明文化。
   - Design: 画面フロー、HTMX インタラクション、API I/O、DB スキーマを簡潔に記述。
   - Tasks: 実装タスクを分解し、タスク ID を付与。必要に応じて approvals をリクエスト。
3) 実装
   - Serena ツール群でリポジトリを調査。既存コードを再利用し、重複実装を避ける。
   - TDD: 可能なところはテストを先行/同時追加。Flask なら `pytest` + `flask.testing.FlaskClient` を基本に。
   - UI: HTMX のエンドポイントはレスポンスを最小限にし、部分テンプレートを返す。DaisyUI コンポーネントを積極利用し、Alpine.js は局所状態に限定。

   ## TDD 手順 (基本ルール)
   1) タスク特定: spec-workflow の Tasks から対象を選び、受け入れ条件を明確化。テスト名に taskId を含める。
   2) 失敗するテストを書く: pytest で happy/edge/invalid を先に書く。Flask クライアントの fixture を使い、HTMX 応答は主要要素を検証。
   3) 最小実装: テストが通る最小コードを追加。データアクセスは `db.py` に集約し、副作用を局所化。
   4) リファクタ: 重複除去、命名整理、DaisyUI コンポーネント/クラスの統一。必要ならテンプレートを partials/components に抽出。
   5) 検証: `pytest` を実行し、必要に応じて `ruff`/`mypy` で静的チェック。
   6) ログ: `mcp_spec-workflow_log-implementation` で taskId ごとにアーティファクトを記録。
4) 実装ログ
   - 作業完了ごとに `mcp_spec-workflow_log-implementation` で API/関数/コンポーネント/統合のアーティファクトを記録。
5) テストと検証
   - ローカル: `pytest`、`ruff`/`flake8`、`mypy` などを走らせる。
   - Databricks: ジョブ/Apps 実行時の設定差分 (環境変数、Secret Scope、Workspace Files) を検証。

## プロンプト運用ガイド
- モデル名は「GPT-5.1-Codex-Max」と明言。
- すべての応答は日本語で行う。
- ファイル参照は workspace-relative パスと行リンク形式で返す。
- 非 ASCII を含む既存ファイル以外では ASCII を基本とする。
- 大きな変更前に計画を共有し、必要なら TODO を管理。

### 典型的なプロンプト例
- 仕様開始: 「spec-workflow の Requirements を開始。対象機能は◯◯。制約は…」
- 実装タスク分解: 「この仕様を Tasks に落とし込み、タスク ID を付けて」
- コード変更: 「[approot/app.py](/workspaces/databricks_apps/approot/app.py) にエンドポイント追加。HTMX で◯◯を返す部分テンプレートを [approot/templates/partials](/workspaces/databricks_apps/approot/templates/partials) に」
- テスト要求: 「エンドポイント `/items/<id>` の pytest を追加。成功/404/validation を網羅」
- ログ記録: 「log-implementation で taskId=… のアーティファクトをまとめて」

## コーディング指針
- Flask
  - Blueprint で責務分割。`request.args`/`form` のバリデーションを必ず行う。
  - HTMX エンドポイントは部分 HTML を返し、`hx-trigger`/`hx-target` をテンプレート側に記述。
- テンプレート
  - `base.html`/`layout.html` にレイアウトを集約。partials/components を活用。
   - Tailwind はユーティリティクラス中心。共通色/スペーシングはカスタムクラス or `@apply` にまとめる。
   - DaisyUI コンポーネントを優先的に利用し、独自スタイルは最低限に抑える。
   - Alpine は小さなインタラクションのみ。状態は data 属性で局所化し、巨大なロジックは Flask 側へ。
- データアクセス
  - DB アクセスは `db.py` に集約し、副作用を局所化。トランザクション境界を意識。
- ロギングとエラーハンドリング
  - Flask の errorhandler で共通処理。API/HTMX は JSON/部分 HTML で整形したエラーを返す。

## Databricks Apps 特有の注意
- Secrets/環境変数を使い、ハードコード禁止。Workspace Files への書き込みは最小限。
- 非同期ジョブが絡む場合は Webhooks/Notifications を検討し、UI には進捗ポーリング用 HTMX エンドポイントを用意。
- スケール: 同時接続・メモリ制約を意識し、レスポンスを軽量化。

## テストポリシー
- 単体: ビュー関数は happy/edge/invalid を網羅。HTMX レスポンスは部分テンプレートのキー要素を検証。
- 結合: 重要なフローは軽量な統合テストで確認。
- Lint/型: ruff/flake8 + mypy を推奨。

## 変更時の出力フォーマット
- 行リンク付きで参照 (例: [approot/app.py](/workspaces/databricks_apps/approot/app.py#L10-L30))。
- 要約は簡潔に、影響範囲と次アクションを提示。不要な長文は避ける。
