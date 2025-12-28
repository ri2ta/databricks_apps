# Technology Stack

## Project Type
Databricks Apps 上で動作する Flask ベースのサーバー駆動 Web アプリ（HTMX + Tailwind + DaisyUI + Alpine.js）。モデル駆動 CRUD フレームワークを提供。

## Core Technologies

### Primary Language(s)
- **Language**: Python 3.x
- **Runtime/Compiler**: Databricks Apps ランタイム（Python）、ローカル開発は devcontainer の Python。
- **Language-specific tools**: pip、venv/conda（開発環境）。

### Key Dependencies/Libraries
- **Flask**: Web フレームワーク。
- **HTMX**: サーバー駆動の部分更新。
- **Tailwind CSS + DaisyUI**: スタイルとコンポーネント。
- **Alpine.js**: 軽量な UI 状態管理。
- **SQLAlchemy**: DB アクセス抽象化とコネクションプーリング（DBAPI 経由）。

### Application Architecture
- シングル Flask アプリ（モノリシック）で HTMX 部分テンプレートを返却。
- UI レイアウト・コンポーネントは `templates/` 配下に集約。
- データアクセスは `db.py` に集約し、サービス/リポジトリ層への拡張を想定。

### Data Storage (if applicable)
- **Primary storage**: RDB。Databricks SQL Warehouse または Lakebase（PostgreSQL 経由）を想定。
- **Caching**: なし（必要に応じてアプリ内メモリキャッシュを検討）。
- **Data formats**: HTML (HTMX partials), JSON (API/内部処理), YAML (エンティティ定義)。

### External Integrations (if applicable)
- Databricks SQL / Unity Catalog を利用したテーブルアクセスを想定（SQLAlchemy から接続）。
- 認証/認可は Databricks Apps の枠組み＋将来の追加ミドルで拡張を検討。

### Monitoring & Dashboard Technologies (if applicable)
- サーバーログ出力（Flask logging + db.py ログ統合）。
- Apps 側でのメトリクス集約（将来: レイテンシ/エラー率の可視化ダッシュボード）。

### Development Environment

### Build & Development Tools
- **Build System**: なし（Flask 実行のみ）。
- **Package Management**: pip (`requirements.txt`)。
- **Development workflow**: devcontainer でのローカル実行、ホットリロードは Flask の debug モードで代替（必要なら watch ツールを追加）。

### Code Quality Tools
- **Static Analysis**: ruff（推奨）、mypy（将来）。
- **Formatting**: ruff/black（未固定、今後選定）。
- **Testing Framework**: pytest（HTMX 応答の部分 HTML 検証を想定）。
- **Documentation**: Markdown（docs/ 配下）。

### Version Control & Collaboration
- **VCS**: Git（GitHub Flow を前提に運用）。
- **Branching Strategy**: main 基軸の小刻み PR。
- **Code Review Process**: PR ベース、ruff/pytest/mypy を推奨。

### Dashboard Development (if applicable)
- 現状なし。必要に応じて HTMX/Alpine で運用向け UI を追加。

## Deployment & Distribution (if applicable)
- **Target Platform(s)**: Databricks Apps（本番）。ローカルは devcontainer で再現。
- **Distribution Method**: Apps ワークスペース内デプロイ。
- **Installation Requirements**: Databricks ランタイム + Python、Secrets/SQL Warehouse への接続設定。
- **Update Mechanism**: GitHub → Apps への再デプロイ。将来 CI/CD を整備。

### Technical Requirements & Constraints

### Performance Requirements
- HTMX 応答 P95 < 500ms を目標（Apps 内計測）。
- 最小メモリフットプリントで複数同時接続を処理できること。

### Compatibility Requirements  
- **Platform Support**: Databricks Apps + ローカル devcontainer（Linux ベース）。
- **Dependency Versions**: Flask 2.x 系、HTMX/Tailwind/DaisyUI/Alpine は最新版安定版を利用。SQLAlchemy は 2.x 系を想定。
- **Standards Compliance**: HTTP/HTML ベース。セキュリティヘッダは Flask で追加予定。

### Security & Compliance
- Secrets は Databricks Secret Scope／環境変数経由で注入しハードコード禁止。
- 入力バリデーションとエラーハンドリングを共通化予定。
- ログの個人情報扱いに注意し、必要ならマスキングを行う。

### Scalability & Reliability
- 予想負荷: 少量〜中規模の業務 CRUD。Apps スケールに依存。
- Availability: Apps の稼働保証に依存。将来はリトライ/タイムアウトを明示設定。
- 成長に合わせてサービス/リポジトリ分割やキャッシュ導入を検討。

## Technical Decisions & Rationale
1. **HTMX + Flask（サーバー駆動）**: JS 薄めで保守容易。Databricks Apps と相性が良い。
2. **Tailwind + DaisyUI**: UI ルックを統一しつつカスタマイズ性を確保。
3. **YAML モデル定義**: 宣言的に UI を生成し、拡張ポイントを持たせるため。
4. **単一アプリ構成**: 初期スコープを小さくし、後でサービス分割できる余地を残す。
5. **SQLAlchemy 採用**: DB 接続先を SQL Warehouse / Lakehouse / PostgreSQL に柔軟に切替えつつ、プールや ORM/SQL 表現を統一するため。

## Known Limitations
- 認証/認可の詳細設計が未記載（Apps 標準を前提、将来拡張）。
- 観測性（メトリクス/トレース）実装はこれから。
- SQLAlchemy 導入後もスキーマ管理/マイグレーション（例: Alembic）が未整備。
