# Design Document

## Overview
YAML 定義 (`config/entities.yaml`) を基点に、一覧・詳細・フォーム UI を自動生成するモデル駆動 CRUD フレームワークを Databricks Apps 上で提供する。Flask + HTMX によりサーバー駆動で部分更新を行い、Tailwind + DaisyUI で一貫した見た目を保つ。Alpine.js はモーダル等の局所状態のみで利用する。DB 層は SQLAlchemy ベースの汎用実装へ置き換え、サービス公開インターフェースは変更しない。

## Steering Document Alignment

### Technical Standards (tech.md)
- サーバー駆動 UI (Flask + HTMX) を採用し、フロントの状態は最小化する。
- YAML 定義が唯一の UI ソース。DB 層は SQLAlchemy Core/ORM を採用し、サービス/テンプレートのインターフェースは維持する。
- Secrets/環境変数で接続設定を注入し、SQLAlchemy エンジン・接続プール設定を外部化。バリデーションとログ出力を標準化する。

### Project Structure (structure.md)
- 依存方向: UI → Service → Repository → DB。`approot/templates` に共通コンポーネント、`approot/services`/`approot/repositories` にロジックを配置。
- `db.py` は SQLAlchemy エンジン/Session 管理を提供し、リポジトリ層は Session/Connection を注入して利用する。

## Code Reuse Analysis
- 既存再利用: `approot/app.py` の Flask 初期化とルーティング枠組み、`approot/templates/partials/list.html`・`detail.html` のパターンを汎用化のたたき台として利用。
- 置き換え戦略: `approot/db.py` を SQLAlchemy エンジン/Session 生成に差し替え、`generic_repo` は既存シグネチャを維持したまま内部実装のみ SQLAlchemy に変更。
- 統合ポイント: `config/entities.yaml` を正規化して読み込むローダーを新設し、サービス層でエンティティ定義を引き回す。

### Existing Components to Leverage
- **db.py**: SQLAlchemy エンジン/Session ファクトリを提供し、リポジトリ層から注入利用。
- **app.py**: ルート定義のベース。汎用エンドポイント追加で拡張。
- **templates/partials/list.html, detail.html**: 既存の一覧・詳細テンプレート構造を汎用コンポーネントへ展開。

### Integration Points
- **Database (SQLAlchemy)**: エンジン/Session を環境変数ベースで初期化し、YAML の `table` を参照して任意テーブルをクエリ。パラメータバインドを徹底し、エンティティ定義のカラムのみ許可。
- **HTMX**: 一覧/詳細/フォーム/lookup の各部分テンプレートを hx-target で返却。

## Architecture
- YAML ローダーでエンティティ定義を読み込み、スキーマ検証後にサービス層へ提供。
- サービス層: 汎用 CRUD 処理 (list/detail/save) とカスタムアクションのディスパッチを担当。公開インターフェース/戻り値は現状維持。
- リポジトリ層: テーブル名とカラムを基にクエリを組み立て、SQLAlchemy Core ステートメントで実行。Session/Connection は `db.py` から注入し、必ずパラメータバインドする。
- UI: Jinja2 テンプレートで汎用グリッド・フォーム・lookup コンポーネントを組み立て、HTMX で部分更新。

```mermaid
flowchart LR
    YAML[config/entities.yaml] --> Loader[Entities Loader + Validation]
    Loader --> Service[Generic Entity Service]
    Service --> Repo[Generic Repository]
    Repo --> Engine[SQLAlchemy Engine/Session]
    Engine --> DB[(Databricks SQL)]
    Service --> Templates[HTMX Partials (grid/form/detail/lookup)]
    Templates --> Browser[HTMX + Alpine]
```

### Modular Design Principles
- Single Responsibility: ローダー/サービス/リポジトリ/テンプレートを分離。
- Component Isolation: グリッド・フォーム・フィールド・lookup を部分テンプレート化。
- Service Layer Separation: Flask ルートはサービス経由でリポジトリを呼ぶ。
- Utility Modularity: YAML 検証と正規化をユーティリティ関数に分割。

## Components and Interfaces

### Entities Loader (`approot/services/entities_loader.py` 想定)
- **Purpose:** YAML を読み込み、スキーマ検証し、辞書形式の定義を返す。
- **Interfaces:** `load_entities(path: str) -> dict[str, EntityConfig]`、`get_entity(name) -> EntityConfig`。
- **Dependencies:** `yaml`、スキーマ定義 (pydantic/手書き検証)。
- **Reuses:** `config/entities.yaml`。

### Generic Repository (`approot/repositories/generic_repo.py`)
- **Purpose:** テーブル名・カラム定義に基づく list/detail/lookup/save を SQLAlchemy Core/ORM で実行。
- **Interfaces:** `fetch_list(entity, page, page_size, sort)`、`fetch_detail(entity, pk)`、`search_lookup(entity, q, limit)`、`save(entity, payload)`（pk 有無で insert/update を分岐）。既存シグネチャと戻り値形状を維持。
- **Dependencies:** `db.py` が提供する Session/Connection。SQLAlchemy のパラメータバインドとカラムホワイトリストで安全性を確保。
- **Reuses:** 既存 `db.py` の呼び出しインターフェース名は維持しつつ中身を SQLAlchemy 化。

### Generic Service (`approot/services/generic_service.py`)
- **Purpose:** エンティティ定義を解釈し、テンプレート描画に必要なデータとコンテキストを組み立てる。保存とカスタムアクションのディスパッチも担う。
- **Interfaces:** `render_list(entity, query_params)`、`render_detail(entity, pk)`、`render_form(entity, pk|None)`、`handle_action(entity, action_name, form_data)`、`handle_save(entity, form_data)`（バリデーション + repository.save を実行し、成功/失敗コンテキストを返す）。
- **Dependencies:** Entities Loader、Generic Repository、テンプレートレンダリング。
- **Reuses:** shared templates。

### DB Layer (`approot/db.py`)
- **Purpose:** SQLAlchemy エンジンと Session/Connection ファクトリを提供し、既存の `init_pool`/`close_pool` 呼び出し口を残したまま内部を SQLAlchemy に置き換える。
- **Interfaces:** `init_pool()`（エンジン初期化）、`close_pool()`（dispose）、`get_session()` または `get_connection()` を提供し、`generic_repo` から注入利用する。アプリ起動時に一度初期化し、atexit で close。
- **Dependencies:** 環境変数/Secret Scope から DSN/接続オプションを取得。SQLAlchemy エンジン設定 (pool_size, max_overflow, pool_timeout)。
- **Reuses:** 既存ログ設定と atexit 登録の仕組み。

### Flask Routes (`approot/app.py` 拡張)
- **Purpose:** 汎用エンドポイントを束ね、HTMX 部分テンプレートを返す。
- **Interfaces:**
  - `GET /<entity>/list` (htmx partial)
  - `GET /<entity>/detail/<id>` (htmx partial, mode="view")
  - `GET /<entity>/form[/<id>]` (htmx partial, mode="create"/"edit")
  - `POST /<entity>/save` (htmx partial; service.handle_save でバリデーション・保存)
  - `GET /lookup/<lookup_name>` (lookup modal 内検索)
  - `POST /<entity>/actions/<action>` (カスタムアクション; 未登録時は 501)
- **Dependencies:** Generic Service。

### Templates
- **components/datagrid.html**: グリッド本体。ソート/ページングを hx-get で処理。
- **components/form.html + field_types/**: フィールドレンダリングを型ごとに分割し、`mode` に応じて表示/入力を切り替える（view: read-only, create/edit: 入力要素）。フィールド単位でエラー表示をサポートする。
- **components/lookup.html**: Lookup モーダルと検索結果描画。
- **partials/entity.html**: 単一テンプレートで view/create/edit を描画。呼び出し側が `mode` を渡し、詳細表示（view）とフォーム（create/edit）を同一テンプレートで切り替える。保存成功時は detail 断片を返し、失敗時はフォーム断片にエラーを埋め込む。
- **layout もしくは base テンプレート**: YAML バリデーションエラー用のバナー/スニペットを表示し、運用者に設定不備を知らせる。

## Data Models

### EntityConfig (YAML 正規化後)
- `name: str`
- `table: str`
- `label: str`
- `primary_key: str` (デフォルト `id` など)
- `list: ListConfig`
- `form: FormConfig`

### ListConfig
- `columns: list[ColumnConfig]` (name, label, width, sortable)
- `default_sort: str | None`
- `page_size: int`
- `actions: list[ActionConfig]`

### FormConfig
- `sections: list[FormSection]`
- `actions: list[ActionConfig]`

### FormSection
- `label: str`
- `fields: list[FieldConfig]`

### FieldConfig
- `name: str`
- `label: str`
- `type: str` (text/textarea/email/lookup 等)
- `lookup: str | None` (lookup 用)
- `rows: int | None`
- `display: str | None` (lookup 表示列)

### ActionConfig
- `name: str`
- `label: str`
- `endpoint: str` (Flask ルートまたはカスタム実装先)

## Error Handling

### YAML 検証エラー
- **Handling:** 読み込み時にバリデーションし、どのエンティティ/パスが不正かをログに出力。UI にはバナー/メッセージで設定エラーを通知し、詳細はログのみ。
- **User Impact:** 管理者向けエラー表示（簡潔）、詳細はログ。

### DB アクセスエラー
- **Handling:** SQLAlchemy OperationalError/Timeout を捕捉しログ、HTMX レスポンスで簡潔な失敗メッセージを返す。接続プール枯渇時はリトライせず 503/500 を返す。
- **User Impact:** 画面内に失敗通知を表示。再試行案内。

### カスタムアクション実行エラー
- **Handling:** サービスで例外を握り、部分テンプレートでエラーを返す。入力検証を徹底。未登録ハンドラは 501 を返す。
- **User Impact:** アクション結果領域にエラー表示。

### 保存エラー
- **Handling:** バリデーション失敗は 400 でフィールド別エラーを含む部分テンプレートを返す。例外はログして 500 の簡潔メッセージを返す。
- **User Impact:** ページ全体をリロードせず、フォーム上でエラーを確認できる。

## Testing Strategy

### Unit Testing
- エンティティローダーの YAML 検証・正規化テスト。
- リポジトリのクエリ組み立てロジック（ソート/ページング/lookup）を SQLAlchemy Core ステートメント単位で検証し、パラメータバインド漏れが無いことを確認。
- サービスのコンテキスト組み立て（list/form/detail のデータ形状）。
- `db.py` のエンジン初期化/Session 取得のユニットテスト（環境変数モックを用いて DSN/プール設定を確認）。

### Integration Testing
- Flask テストクライアントで HTMX エンドポイントを叩き、部分テンプレート内の主要要素（列ヘッダ、フィールド、ボタン生成、エラー表示）を検証。
- Lookup エンドポイントの検索結果と選択動作（hidden 値更新の HTML）を確認。
- `POST /<entity>/save` の成功/バリデーションエラー/404/500 経路を検証し、部分更新を確認。
- `POST /<entity>/actions/<action>` で定義なし/未登録ハンドラ/成功/例外を検証。
- SQLAlchemy 経由で CRUD/lookup が正しく動作することを、テスト用 SQLite/メモリ DB で結合テストする。

### End-to-End Testing
- 簡易なブラウザレス E2E（htmx リクエスト連鎖）で一覧→詳細→フォーム送信のフローを確認。
- 主要エンティティ（例: customer）で P95 応答 < 500ms を計測し、リグレッション検出のベースラインを作成。SQLAlchemy への置き換え後に同指標で回帰がないか確認。
