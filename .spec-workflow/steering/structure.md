# Project Structure

## Directory Organization

```
project-root/
├── approot/                  # Flask アプリ本体
│   ├── app.py                # Flask エントリポイント
│   ├── db.py                 # DB アクセスレイヤ（SQLAlchemy 予定）
│   ├── services/             # 業務ロジック層（サービス）
│   ├── repositories/         # データアクセス層（リポジトリ）
│   ├── config/
│   │   └── entities.yaml     # モデル駆動エンティティ定義
│   ├── templates/            # Jinja2 テンプレート
│   │   ├── base.html         # ベースレイアウト
│   │   ├── layout.html       # 画面枠レイアウト
│   │   ├── components/       # 共通 UI コンポーネント
│   │   │   ├── header.html
│   │   │   └── nav.html
│   │   └── partials/         # HTMX で返す部分テンプレート
│   │       ├── list.html
│   │       └── detail.html
│   ├── requirements.txt      # アプリ依存
│   └── .env / .databrickscfg # 環境・接続設定
├── docs/
│   └── draft/                # 検討レポート等
└── .spec-workflow/           # spec/steering ドキュメント
    ├── steering/             # 本ドキュメント群
    ├── templates/            # テンプレート
    ├── specs/                # 仕様書
    └── approvals/            # 承認記録
```

## Naming Conventions

### Files
- Flask/Python: `snake_case.py`
- Templates: `kebab-case.html`（必要に応じてディレクトリで役割分離）
- Config: `snake_case.yaml`
- Tests（将来追加）: `test_*.py`

### Code
- Functions/Variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`

## Import Patterns
- 標準ライブラリ → サードパーティ → ローカルモジュールの順にグループ化。
- ルート配下からの相対インポート（`approot` 内は相対/絶対いずれも可だが一貫性を優先）。
- DB アクセスは `db.py` に集約し、将来は SQLAlchemy セッションをサービス層へ DI する前提。

## Code Structure Patterns
- ファイル内順序: Imports → 定数/設定 → 関数/クラス定義 → 実装 → エントリポイント。
- Flask ルートは `app.py`（将来 Blueprint 分割可）。DB 呼び出しは `db.py` 経由。
- HTMX エンドポイントは部分テンプレートを返し、テンプレート側で `hx-target`/`hx-trigger` を指定。

## Code Organization Principles
1. **Single Responsibility**: 各ファイルは明確な役割を一つ持つ。
2. **Modularity**: 共通 UI は components/、部分レスポンスは partials/ に分離。
3. **Testability**: DB 呼び出しを関数化し、pytest でモックしやすくする。
4. **Consistency**: Tailwind/DaisyUI クラスと命名規則を統一。

## Module Boundaries
- UI（templates）とロジック（Flask + db.py）を分離。
- サービス層は `approot/services/`、リポジトリ層は `approot/repositories/` に配置し、依存方向を UI → サービス → リポジトリ → DB とする。

## Code Size Guidelines
- 目安: 1 ファイル 300 行程度まで。関数は 30〜50 行を目安に分割。
- 深いネストは 3 レベル以内を推奨。

## Dashboard/Monitoring Structure (if applicable)
- 現状なし。将来追加する場合は `approot/monitoring/` など独立ディレクトリを作り、UI/ロジックと分離。

## Documentation Standards
- 新規モジュールには README や docstring を付与（特に DB アクセス・HTMX エンドポイント）。
- 複雑なロジックには最小限のコメントを追加。
