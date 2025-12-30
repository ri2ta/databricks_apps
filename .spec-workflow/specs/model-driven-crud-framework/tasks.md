# Tasks Document

- [x] 1. エンティティローダーとスキーマ検証を TDD で実装する
  - File: approot/services/entities_loader.py
  - 内容: 先に pytest を書き、config/entities.yaml を読み込みつつエンティティ定義を検証・正規化する関数を実装。必須フィールド欠落・型不整合を検出しエラーメッセージを返す。
  - _Leverage: config/entities.yaml, design.md の EntityConfig 定義, yaml ライブラリ_
  - _Requirements: 要件1, 非機能(セキュリティ/信頼性)_
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Python backend developer (validation重視) | Task: TDD で YAML ローダー/バリデータのテストを先に書き、EntityConfig に正規化して返す関数を実装 | Restrictions: 外部 I/O は entities.yaml のみ、エラーは例外でなく検証結果として返す関数を用意、テスト先行 | _Leverage: config/entities.yaml, design.md | _Requirements: 要件1, 非機能(セキュリティ/信頼性) | Success: 失敗するテスト→実装→テスト緑の流れで、必須キー欠落や不正型で明示的エラーを返し、有効な定義は dict で取得できる_

- [x] 2. 汎用リポジトリを TDD で実装する（list/detail/lookup）
  - File: approot/repositories/generic_repo.py
  - 内容: 先にリポジトリの単体テストを作成し、table/primary_key/columns に基づく list/detail/lookup を実装。ページング・ソートをサポートし、db.py のプールを利用。SQL バインドでインジェクションを防ぐ。
  - _Leverage: approot/db.py, design.md Generic Repository 節_
  - _Requirements: 要件1, 要件2, 要件4, 非機能(パフォーマンス)_
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Python data access developer | Task: TDD で list/detail/lookup のテストを先に書き、ページング/ソート/lookup を実装 | Restrictions: パラメータバインド必須、db.py を必ず経由、テスト先行 | _Leverage: db.py, design.md | _Requirements: 要件1, 要件2, 要件4 | Success: 失敗するテスト→実装→緑で、クエリパラメータに基づき正しい結果を返す_

- [x] 3. 汎用サービスを TDD で実装する（render_list/detail/form + actions）
  - File: approot/services/generic_service.py
  - 内容: 先にサービス層のテスト（list/detail/form のコンテキスト形状、未定義エンティティ/レコード時の応答、actions ディスパッチ）を書き、その後実装。mode="view/create/edit" で単一テンプレートを使う前提。
  - _Leverage: entities_loader.py, generic_repo.py, design.md Generic Service 節_
  - _Requirements: 要件1, 要件2, 要件3, 要件4_
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Python service layer developer | Task: TDD で render_list/detail/form と actions ディスパッチを実装 | Restrictions: mode で単一テンプレートを使う、存在しないエンティティ/レコード時はユーザーフレンドリーなメッセージ、テスト先行 | _Leverage: entities_loader, generic_repo, design.md | _Requirements: 要件1-4 | Success: 失敗テスト→実装→緑で、list/detail/form が正しく描画できる_

- [x] 4. テンプレート群を TDD で実装する（統一 entity テンプレート + コンポーネント）
  - Files: approot/templates/components/datagrid.html; approot/templates/components/form.html; approot/templates/components/field_types/*.html; approot/templates/components/lookup.html; approot/templates/partials/entity.html; approot/templates/partials/list.html
  - 内容: 先に HTMX レスポンスを検証するテスト（主要要素/モード切替/lookup）を書き、datagrid でページング・ソート、entity.html で mode(view/create/edit) 切替、form.html + field_types で入力、lookup モーダルを実装。
  - _Leverage: design.md Templates 節, 既存 templates/partials/list.html/detail.html の構造, draft レポートのサンプルマークアップ_
  - _Requirements: 要件1, 要件2, 要件3, 要件4, 非機能(ユーザビリティ/パフォーマンス)_
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Flask/Jinja + HTMX UI developer | Task: テスト先行で datagrid/form/lookup/統一 entity テンプレートを実装し、mode で表示/編集を切替える | Restrictions: DaisyUI/Tailwind を用い、HTMX ターゲット/スワップを正しく指定、冗長な JS を追加しない、テスト先行 | _Leverage: design.md, draft レポートの HTML サンプル | _Requirements: 要件1-4, 非機能(ユーザビリティ/パフォーマンス) | Success: 失敗テスト→実装→緑で、一覧/詳細/フォーム/lookup が共通テンプレートで表示され HTMX で部分更新が機能する_

- [x] 5. Flask ルートを TDD で汎用化して配線する
  - File: approot/app.py (既存を拡張)
  - 内容: 先に FlaskClient でルート疎通/HTMX レスポンスを検証するテストを書き、汎用ルート `/<entity>/list`, `/<entity>/detail/<id>`, `/<entity>/form[/<id>]`, `/<entity>/save`, `/<entity>/actions/<action>`, `lookup/<lookup_name>` をサービス経由で呼び出す。mode を渡して単一テンプレートを描画。
  - _Leverage: generic_service.py, design.md Flask Routes 節, 既存 app.py のルーティング_
  - _Requirements: 要件1-4, 非機能(信頼性/セキュリティ)_
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Flask developer | Task: テスト先行で汎用エンドポイントを追加し、HTMX 部分テンプレートを返す | Restrictions: 既存ルートを壊さない、例外はログして簡潔なメッセージを返す、mode を適切に渡す、テスト先行 | _Leverage: generic_service, design.md | _Requirements: 要件1-4 | Success: 失敗テスト→実装→緑で、各エンドポイントがサービスを呼び、共通テンプレートが適切なモードで描画される_

- [x] 6. テストスイートを強化する（ローダー/リポジトリ/サービス/HTMX）
  - Files: tests/test_entities_loader.py; tests/test_generic_repo.py; tests/test_generic_service.py; tests/test_htmx_endpoints.py (pytest, FlaskClient)
  - 内容: 既に各タスクで先行させたテストを集約・補完し、境界ケースとエラー経路を追加してカバレッジを高める。外部接続はモック/スタブ化し、主要 HTML 要素を検証。
  - _Leverage: pytest, Flask test client, design.md Testing Strategy 節_
  - _Requirements: 要件1-4, 非機能(信頼性/パフォーマンス)
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Python QA/pytest エンジニア | Task: 先行テストを補強し、境界ケースとエラー系を追加して回帰を防ぐ | Restrictions: 外部接続はモック/スタブ化、主要要素の HTML を assert、パフォーマンスは緩やかなしきい値で確認 | _Leverage: pytest, FlaskClient, design.md | _Requirements: 要件1-4, 非機能(信頼性/パフォーマンス) | Success: 追加テストがカバレッジを向上し、回帰を検知できる状態になる_

- [x] 7. 汎用保存パイプラインを実装する（TDD）
  - Files: approot/repositories/generic_repo.py; approot/services/generic_service.py; approot/app.py; templates/components/form.html (エラー表示拡張); 新規/既存テスト
  - 内容: 先に `POST /<entity>/save` の成功/バリデーションエラー/404/500 経路をテストし、repository.save で insert/update を実装。service.handle_save で入力検証・pk 反映を行い、成功時は detail 断片、失敗時はフォーム断片にフィールドエラーを埋め込む。SQL はパラメータバインドを徹底。
  - _Leverage: design.md Generic Repository/Service/Flask Routes/Testing Strategy, existing entity templates_
  - _Requirements: 要件5, 要件7, 非機能(セキュリティ/信頼性/ユーザビリティ)
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Python service+repo developer | Task: TDD で save エンドポイントのテストを先に書き、repository.save + service.handle_save + フォームエラー描画を実装 | Restrictions: パラメータバインド必須、フィールド別エラーを部分テンプレートで返す、既存エンドポイント互換を壊さない | _Leverage: design.md, templates/components/form.html | _Requirements: 要件5/7 | Success: save 成功で detail 200、バリデーション失敗で 400 エラー付きフォーム、未知エンティティ/レコードで 404 を返す_

- [x] 8. カスタムアクションディスパッチを実装する（TDD）
  - Files: approot/services/generic_service.py; approot/app.py; テンプレート（必要ならアクション結果用スニペット）; テスト
  - 内容: `POST /<entity>/actions/<action>` のテストを先に追加し、定義なし 404、未登録ハンドラ 501、ハンドラ成功 200、例外 500 を検証。handle_action でハンドラ登録を受け取り、結果を部分テンプレートで返す。
  - _Leverage: design.md Generic Service/Flask Routes/Testing Strategy_
  - _Requirements: 要件6, 非機能(セキュリティ/信頼性)
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Python service layer developer | Task: TDD で actions エンドポイントの正常系/異常系を網羅し、ハンドラ呼び出しとエラー応答を実装 | Restrictions: 未登録ハンドラは 501、例外は握って 500、エラーメッセージは簡潔 | _Leverage: design.md | _Requirements: 要件6 | Success: テストが通り、アクション定義/登録の有無に応じたステータスで部分レスポンスが返る_

- [-] 9. YAML バリデーションエラーの UI 通知を実装する
  - Files: approot/app.py (エンティティロード結果の扱い), templates/layout.html もしくは base/partials でバナー追加, tests/test_htmx_endpoints.py など
  - 内容: entities_loader の検証結果が失敗した場合、ログを残しつつレイアウトに簡潔なバナー/メッセージを表示する実装を追加。存在しないエンティティ要求時も 404/500 の部分テンプレートを返すことをテストで確認。
  - _Leverage: design.md YAML Error Handling, existing layout/partials_
  - _Requirements: 要件7, 非機能(ユーザビリティ/信頼性)
  - _Prompt: Implement the task for spec model-driven-crud-framework, first run spec-workflow-guide to get the workflow guide then implement the task: Role: Flask/Jinja developer | Task: YAML 構成エラーを UI で可視化するバナーを追加し、未定義エンティティ要求で部分テンプレートのエラーを返す | Restrictions: 詳細エラーはログのみ、UI は簡潔、既存スタイルに合わせる | _Leverage: design.md, layout.html | _Requirements: 要件7 | Success: バナーが表示され、エンティティ不正時の HTMX 応答が 404/500 でテストに合格する_

- [x] 10. 仕様更新（保存/アクション/YAML エラー UI の要件/設計追記）
  - Files: .spec-workflow/specs/model-driven-crud-framework/requirements.md; design.md; tasks.md
  - 内容: 保存とバリデーション応答、カスタムアクションディスパッチ、YAML エラーバナーの要件を日本語で明文化し、設計とタスクを拡張して実装フェーズの準備を完了。
  - _Leverage: 要件5-7, design.md 更新方針_
  - _Requirements: 要件5-7, 非機能(信頼性/ユーザビリティ/セキュリティ)
  - _Prompt: (記録用)_
