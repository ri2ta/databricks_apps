# セッションメモ (2025-12-28)
- ステアリング: product/tech/structure 承認済み。方針は YAML 主導 CRUD、Flask+HTMX、Tailwind+DaisyUI、レイヤーは UI→Service→Repository→DB、DB は Databricks SQL/Lakebase、db.py プール活用。
- Requirements (model-driven-crud-framework): 日本語化済み、承認・削除済み。要件4本（YAML CRUD自動生成、HTMXグリッド、汎用フォーム+カスタムアクション、Lookup）＋非機能（パフォーマンス<500ms、セキュリティ/信頼性/UX）。
- Design: 単一テンプレートで view/create/edit を mode 切替。汎用ルート（list/detail/form/save/actions/lookup）、Entities Loader→Generic Service→Generic Repo→db.py→DB、Templates は datagrid/form/lookup/entity/list。承認・削除済み。
- Tasks: TDD 強調で 6 タスク定義（ローダー、リポジトリ、サービス、テンプレート、Flask ルート、追加テスト強化）。最新承認待ち ID: approval_1766899668699_v6zaaphbo（未確認）。
- 次のアクション: タスク承認ステータスを確認→Implementation フェーズ開始。TDDで各タスク着手。
- リマインダ: セッションごとにメモリ記録を忘れずに行う。カスタム指示書に従い日本語応答、ファイル参照は行リンク形式。