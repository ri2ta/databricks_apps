# Product Overview

## Product Purpose
Databricks Apps 上で、YAML によるエンティティ定義だけで CRUD UI を自動生成する「モデル駆動アプリ」基盤を提供し、業務 CRUD 開発の手間を最小化する。

## Target Users
- データ／分析チームの内製業務アプリ開発者（Python/SQL が主）
- ビジネス部門の市民開発者（簡易な設定変更で UI を作りたい）
- 運用担当（ログ・監査が見える形で安全に運用したい）

## Key Features
1. **YAML 主導のエンティティ定義**: `config/entities.yaml` を書くだけで一覧・フォームを生成。
2. **htmx ベースのサーバー駆動 UI**: 部分更新で高速な UX を提供。
3. **汎用データグリッド／フォーム部品**: Tailwind + DaisyUI で統一されたコンポーネントを再利用。
4. **カスタムアクション差し込み**: YAML で宣言したエンドポイントを Flask 側に実装し、拡張性を確保。
5. **Databricks ネイティブ統合**: データ基盤・バッチ・モデル提供との連携を前提に設計。

## Business Objectives
- CRUD アプリの初期提供リードタイムを大幅短縮（テンプレ化）。
- UI/UX の統一により保守コストを削減、重複実装を防止。
- Databricks 環境での運用・監査要件（ログ／権限／Secret 管理）を満たす。
- 業務ロジック実装に開発リソースを集中させ、設定で UI を作る文化を醸成。

## Success Metrics
- 新エンティティ追加から CRUD 画面提供までのリードタイム: 1 日以内。
- フレームワーク上で提供されるエンティティ数: 月次で増加。
- UI 層の改修頻度（重複実装の減少）: 従来比で 50% 減。
- 主要画面の P50/P95 レイテンシ: htmx リクエストで P95 < 500ms（Apps 内計測）。

## Product Principles
1. **Declarative-first**: UI は可能な限り YAML 定義で完結させ、ロジックは Flask へ分離。
2. **Server-driven UI**: htmx による部分 HTML 配信を基本とし、フロント状態は最小化。
3. **Extensible hooks**: カスタムアクション／フィールド型を追加しやすい設計を維持。
4. **Secure-by-default**: Secrets/権限を外出しし、入力バリデーションと監査ログを標準化。
5. **Observable**: ログ・メトリクスをデフォルトで出力し、Apps 内で可視化可能に。

## Monitoring & Visibility (if applicable)
- **Dashboard Type**: Web UI（Apps 内 HTMX ページ）
- **Real-time Updates**: htmx 部分更新（必要に応じてポーリング）
- **Key Metrics Displayed**: CRUD レイテンシ、エラーレート、エンティティ別利用件数
- **Sharing Capabilities**: ワークスペース内共有（将来は read-only リンク/エクスポートを検討）

## Future Vision
YAML 定義でスキーマだけでなく、ワークフロー・権限モデル・通知まで declarative に拡張し、Apps 上で完結するモデル駆動業務アプリ基盤へ進化させる。

### Potential Enhancements
- **Remote Access**: 外部ステークホルダー向けの安全な閲覧リンク配布（トンネル／リバースプロキシ前提）。
- **Analytics**: 利用状況ダッシュボード、履歴トレンド、パフォーマンス分析の自動化。
- **Collaboration**: コメント、承認フロー、マルチユーザー編集を段階的に追加。
