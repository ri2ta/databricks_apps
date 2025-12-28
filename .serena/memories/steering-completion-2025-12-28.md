# Steering Docs Completion (2025-12-28)

- 完了物: product.md, tech.md, structure.md（すべて承認・cleanup 済み）。
- 承認 IDs: product=approval_1766897524830_ck8q3advu, tech (Lakebase fix)=approval_1766898070178_cfyqaniow, structure (services/repositories)=approval_1766898355874_uyq4gidjh。
- 技術方針: DB アクセスは SQLAlchemy。DB 想定は Databricks SQL Warehouse または Lakebase/PostgreSQL。
- 構造方針: approot/services と approot/repositories を明示。依存方向は UI → Service → Repository → DB。
- プロダクト方針: YAML ベースで CRUD UI 自動生成、HTMX サーバー駆動、Tailwind + DaisyUI、Alpine.js 最小使用。
