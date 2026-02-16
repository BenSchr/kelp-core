### Catalog Compatibility Matrix

| Feature                                  | Managed Table | View | Materialized View | Streaming Table |
|-----------------------------------------|-------------|----|-----------------|---------------|
| Description                              | ✅            | ✅   | ✅                | ⚠️ (not supported in Unity Catalog) |
| Tags                                     | ✅            | ✅   | ✅                | ✅             |
| Properties                               | ✅            | ✅   | ✅ (Use SDP)      | ✅ (Use SDP)   |
| ClusterBy (not implemented yet)          | —             | —    | —                 | —               |
| Column Masking (not implemented yet)     | —             | —    | —                 | —               |
| Row Access Policies (not implemented yet)| —             | —    | —                 | —               |
| Constraints (not implemented yet)        | —             | —    | —                 | —               |
