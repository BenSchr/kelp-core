# Unity Catalog Grants

Kelp can synchronize direct Unity Catalog privileges for tables, views,
materialized views, streaming tables, metric views, and functions. Catalog and
schema grants are managed separately and are not included in this feature.

## Configure grants

Each entry names a principal and its privileges:

```yaml
kelp_project:
  remote_catalog_config:
    grant_mode: append # append (default) or managed
  models:
    +grants:
      - principal: ${ engineers_group }
        privileges: [MODIFY]
    gold:
      +grants:
        - principal: analysts
          privileges: [SELECT]

kelp_models:
  - name: gold_orders
    grants:
      - principal: analysts
        privileges: [APPLY TAG]
```

Grants from the project root, each matching folder, and the object are extended
and combined by principal. Privileges are unioned, normalized to uppercase, and
deduplicated. For example, the model above gives `analysts` both `SELECT` and
`APPLY TAG`. Principal values may use Kelp's normal Jinja variable syntax.

An entry with `privileges: []` or `privileges: null` marks that principal for
revocation if the principal has no non-empty privilege entry anywhere else in
the hierarchy. It cannot remove a privilege granted by another level. An
empty `grants` list or `grants: null` adds nothing; it does not clear inherited
grants. A missing `privileges` field is invalid.

## Grant modes

- **`append` (default):** add configured privileges and preserve direct grants
  for unconfigured principals. A principal with a revoke marker has all its
  direct privileges revoked.
- **`managed`:** make the object's direct grants match the configured
  principals and privileges. Direct grants for unconfigured principals are
  revoked. Use with care: this can remove access granted directly outside
  Kelp, including a grant needed by the identity performing the sync.

Kelp compares only grants assigned directly to the object. Effective grants
inherited from a catalog or schema are ignored. A folder-level revoke marker
applies to each object under that folder unless another hierarchy level grants
that principal a non-empty privilege list.

`ALL PRIVILEGES` is treated as a single privilege token. When Kelp needs to
revoke it, the remote system also removes the principal's other direct
privileges, so Kelp revokes all and re-grants every desired privilege for that
principal.

## Synchronize grants

`sync_tables()`, `sync_metric_views()`, and `sync_functions()` include grant
statements when an object's resolved `grants` is non-empty. Functions are
synced before a pipeline; table-like objects should be synced after the
pipeline creates them. Review generated statements before executing them,
especially in `managed` mode.

The `generate-ddl` command remains focused on CREATE DDL. Use the catalog
sync functions to generate grant statements after the target object exists.

Grant statements for table-like objects use the object's bare fully qualified
name. Function statements add the `FUNCTION` keyword:

```sql
GRANT SELECT ON catalog.schema.orders TO `analysts`;
GRANT EXECUTE ON FUNCTION catalog.schema.function_name TO `analysts`;
```

The current remote grant reader uses the SDK's raw Unity Catalog permissions
response or Spark `SHOW GRANTS`, depending on the selected remote-fetch engine.
It reads direct grants only; `SHOW GRANTS` rows inherited from parent
securables are filtered out.
