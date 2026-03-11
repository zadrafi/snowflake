# Security

## Threat Model

This POC processes documents that may contain sensitive business data (invoices, contracts, receipts). The following controls are in place:

| Threat | Mitigation |
|--------|-----------|
| Unauthorized data access | Dedicated `AI_EXTRACT_APP` role with least-privilege grants; MANAGED ACCESS schema |
| Privilege escalation | Ownership transferred to SYSADMIN; account-level CREATE grants revoked post-setup |
| PUBLIC role leakage | All PUBLIC grants on database, schema, tables, views, and warehouse are revoked |
| Runaway compute cost | Resource monitor caps warehouse at 100 credits/month with SUSPEND at 100% |
| Data loss | 14-day `DATA_RETENTION_TIME_IN_DAYS` on all audit tables; append-only review trail |
| SQL injection | All queries use parameterized bindings (`?` placeholders); no string interpolation of user input |
| AI hallucination | Extraction output validated with `TRY_TO_*` casts; numeric bounds checked before save |
| Network exposure | BIND SERVICE ENDPOINT revoked after Streamlit deployment |

## RBAC Summary

```
ACCOUNTADMIN
  └── SYSADMIN (owns all POC objects)
        └── AI_EXTRACT_APP (operational role)
              ├── ALL on AI_EXTRACT_POC database + schema
              ├── ALL on tables, views, stages, procedures
              ├── USAGE + MONITOR + MODIFY on AI_EXTRACT_WH
              └── CREATE STREAMLIT, CREATE SERVICE (during setup only)
```

Users are granted `AI_EXTRACT_APP` to access the dashboard. The role cannot create databases, warehouses, or bind service endpoints.

## Data Handling

- **At rest**: Internal stage uses `SNOWFLAKE_SSE` encryption. All tables inherit Snowflake's default encryption.
- **In transit**: All connections use TLS. Container runtime uses HTTPS endpoints.
- **Audit trail**: Every review correction is an INSERT (never UPDATE/DELETE). Full traceability via `reviewed_by` (CURRENT_USER) and `reviewed_at` (CURRENT_TIMESTAMP) columns.
- **Retention**: Key tables retain 14 days of Time Travel for point-in-time recovery.

## Secrets

- No secrets are stored in code or committed to the repository.
- `.streamlit/secrets.toml` is gitignored.
- CI/CD uses Snowflake PAT tokens stored as repository secrets.
- Connection credentials are managed via `~/.snowflake/connections.toml` (local) or environment variables (CI).

## Reporting Vulnerabilities

This is a POC/demo kit. If you discover a security issue, contact the repository owner directly.
