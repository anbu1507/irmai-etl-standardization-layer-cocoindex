# CLAUDE.md — irmai-etl-cloud-pipeline

## Project Overview
Azure Function App that standardizes CSV data from `raw-zone` blob storage and writes cleaned output to `staging-zone`.

## Structure
```
irmai-etl-cloud-pipeline/
├── src/
│   ├── function_app.py       # Azure Function (blob trigger on raw-zone/{name})
│   ├── host.json
│   ├── local.settings.json
│   └── requirements.txt
├── terraform/
│   ├── main.tf               # Creates Service Plan + Linux Function App
│   ├── variables.tf          # resource group, storage account, function app name
│   └── providers.tf          # azurerm ~> 3.0
└── .gitlab-ci.yml            # plan → deploy (terraform + func publish)
```

## Key Details
- **Storage account:** `irmaiuatstorage`
- **Resource group:** `rg-irmai-uat-us-1`
- **Function app name:** `irmai-standardization-engine-v1`
- **Trigger:** blob upload to `raw-zone/{name}`
- **Output:** cleaned CSV to `staging-zone/{name}`
- **Connection string env var:** `MyStorageConn`

## Standardization Logic (4 Mandatory Pillars)
All incoming CSVs are standardized to: `case_id`, `activity`, `timestamp`, `resource`

### Column Alias Mappings
| Pillar | Source Aliases |
|--------|---------------|
| `case_id` | case_id, policy_id, policy_no, policy_num, workflow_instance_id, parent_workflow_instance_id, source_workflow_instance_id |
| `activity` | activity, activity_name, event_action, billing_activity, activity_code, remarks |
| `timestamp` | event_timestamp, timestamp, created_at, tx_time |
| `resource` | resource, resource_type, resource_id |

### Transformations Applied
1. **Deduplication** — `drop_duplicates()`
2. **Identifier consolidation** — strip `^[A-Z]{2}_` prefix, join with `, `
3. **Activity consolidation** — join with ` | `
4. **Numeric fixes** — `risk_score` nulls filled with mean; `event_duration_seconds` negatives clamped to 0
5. **Pillar enforcement** — 4 pillars placed first, original alias columns removed

## Infrastructure (Terraform)
- References existing RG and storage account (does not create them)
- Consumption plan (Y1), Linux, Python 3.9
- System-assigned managed identity enabled
- `WEBSITE_RUN_FROM_PACKAGE=1` to avoid File Share 403 issues

## CI/CD (.gitlab-ci.yml)
- `terraform plan` → `terraform apply -auto-approve` → `func azure functionapp publish irmai-standardization-engine-v1 --python`
- All stages run on `main` branch only
