# Directory Sync and Reconciliation Pipeline

A nightly pipeline that keeps an HR system and an identity directory in agreement. It copies only what changed from each source, checks the data, and produces a daily report of every account that doesn't match HR: leavers who still have access, new hires with no account, duplicates, orphaned accounts, and wrong names or departments.

**Stack:** Python · PostgreSQL · SQL · Docker · Kubernetes CronJob · GitHub Actions · Faker (synthetic data)

## Results

A 30-day simulation with **6,000 employees** (all synthetic: fake names, example.com emails). Each day HR records about 40–55 changes, and the directory sometimes fails to follow; after each nightly run, IT fixes 30% of the reported issues.

![Accuracy and open issues per day](docs/accuracy.png)

| | Result |
| --- | --- |
| Directory accuracy | 99.58%–99.93% every day (mean 99.68%) |
| Changes captured | Every HR change synced on the day it happened (exact match all 30 days); every directory change too, including the previous evening's IT fixes |
| Issues found per day | 8–34, across all six mismatch types |
| Issues fixed from the report | 242 over 30 days |

Daily numbers are in [docs/daily.csv](docs/daily.csv). Accuracy = share of active employees with exactly one enabled account whose name and department match HR.

## Architecture

```mermaid
flowchart LR
    HR[(HR system<br/>hr.employees)] -->|changed rows since watermark| S1[Incremental pull<br/>watermark + 5 min overlap<br/>row hash]
    DIR[(Identity directory<br/>directory.accounts)] -->|changed rows since watermark| S2[Incremental pull]
    S1 --> C[(sync.hr_employees)]
    S2 --> D[(sync.dir_accounts)]
    C --> Q{Quality checks}
    D --> Q
    Q -->|error| X[Stop the run<br/>exit code 1]
    Q -->|pass| R[Reconcile<br/>match by employee ID, else email]
    R --> I[(reconciliation_issues<br/>IT worklist)]
    R --> M[(reconciliation_summary<br/>accuracy + counts)]
    K[Kubernetes CronJob<br/>nightly] -.runs.-> S1
```

## What it finds

| Issue | Meaning | Why it matters |
| --- | --- | --- |
| `leaver_still_active` | Terminated employee with an enabled account | Security risk: an ex-employee can still sign in |
| `missing_account` | Active employee with no enabled account | New hire can't work |
| `orphan_account` | Enabled account matching no employee | Unowned access, often contractors |
| `duplicate_account` | Active employee with more than one enabled account | Extra attack surface and licence cost |
| `name_mismatch` | Display name differs from HR | Wrong person shown in email and directories |
| `department_mismatch` | Department differs from HR | Wrong access groups and reporting lines |

## Data quality checks

Run on the synced copies before reconciling. A failing **error** check stops the run with exit code 1, so the scheduler shows it as failed and IT never gets a worklist built from bad data.

| Check | Severity |
| --- | --- |
| Active employee without an email | error |
| Terminated employee without a termination date | error |
| Two active employees with the same email | error |
| Enabled account without a username (UPN) | error |
| Synced row counts equal source row counts (completeness) | error |
| Hire date in the future | warn |
| Enabled account with no employee ID | warn |

## Run it

```bash
pip install -r requirements-dev.txt
docker compose up -d     # local Postgres
export DIRSYNC_DSN=postgresql://postgres:postgres@localhost:5432/dirsync

# The 30-day simulation (resets the hr, directory and sync schemas)
PYTHONPATH=src python -m dirsync.demo --employees 6000 --days 30 --out reports

# One nightly run against real source tables
PYTHONPATH=src python -m dirsync.run --run-date 2026-11-07
```

On Kubernetes (kind or minikube):

```bash
docker build -t dirsync:latest .
kind load docker-image dirsync:latest
cp k8s/secret.example.yaml k8s/secret.yaml     # put your DSN in it; it's git-ignored
kubectl apply -f k8s/secret.yaml -f k8s/cronjob.yaml
kubectl create job --from=cronjob/dirsync-nightly dirsync-manual
```

**Tests** run against a real Postgres: `DIRSYNC_TEST_DSN=postgresql://postgres:postgres@localhost:5432/postgres pytest`. They cover every mismatch type on hand-built data, watermark and overlap behaviour, a failing check stopping the run, and a 10-day simulation where every change must be counted exactly. CI runs them with a Postgres service, then builds the Docker image.

## Design decisions

- **Watermark with an overlap, plus a row hash.** Reading only rows with `updated_at` after the last load keeps runs fast. A 5-minute overlap catches rows committed slightly late with older timestamps; the hash means those re-read rows are recognized as unchanged instead of being rewritten or double-counted.
- **Match by employee ID, then by email.** Accounts created by hand often lack the employee ID, so email is the fallback key.
- **Duplicates keep the oldest account.** The newest extra account is the one reported, since the original is usually the one in use.
- **Checks are plain SQL, stored with results per run.** Each check is one query returning a count of bad rows, saved to `sync.check_results`, so the history of data quality is queryable. A framework such as Great Expectations could replace this as the number of checks grows.
- **Kubernetes CronJob with `concurrencyPolicy: Forbid`.** Two overlapping runs could both move the watermark; forbidding overlap removes that risk. Failed runs are retried twice, then left visible as failed.
- **Secrets stay out of the repo.** The connection string comes from a Kubernetes secret; only an example file is committed.

## Limitations

- Deletes in a source aren't captured; the design assumes HR keeps terminated employees and the directory disables rather than deletes accounts. Capturing hard deletes would need a periodic full-key comparison.
- A row committed more than 5 minutes late with an old timestamp would be missed until it changes again; a periodic full reconciliation of keys would close that gap.
- All data is synthetic. Against a real directory (for example Microsoft Entra ID through Microsoft Graph), the `directory.accounts` table would be filled by an extract step instead of the simulator.
