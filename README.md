# A3 Purification — dbt Data Pipeline Portfolio

> A production-grade dbt project for sports analytics, featuring a layered transformation architecture with 80+ custom SQL models, 35+ automated governance tests, and AI-assisted development workflow.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   L9: Validation Layer                      │
│              Accuracy reports, self-audit                   │
├─────────────────────────────────────────────────────────────┤
│                   L8: Reporting Layer                       │
│     API views, reconciliation, strategy signals             │
├─────────────────────────────────────────────────────────────┤
│                  L7: Evolution Layer                        │
│   Long-term baselines, causal uplift, synergy detection     │
├─────────────────────────────────────────────────────────────┤
│                  L6: Strategy Layer                         │
│           Portfolio optimization, risk management           │
├─────────────────────────────────────────────────────────────┤
│                L5: Consilience Layer                        │
│      Cross-source consensus, anomaly detection              │
├─────────────────────────────────────────────────────────────┤
│              L4: External / Future Layer                    │
│    Third-party opinions, trainer strategy acceleration      │
├─────────────────────────────────────────────────────────────┤
│                  L3: Bias Layer                             │
│          Track bias, day-of-week effects                    │
├─────────────────────────────────────────────────────────────┤
│                 L2: Context Layer                           │
│   Topological features, z-scores, physical conditions       │
├─────────────────────────────────────────────────────────────┤
│                  L1: Micro Layer                            │
│     Horse-level fixed/line/point features                   │
├─────────────────────────────────────────────────────────────┤
│               L0: Foundation Layer                          │
│            Staging models, source definitions                │
├─────────────────────────────────────────────────────────────┤
│                   Raw Sources                               │
│         PostgreSQL (JRDB, JRA-VAN, External)                │
└─────────────────────────────────────────────────────────────┘
```

## Project Stats

| Metric | Value |
|---|---|
| Custom SQL Models | 82 |
| Automated Tests | 35 |
| Governance Tests | 25 |
| Custom Macros | 2 |
| Transformation Layers | 10 (L0–L9) |
| Source Tables | 20+ |
| Total SQL Lines | 4,300+ |

## Key Design Decisions

### "Fat DB / Thin Python" Philosophy

All data transformations, feature engineering, and statistical calculations are performed in SQL via dbt — not in Python/Pandas. This ensures:

- **Reproducibility**: Every transformation is version-controlled SQL
- **Testability**: Every model can be validated with dbt tests
- **Performance**: PostgreSQL's query optimizer handles complex window functions efficiently
- **Auditability**: No hidden state in Python memory; everything is traceable in the DB

### Layered Architecture (L0–L9)

The 10-layer structure ensures strict separation of concerns:

- **L0 (Foundation)**: Raw source staging with type casting and validation
- **L1 (Micro)**: Individual entity features (fixed traits, time-series, point-in-time)
- **L2 (Context)**: Race-level context (topological features, z-scores, physical conditions)
- **L3 (Bias)**: Environmental biases (track, weather, day effects)
- **L4 (External)**: Third-party data integration with reliability scoring
- **L5 (Consilience)**: Cross-source consensus and anomaly detection
- **L6 (Strategy)**: Portfolio construction and risk optimization
- **L7 (Evolution)**: Long-term performance baselines and causal analysis
- **L8 (Reporting)**: API-ready views and reconciliation reports
- **L9 (Validation)**: Self-audit and accuracy measurement

### Automated Governance

25+ dbt tests function as continuous governance checks:

- **Data Quality**: Null rate monitoring, format validation, freshness checks
- **Feature Integrity**: Target variable leak prevention, column count gates
- **System Health**: Task queue monitoring, model count verification
- **Domain Logic**: Chaos detection validation, regime distribution checks

## Notable Models

### `int_topological_features` (L2)
Computes race-level statistical features using window functions — entropy-based chaos indicators, field-size-adjusted z-scores, and odds-derived information metrics.

### `v_inference_today_v316` (Mart)
The final inference view assembling 120+ features from all layers, with WINDOW-function-computed race statistics replacing what was previously calculated in Python.

### `calc_wakuban` (Macro)
A domain-specific Jinja macro implementing the official gate-number-to-bracket mapping rules for Japanese horse racing (8–18 horse fields).

## Governance Test Examples

```sql
-- Prevent target variable leakage into inference features
SELECT attname as leaked_column
FROM pg_attribute
WHERE attrelid = 'api.a3_features_today_base'::regclass
  AND attname IN ('actual_rank', 'is_winner', 'payout_win', ...)
```

```sql
-- Ensure chaos detection is never silently disabled
SELECT 'chaos_detection', COUNT(*)
FROM v_inference_today_v316
WHERE is_chaos = true
HAVING COUNT(*) < 1  -- Fails if zero chaos detected
```

## Tech Stack

- **dbt** (Data Build Tool) — Transformation orchestration
- **PostgreSQL** 15 — Primary data warehouse
- **Docker** / Docker Compose — Infrastructure
- **elementary-data** — Data observability
- **dbt-expectations** — Advanced test assertions
- **sqlfluff** — SQL linting

## Development Approach

This project was developed through **AI-assisted collaboration** — architectural design, quality governance, and technical decisions were made by the author, with AI agents (Claude, etc.) assisting in code generation and automation. Over 450 iterative improvement cycles were executed within approximately 6 months, each with automated integrity auditing.

## Setup

```bash
# 1. Install dbt
pip install dbt-postgres

# 2. Configure database connection
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit with your PostgreSQL credentials

# 3. Install packages
dbt deps

# 4. Run models
dbt run

# 5. Run tests
dbt test
```

## License

This project is shared as a portfolio demonstration. The transformation logic and test patterns are original work.
