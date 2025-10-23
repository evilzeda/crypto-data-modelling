# Crypto Data Modeling

## Project Overview
This repository contains the data modeling project for CoinX, a cryptocurrency exchange in Southeast Asia.

### Objectives
- Analyze trading concentration risk
- Evaluate user retention & cross-product adoption
- Ensure data quality and compliance

---

## Repository Structure
- `models/`
  - `staging/`: Clean raw sources (`stg_raw_users`, `stg_raw_trades`, `stg_raw_tokens`, `stg_raw_p2p`)
  - `core/`: Trusted integration tables (`core_trades`, `core_p2p`)
  - `marts/`: Analytics-ready tables for user retention, token concentration, cross-product funnel, and data quality
- `.github/workflows/`: GitHub Actions workflow for scheduled dbt runs
- `snapshots/`: Historical snapshots
- `macros/`: Reusable SQL macros
- `README.md`: Project documentation

---

## Setup Instructions
1. Clone the repository:
   ```bash
   git clone https://github.com/evilzeda/crypto-data-modelling.git
