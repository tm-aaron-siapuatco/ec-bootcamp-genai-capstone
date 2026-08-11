# L1 Test Verification

L1 questions ask for a single fact that's stated **explicitly** in the source data — no aggregation, no cross-record reasoning, no computed/derived values. If the chatbot's answer doesn't match what's written verbatim in the PDF or the CSV, that's a fail.

**Out of scope for L1** (these are computed by the gold Dagster layer, not stated directly in any source file, so verifying them means checking pipeline business logic, not retrieval — that's L2 territory): `value_tier`, the tenure tier bucket, and derived `age`. This doc only covers fields that exist verbatim in `core_customers.csv`, `crm_contacts.csv`, or the product PDFs.

## Running this locally

```bash
cp .env.sample .env    # fill in Azure OpenAI credentials + DB config, if not already done
docker compose up -d
```

The data needs to actually be ingested before any of these questions can pass — a fresh stack has an empty `knowledge_base` collection and empty Postgres tables. Materialize once (this is the same command `ci.yml` runs automatically after every deploy, so you only need this manually for a fresh local stack):

```bash
docker compose exec dagster-webserver dagster asset materialize --select '*' -m pipelines.definitions
```

Then open the frontend at **http://localhost:8501**.

If you're testing against the deployed Azure VM instead of local, the frontend isn't exposed externally — SSH-tunnel it first:
```bash
ssh -L 8501:localhost:8501 azureuser@<vm-ip>
```
then open http://localhost:8501 the same way.

## How to use this doc

1. In the frontend, toggle the data source indicated in each section (**Bank Offers (ChromaDB)** for Section A, **Customer Data (Postgres)** for Section B — not both at once, so you're testing one retrieval path at a time).
2. Ask the question (verbatim or close to it).
3. Compare the answer against **Expected**.
4. Fill in **Result** (✅ Pass / ❌ Fail) and **Notes** (paste the actual answer if it failed, or anything odd) as you go. Add your name/date if more than one person is running these.

---

## Section A — ChromaDB / Product Documents

Toggle: **Bank Offers (ChromaDB)** only. Source: `pipelines/data/capstone_part_2/*.pdf`.

### A.1 Astra Travel Miles Platinum

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| ATM-01 | What's the age eligibility range for the Astra Travel Miles Platinum card? | 21–70 |✅ | |
| ATM-02 | What's the minimum monthly income requirement for the Astra Travel Miles Platinum? | ₱35,000, OR Average Daily Balance (ADB) ≥ ₱50,000 across CASA |✅ | |
| ATM-03 | What's the annual fee for the primary cardholder on Astra Travel Miles Platinum? | ₱4,500 |✅ | |
| ATM-04 | What's the annual fee for a supplementary Astra Travel Miles Platinum card? | ₱2,250 |✅ | |
| ATM-05 | What's the foreign transaction service fee on Astra Travel Miles Platinum? | 2% |✅ | |
| ATM-06 | What's the credit limit range for Astra Travel Miles Platinum? | ₱80,000–₱800,000 |✅ | |
| ATM-07 | What miles multiplier do 25–45 year olds earn on airline/hotel spend with this card? | 1.5x |✅ | |
| ATM-08 | How many lounge passes per year does the 25–45 age band get? | 4 |✅ | |
| ATM-09 | What's the travel insurance coverage amount for the 25–45 age band? | Up to ₱5,000,000 |✅ | |
| ATM-10 | What perks does the 18–24 age band get on Astra Travel Miles Platinum? | 1.0x miles on spend, 1 lounge pass/yr, student/first-job travel kit |✅ | |
| ATM-11 | What perks does the 46–59 age band get? | 1.25x miles, 2 lounge passes/yr, medical add-on coverage, concierge upgrades |✅ | |
| ATM-12 | What perks does the 60+ age band get? | 1.25x miles, 2 lounge passes/yr, senior priority hotline, trip disruption assistance |✅ | |
| ATM-13 | At what ADB does the Prestige Hexagon tier waive the annual fee, and what else comes with it? | ADB ≥ ₱150,000 — annual fee waived, +2 lounge passes, minimum spend requirement removed |✅ | |
| ATM-14 | What's the Priority Hexagon tier condition for this card? | ADB ≥ ₱100,000 → annual fee waived; otherwise 50% off if ADB ≥ ₱75,000 |✅ | |
| ATM-15 | What's the Privilege Hexagon tier condition? | ADB ≥ ₱75,000 → annual fee 50% off; upgrades to full waiver if annual spend ≥ ₱300,000 |✅ | |
| ATM-16 | What's the Lite Hexagon tier condition? | ADB ≥ ₱50,000 → annual fee 25% off; upgrades to 50% off if annual spend ≥ ₱200,000 |✅ | |
| ATM-17 | Who is the Astra Travel Miles Platinum card best suited for, per the recommendation notes? | Customers aged 25–45 who show high travel/entertainment spend |✅ | |

### A.2 Build+ Student & Starter

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| BSS-01 | What's the age eligibility range for the Build+ Student & Starter card? | 18–35 |✅ | |
| BSS-02 | What do 18–20 year olds need to be eligible for Build+ Student & Starter? | School ID + parent/guardian co-maker, unless Hexagon tier ≥ Priority |✅ | |
| BSS-03 | What do 21–35 year olds need to be eligible? | Employment certificate OR ADB ≥ ₱15,000 |✅ | |
| BSS-04 | What's the annual fee range for Build+ Student & Starter? | ₱0–₱1,200, depending on tier + ADB |✅ | |
| BSS-05 | What's the late payment fee, and is there a waiver? | ₱800; a first waiver is available for students |✅ | |
| BSS-06 | What's the credit limit range for this card? | ₱8,000–₱120,000 |✅ | |
| BSS-07 | What perks does the 18–20 age band get? | Tuition/installment support, 2% cashback on campus needs, 1 free missed-payment waiver |✅ | |
| BSS-08 | What perks does the 21–29 age band get? | 3% cashback on transport/food, 1 bill-payment rebate/mo, credit-limit review after 6 months |✅ | |
| BSS-09 | What perks does the 30–35 age band get? | 3% cashback on utilities/mobile, upgrade path to Everyday Max if on-time for 12 months |✅ | |
| BSS-10 | Can a 36-year-old get the Build+ Student & Starter card? | No — not eligible; recommended to use Everyday Max or the Miles card instead |✅ | |
| BSS-11 | What's the Prestige Hexagon tier condition for  Build+ Student & Starter? | ADB ≥ ₱60,000 → annual fee waived, auto-upgrade review after 6 months |✅ | |
| BSS-12 | What's the Priority Hexagon tier condition? | ADB ≥ ₱40,000 → annual fee waived; co-maker not required for 18–20 |✅ | |
| BSS-13 | What's the Privilege Hexagon tier condition? | ADB ≥ ₱25,000 → annual fee 50% off; starter limit +20% |✅ | |
| BSS-14 | What's the Lite Hexagon tier condition? | ADB ≥ ₱15,000 → annual fee ₱0 for the first year; must maintain ADB to keep the waiver |✅ | |
| BSS-15 | What's the auto-upgrade rule to Everyday Max? | On-time payments for 12 months AND ADB at/above the tier threshold |✅ | |

### A.3 Everyday Max Cashback

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| EMC-01 | What's the age eligibility range for Everyday Max Cashback? | 18–75 |✅ | |
| EMC-02 | What's the minimum monthly income requirement? | ₱20,000, OR ADB ≥ ₱30,000 |✅ | |
| EMC-03 | What other eligibility condition is preferred besides income/ADB? | No current delinquency; at least 6 months of relationship preferred |✅ | |
| EMC-04 | What's the annual fee for Everyday Max Cashback? | ₱2,500 (tiered discounts/waivers via Hexagon tier + ADB) |✅ | |
| EMC-05 | What's the overlimit fee? | ₱750 |✅ | |
| EMC-06 | What's the credit limit range? | ₱30,000–₱500,000 |✅ | |
| EMC-07 | What cashback rate do 18–24 year olds get on transport/food? | Up to 5%, plus 1 streaming rebate/mo capped at ₱150 |✅ | |
| EMC-08 | What cashback rate do 25–39 year olds get on supermarkets/fuel? | Up to 6%, 3% on utilities, quarterly bill rebate capped at ₱300 |✅ | |
| EMC-09 | What perks does the 40–59 age band get? | 6% on groceries/health, 3% on utilities, family mobile plan rebate |✅ | |
| EMC-10 | What perks does the 60+ age band get? | 7% on pharmacies/clinics, 3% on groceries, priority dispute assistance |✅ | |
| EMC-11 | What's the Prestige Hexagon tier condition? | ADB ≥ ₱120,000 → annual fee waived + 1% cashback booster on two chosen categories |✅ | |
| EMC-12 | What's the Priority Hexagon tier condition? | ADB ≥ ₱80,000 → annual fee waived; otherwise 50% off if ADB ≥ ₱50,000 |✅ | |
| EMC-13 | What's the Privilege Hexagon tier condition? | ADB ≥ ₱50,000 → annual fee 50% off; base rates apply |✅ | |
| EMC-14 | What's the Lite Hexagon tier condition? | ADB ≥ ₱30,000 → annual fee 25% off; base rates apply |✅ | |
| EMC-15 | Who is this card recommended for, per the notes? | Customers with high monthly grocery/utility spend or dependents |✅ | |

---

## Section B — Postgres / Customer Data

Toggle: **Customer Data (Postgres)** only. Source: `gold_customers` (merged from `core_customers.csv` + `crm_contacts.csv` by email). Retrieval matches on an **email**, a **`CUST-######`** ID, or a **first + last name** pair found in the question — see `backend/postgres_rag.py`.

Reference records (pulled directly from the raw CSVs, rows 2–5):

| Field | Sofia Garcia | Miguel Ramos | Andrea Aquino | Sofia Castillo |
|---|---|---|---|---|
| `cust_id` | CUST-000001 | CUST-000002 | CUST-000003 | CUST-000004 |
| `email` | sofia.garcia1@mail.com | miguel.ramos2@bankmail.ph | andrea.aquino3@inbox.ph | sofia.castillo4@mail.com |
| `phone_e164` | +639276804025 | +639086875935 | +639760652429 | +639225258329 |
| `city` | Makati | Iloilo City | Bacolod | Bacolod |
| `province` | Iloilo | Davao del Sur | Cebu | Negros Occidental |
| `segment` | Mass Affluent | Mass | Mass | Affluent |
| `hexagon_tier` | Priority | Lite | Lite | Lite |
| `kyc_level` | Basic | Basic | Simplified | Basic |
| `status` | Dormant | Active | Active | Active |
| `total_relationship_balance` | 2,113,649.38 | 2,267,598.10 | 4,381,961.76 | 2,646,042.61 |
| `average_daily_balance` | 311,245.38 | 775,379.43 | 701,715.34 | 1,031,632.30 |
| `marketing_opt_in` | True | False | True | True |

### B.1 Lookup by email

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| PG-EMAIL-01 | What's the total relationship balance for sofia.garcia1@mail.com? | ₱2,113,649.38 |✅ | |
| PG-EMAIL-02 | What city and province is miguel.ramos2@bankmail.ph in? | Iloilo City, Davao del Sur |✅ | |
| PG-EMAIL-03 | Is andrea.aquino3@inbox.ph opted into marketing? | Yes (True) |✅ | |
| PG-EMAIL-04 | What's the KYC level and status for sofia.castillo4@mail.com? | KYC level: Basic; Status: Active |✅ | |

### B.2 Lookup by customer ID

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| PG-ID-01 | What's the average daily balance for CUST-000001? | ₱311,245.38 |✅ | |
| PG-ID-02 | What Hexagon tier is CUST-000002? | Lite |✅ | |
| PG-ID-03 | What segment is CUST-000003 in? | Mass |✅ | |
| PG-ID-04 | What's the phone number for CUST-000004? | +639225258329 |✅ | |

### B.3 Lookup by name

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| PG-NAME-01 | What's Miguel Ramos's account status? | Active |✅ | |
| PG-NAME-02 | What's Andrea Aquino's total relationship balance? | ₱4,381,961.76 |✅ | |
| PG-NAME-03 | Is Sofia Garcia's account active or dormant? | Dormant |✅ | |

### B.4 Edge case — ambiguous name

There are two customers named Sofia in this sample set (Garcia and Castillo). This isn't a bug to fix — it's worth confirming the system's actual behavior is at least sane (returns one clearly-labeled match, multiple matches, or asks for disambiguation) rather than silently returning the wrong person's data.

| ID | Question | Expected Answer | Result | Notes |
|---|---|---|---|---|
| PG-EDGE-01 | What's Sofia's total relationship balance? | Ambiguous by design (two Sofias). Record which customer(s) the system actually returns — that's the thing to sanity-check here, not a fixed expected value. |✅ |Sofia Bautista: 1,673,912.09, Sofia C. Castillo: 2,646,042.61, Sofia C. Santos: 4,493,211.00 |

---

## Summary

| Section | Total questions | Passed | Failed |
|---|---|---|---|
| A.1 Astra Travel Miles Platinum | 17 |17 | |
| A.2 Build+ Student & Starter | 15 | 15 | |
| A.3 Everyday Max Cashback | 15 |15 | |
| B.1–B.3 Postgres lookups | 11 |11 | |
| B.4 Edge case | 1 | 1  | |
| **Total** | **59** |**59** | |