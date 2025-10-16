# xbrlstream-mvp (1,2,3)
To automate the preparation of Malaysia SSM XBRL filings (specifically for Financial Statements under MFRS using SSMxT_2022v1.0). Reverse-engineer the end-to-end workflow that a human preparer would follow using the SSM MBRS mTool, then map it to an Agentic AI-driven pipeline that ingests a PDF financial statement, extracts relevant data, maps it to taxonomy concepts, validates against business rules, and outputs a compliant XBRL instance document.

#MVP (1) 17-August-2025 - 1 month
## Step-by-step Technical Blueprint v0.1
The documentation below is based on the real-world example provided by our partner Omesti:

Company: MIHCM Asia Sdn Bhd
Filing Type: FS-MFRS
Reporting Period: 1 April 2023 – 31 March 2024
Taxonomy: SSMxT_2022v1.0
Software: MBRS Preparation Tool v2.2

### 1. Understand the Regulatory & Taxonomic Framework

Key Sources:
SSMxT_2022 Architecture Document (PDF)
SSM Companies Act 2016
Malaysian Financial Reporting Standards (MFRS) – aligned with IFRS
SSMxT_2022v1.0 = IFRS Taxonomy 2022 + SSM-specific extensions

Core Principles:
No company extensions allowed (SSMxT_2022 Architecture Doc, p.12). All disclosures must use pre-defined concepts.
Use textBlock elements for narrative disclosures not covered by structured items.
Two main entry points for FS:
ssmt-fs-mfrs_YYYY-MM-DD_entry_point.xsd → for MFRS filers
ssmt-fs-mpers_... → for MPERS (not used here)

Namespaces Used (from XML):
``` xml
xmlns:ifrs-full="https://xbrl.ifrs.org/taxonomy/2022-03-24/ifrs-full"
xmlns:ssmt-dei="http://xbrl.ssm.com.my/taxonomy/2022-12-31/ssmt-dei-core"
xmlns:ssmt="http://xbrl.ssm.com.my/taxonomy/2022-12-31/ssmt-cor"
xmlns:ssmt-mfrs="http://xbrl.ssm.com.my/taxonomy/2022-12-31/ssmt-mfrs-cor"
```


# FINAL STEPS: VALIDATION & SUBMISSION
1. Click Generate XBRL in the Toolbox.
2. Review Errors (red) - MUST fix. (and self-healing by AI Agent first attempt and then HITL)
3. Review Warnings (orange) - optional to fix.
4. Click "Save XBRL" zipfile.
5. Upload .zip file to SSM mPortal.

# 🧭 SUMMARY CHECKLIST FOR ANY COMPANY
## ✅ Step 1: Filing Information

Company name, reg no, origin, status, type
Audit status, currency, FYE
MFRS/MPERS selected

## ✅ Step 2: Financial Statements

Balance Sheet: Cash, assets, liabilities, equity
P&L: Revenue, expenses, profit/loss
If no data, enter 0 or leave blank (if allowed)

## ✅ Step 3: Notes

Corporate info: directors, auditors, employees
Accounting policies: copy from PDF
Related parties: state “none” if applicable
Directors’ Report: copy from PDF
Auditor’s Report: copy from PDF

## ✅ Step 4: Validation

Fix all red errors
Ignore orange warnings if correct
Save XBRL .zip

## *** LLM EVAL to be added ***

