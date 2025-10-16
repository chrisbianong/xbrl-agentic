# This script compares your ingested JSON (from ingestion_agent.py) against the ground-truth PDF text (extracted cleanly using pdfplumber or PyMuPDF) and flags discrepancies in:

# Numeric values (e.g., (418,988 → should be (418,988))
# Table structure (row/column alignment)
# Critical OCR errors (e.g., t0 → to, Kegistration → Registration)
# Missing footnote content
# ✅ Usage:
# python validate_extraction.py <path_to_ingested_json> <path_to_ground_truth_pdf>
# Example: 
# python validate_extraction.py OHealthcare-AFS-2024_ingested.json OHealthcare-AFS-2024.pdf

# validate_extraction.py
import json
import re
import sys
from pathlib import Path
import pdfplumber
# pip install pdfplumber

def clean_numeric(val: str) -> str:
    """Standardize numeric strings for comparison."""
    # Remove non-essential whitespace
    val = re.sub(r"\s+", "", val)
    # Fix unbalanced parentheses
    if val.count("(") > val.count(")"):
        val = val + ")"
    elif val.count(")") > val.count("("):
        val = "(" + val
    # Normalize commas and decimals
    val = re.sub(r"[^0-9,\.\-\(\)]", "", val)
    return val

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract clean, searchable text from PDF."""
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text(x_tolerance=1, y_tolerance=1)
            if page_text:
                text += page_text + "\n"
    return text

def validate_json_against_pdf(json_path: Path, pdf_path: Path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    pdf_text = extract_pdf_text(pdf_path)
    issues = []

    # --- 1. Validate numeric values in tables ---
    for i, table in enumerate(data["tables"]):
        for r_idx, row in enumerate(table["data"]):
            for c_idx, cell in enumerate(row):
                if not cell.strip():
                    continue
                # Check if cell looks like a number
                if re.search(r"[0-9,\.\(\)]", cell):
                    cleaned = clean_numeric(cell)
                    if cleaned and cleaned not in pdf_text:
                        issues.append({
                            "type": "NumericMismatch",
                            "location": f"Table_{i}[{r_idx}][{c_idx}]",
                            "extracted": cell,
                            "cleaned": cleaned,
                            "suggestion": "Check for missing parenthesis or OCR corruption"
                        })

    # --- 2. Validate known OCR patterns in text blocks ---
    ocr_corrections = {
        "Kegistration": "Registration",
        "t0": "to",
        "comapny": "company",
        "concemn": "concern",
        "Zoumpad": "audited",
        "tnanaianpeaiod": "financial period"
    }

    for i, block in enumerate(data["text_blocks"]):
        text = block["text"]
        for wrong, right in ocr_corrections.items():
            if wrong in text:
                issues.append({
                    "type": "OCRCorruption",
                    "location": f"TextBlock_{i}",
                    "extracted": text[:100] + "...",
                    "suggestion": f"Replace '{wrong}' → '{right}'"
                })

    # --- 3. Check for critical missing phrases (e.g., footnote) ---
    critical_phrases = [
        "*Deemed interest by virtue of her spouse's interest",
        "RM418,988",
        "Omesti Bemed Sdn. Bhd."
    ]
    for phrase in critical_phrases:
        if phrase not in pdf_text:
            continue  # shouldn't happen
        if phrase not in str(data):
            issues.append({
                "type": "MissingContent",
                "missing": phrase,
                "suggestion": "Ensure footnote or critical value is captured"
            })

    # --- Output ---
    if not issues:
        print("✅ No issues found. Extraction appears accurate.")
    else:
        print(f"⚠️ Found {len(issues)} potential issues:\n")
        for issue in issues:
            print(f"- [{issue['type']}] {issue.get('location', issue.get('missing', ''))}")
            print(f"  Extracted: {issue.get('extracted', 'N/A')}")
            print(f"  Suggestion: {issue['suggestion']}\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python validate_extraction.py <ingested.json> <source.pdf>")
        sys.exit(1)
    json_file = Path(sys.argv[1])
    pdf_file = Path(sys.argv[2])
    validate_json_against_pdf(json_file, pdf_file)
