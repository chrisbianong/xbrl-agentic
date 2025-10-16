# mapping_agent.py

import json
import logging
import os
import re
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd
from thefuzz import fuzz # Requires: pip install thefuzz
from typing import List, Dict, Any, Optional, Tuple

# --- Configuration ---
# Define base directories
PROJECT_ROOT = Path(__file__).parent
INGESTED_DATA_DIR = PROJECT_ROOT / "backend" / "ingested_data"
TAXONOMY_DIR = PROJECT_ROOT / "backend" / "taxonomies" / "SSMxT_2022v1"
OUTPUT_DIR = PROJECT_ROOT / "backend" / "mapped_facts"
REFERENCE_XLSX_PATH = PROJECT_ROOT / "backend" / "reference" / "FS-MFRS-2022-12-31.xlsx" # Adjust filename if needed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Taxonomy Parser Class ---
class SSMxT2022TaxonomyParser:
    """
    Parses SSMxT 2022 taxonomy files to extract concept information.
    """
    def __init__(self, taxonomy_dir: Path):
        self.taxonomy_dir = taxonomy_dir
        self.concepts = {} # {concept_name: {label_en, label_local, ...}}
        self.load_concepts()

    def load_concepts(self):
        """
        Loads concept names and labels from the taxonomy files.
        Specifically looks for the 'label.xml' file.
        """
        label_xml_path = None
        for root, dirs, files in os.walk(self.taxonomy_dir):
            for file in files:
                if file.endswith("label.xml"):
                    label_xml_path = Path(root) / file
                    break
            if label_xml_path:
                break

        if not label_xml_path or not label_xml_path.exists():
            logger.error(f"Label XML file not found in {self.taxonomy_dir}")
            return

        logger.info(f"Loading labels from {label_xml_path}")
        try:
            tree = ET.parse(label_xml_path)
            root = tree.getroot()
            # Define namespaces if present in the XML
            # Example namespace might be like: xmlns:link="http://www.xbrl.org/2003/linkbase"
            # You might need to adjust based on the actual XML structure
            # For now, assuming no namespace or using local tags
            # If namespaces are present, you'd use them like:
            # label_role = root.find(".//{http://www.xbrl.org/2003/linkbase}roleType[@roleURI='http://www.xbrl.org/2008/role/label']")
            # A more robust way is to get the default namespace dynamically:
            # ns = {'link': 'http://www.xbrl.org/2003/linkbase', 'xlink': 'http://www.w3.org/1999/xlink'}
            # Or find the namespace map from the root element:
            # _, tags = root.tag.split('}') if '}' in root.tag else ('', root.tag)
            # For simplicity, assuming no prefixes are needed in the find expression if namespace is default.
            # The actual structure might require namespace handling.
            # Let's attempt to find label resources directly.
            # Common structure is: <link:label> with attributes like for (concept), lang, role, and text content.
            # Example tag might be {http://www.xbrl.org/2003/linkbase}label
            # Let's get the namespace map from the root:
            namespace_map = {}
            for event, elem in ET.iterparse(str(label_xml_path), events=('start-ns',)):
                 if event == 'start-ns':
                     prefix, uri = elem
                     namespace_map[prefix if prefix else 'default'] = uri
            logger.debug(f"Detected namespaces: {namespace_map}")
            # Assuming the default namespace for linkbase elements is common
            # Typical URIs: http://www.xbrl.org/2003/XLink-1.1, http://www.xbrl.org/2003/linkbase
            # Let's assume the default namespace for linkbase elements if present
            # Find the default namespace for linkbase elements
            default_ns = namespace_map.get('default', namespace_map.get('link', ''))
            if default_ns:
                 # Use the default namespace for finding elements
                 # The xlink namespace is often needed for 'href' attribute
                 xlink_ns = namespace_map.get('xlink', 'http://www.w3.org/1999/xlink')
                 # Example: find elements like <link:labelArc>
                 # labels = root.findall(f".//{{{default_ns}}}label")
                 # Resources are often under <link:labelLink>
                 label_links = root.findall(f".//{{{default_ns}}}labelLink")
                 for link in label_links:
                     label_resources = link.findall(f".//{{{default_ns}}}label")
                     for label_elem in label_resources:
                         concept_href = label_elem.get(f"{{{xlink_ns}}}resource") # xlink:href points to the concept
                         if concept_href:
                             # Extract the concept name from the href (e.g., #ca-mfrs_Revenue)
                             concept_name = concept_href.split('#')[-1]
                             lang = label_elem.get('lang', 'en')
                             role = label_elem.get('role', '')
                             label_text = label_elem.text.strip() if label_elem.text else ''
                             if concept_name not in self.concepts:
                                 self.concepts[concept_name] = {'labels': {}}
                             self.concepts[concept_name]['labels'][f'label_{lang}_{role}'] = label_text
                             # Also store a simpler 'label_en' if available
                             if lang == 'en' and 'label_en' not in self.concepts[concept_name]:
                                 self.concepts[concept_name]['label_en'] = label_text
                             if lang != 'en' and 'label_local' not in self.concepts_name]:
                                 self.concepts[concept_name]['label_local'] = label_text # Store first non-en label found

            # If no default namespace found, try without namespace
            else:
                 labels = root.findall(".//label") # This is less likely to work if namespace is required
                 # This path assumes no namespace is used, which is rare for XBRL
                 # The previous block with namespace handling is more robust.
                 # If the previous block didn't find labels, this might be needed, but structure likely requires namespace.
                 # Let's assume the previous block handled it correctly for now.
                 pass

        except ET.ParseError as e:
            logger.error(f"Error parsing label XML {label_xml_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading concepts from {label_xml_path}: {e}")

    def get_concept_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Retrieve concept details by its name."""
        return self.concepts.get(name)

    def get_all_concepts(self) -> Dict[str, Any]:
        """Retrieve all loaded concepts."""
        return self.concepts

# --- Mapping Agent Class ---
class MappingAgent:
    """
    Maps extracted data from the Ingestion Agent to SSMxT 2022 taxonomy concepts.
    """
    def __init__(self, taxonomy_parser: SSMxT2022TaxonomyParser, reference_xlsx_path: Optional[Path] = None):
        self.taxonomy_parser = taxonomy_parser
        self.reference_xlsx_path = reference_xlsx_path
        self.reference_mappings = self._load_reference_mappings()

    def _load_reference_mappings(self) -> Dict[str, str]:
        """
        Loads a reference mapping dictionary from the Excel file (if available).
        This can be used for high-confidence lookups or validation during development.
        Assumes the Excel file has columns like 'Label' and 'Element Name'.
        """
        if not self.reference_xlsx_path or not self.reference_xlsx_path.exists():
            logger.warning(f"Reference Excel file not found: {self.reference_xlsx_path}")
            return {}

        logger.info(f"Loading reference mappings from {self.reference_xlsx_path}")
        try:
            # Load the Excel file
            df = pd.read_excel(self.reference_xlsx_path, sheet_name=0) # Assuming first sheet

            # Identify relevant columns - this requires knowledge of the Excel structure
            # Common names might be 'Label', 'Element Name', 'Concept Name', 'Disclosure', 'Amount'
            # Attempt to find standard names, or prompt user if ambiguous
            label_col = None
            element_col = None
            for col in df.columns:
                if 'label' in col.lower():
                    label_col = col
                elif 'element' in col.lower() or 'concept' in col.lower():
                    element_col = col
            if not label_col or not element_col:
                 logger.warning(f"Could not find standard 'Label' and 'Element Name' columns in {self.reference_xlsx_path}. Columns found: {list(df.columns)}")
                 return {}

            # Create a mapping dictionary: {label: element_name}
            # Drop rows where either label or element is NaN
            df_clean = df[[label_col, element_col]].dropna()
            mapping_dict = dict(zip(df_clean[label_col].astype(str), df_clean[element_col].astype(str)))
            logger.info(f"Loaded {len(mapping_dict)} reference mappings.")
            return mapping_dict
        except Exception as e:
            logger.error(f"Error loading reference mappings from {self.reference_xlsx_path}: {e}")
            return {}

    def _fuzzy_match_label(self, extracted_label: str, threshold: int = 80) -> Optional[Tuple[str, float]]:
        """
        Attempts to find a matching taxonomy concept using fuzzy string matching.
        Returns the best match concept name and its confidence score (0-100).
        """
        best_match = None
        best_score = 0
        best_concept_name = None

        for concept_name, concept_info in self.taxonomy_parser.get_all_concepts().items():
            labels = concept_info.get('labels', {})
            # Check against all available labels for the concept
            for label_key, label_text in labels.items():
                if label_text: # Ensure label text is not empty
                    score = fuzz.partial_ratio(extracted_label.lower(), label_text.lower())
                    if score > best_score:
                        best_score = score
                        best_concept_name = concept_name
                        best_match = label_text
            # Also check against the simplified 'label_en' if present
            label_en = concept_info.get('label_en')
            if label_en:
                score = fuzz.partial_ratio(extracted_label.lower(), label_en.lower())
                if score > best_score:
                    best_score = score
                    best_concept_name = concept_name
                    best_match = label_en

        if best_score >= threshold:
            return best_concept_name, best_score / 100.0 # Normalize score to 0-1
        else:
            return None, 0.0

    def _exact_match_label(self, extracted_label: str) -> Optional[str]:
        """
        Attempts to find an exact match for the label in the reference mappings.
        """
        # Check reference mappings first for high confidence
        if self.reference_mappings:
            for label, element_name in self.reference_mappings.items():
                if extracted_label.lower().strip() == label.lower().strip():
                    return element_name
        # If not found in reference, could also check taxonomy labels directly for exact match
        # This is less common but possible
        for concept_name, concept_info in self.taxonomy_parser.get_all_concepts().items():
            labels = concept_info.get('labels', {}).values()
            label_en = concept_info.get('label_en')
            if label_en and extracted_label.lower().strip() == label_en.lower().strip():
                return concept_name
            for label_text in labels:
                 if label_text and extracted_label.lower().strip() == label_text.lower().strip():
                     return concept_name
        return None

    def map_tables(self, tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Maps a list of extracted tables to taxonomy concepts.
        Returns a list of mapped facts.
        """
        mapped_facts = []
        for table_idx, table in enumerate(tables):
            logger.info(f"Processing table {table_idx}: {table.get('name', 'Unnamed Table')}")
            header_row_idx = table.get('header_row_index', 0)
            rows = table.get('data', [])
            headers = rows[header_row_idx] if 0 <= header_row_idx < len(rows) else []

            # Assume the first column contains labels, subsequent columns contain values
            for row_idx, row in enumerate(rows):
                if row_idx == header_row_idx: # Skip header row for data mapping
                    continue
                if not row: # Skip empty rows
                    continue
                label_cell = row[0] if len(row) > 0 else ""
                if not label_cell:
                    continue # Skip rows without a label

                # Attempt exact match first
                matched_concept = self._exact_match_label(str(label_cell))
                confidence = 1.0
                method = "exact_ref" if matched_concept in self.reference_mappings.values() else "exact_tax"

                if not matched_concept:
                    # If no exact match, try fuzzy matching
                    matched_concept, confidence = self._fuzzy_match_label(str(label_cell))
                    method = "fuzzy"

                if matched_concept:
                    # Extract values from other columns (assuming columns are periods/amounts)
                    for col_idx, value in enumerate(row[1:], start=1): # Start from column 1
                        if value is not None and str(value).strip() != "":
                            fact = {
                                "concept_name": matched_concept,
                                "value": value,
                                "source": {
                                    "table_name": table.get('name', f'Table_{table_idx}'),
                                    "row_index": row_idx,
                                    "column_index": col_idx,
                                    "label_text": str(label_cell)
                                },
                                "confidence": confidence,
                                "mapping_method": method
                            }
                            mapped_facts.append(fact)
                            logger.debug(f"Mapped: '{label_cell}' -> '{matched_concept}' (Conf: {confidence:.2f}, Method: {method})")
                else:
                    logger.debug(f"No match found for label: '{label_cell}' (Conf: 0.0)")

        return mapped_facts

    def run(self, ingestion_output_path: Path) -> List[Dict[str, Any]]:
        """
        Main execution method for the mapping agent.
        Loads ingestion output, performs mapping, and returns results.
        """
        logger.info(f"Starting mapping process using ingestion output: {ingestion_output_path}")
        try:
            with open(ingestion_output_path, 'r', encoding='utf-8') as f:
                ingestion_data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load ingestion output from {ingestion_output_path}: {e}")
            return []

        tables = ingestion_data.get('tables', [])
        # text_blocks = ingestion_data.get('text_blocks', []) # Future use?

        logger.info(f"Found {len(tables)} tables to map.")
        mapped_facts = self.map_tables(tables)

        logger.info(f"Mapping complete. Generated {len(mapped_facts)} mapped facts.")
        return mapped_facts

# --- Main Execution Block ---
if __name__ == "__main__":
    # --- Setup ---
    logger.info("Initializing Mapping Agent...")
    taxonomy_parser = SSMxT2022TaxonomyParser(TAXONOMY_DIR)
    if not taxonomy_parser.concepts:
        logger.error("Failed to load taxonomy concepts. Cannot proceed with mapping.")
        exit(1)
    mapping_agent = MappingAgent(taxonomy_parser, REFERENCE_XLSX_PATH)

    # --- Process Ingested Files ---
    for json_file in INGESTED_DATA_DIR.glob("*.json"):
        logger.info(f"Processing ingestion output file: {json_file.name}")
        mapped_facts = mapping_agent.run(json_file)

        # --- Output Mapped Facts ---
        output_filename = f"mapped_{json_file.name}" # e.g., mapped_example.json
        output_path = OUTPUT_DIR / output_filename
        os.makedirs(OUTPUT_DIR, exist_ok=True) # Ensure output directory exists
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(mapped_facts, f, indent=2, ensure_ascii=False)
            logger.info(f"Mapped facts saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save mapped facts to {output_path}: {e}")

    logger.info("Mapping Agent execution completed.")
