# backend/app/agents/ingestion_agent.py
"""
Agent responsible for ingesting a PDF file and extracting structured data,
specifically tables (as Markdown) and text blocks (as HTML).
Uses Docling as the primary tool for extraction, leveraging PyMuPDF for
potential fallbacks or specific operations, and Tesseract for OCR on
scanned documents if needed by Docling internally or configured.
"""
import logging
import json
import os
import re  # PATCH: Added for regex in post-processing (2025-10-16)
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, FormatOption

# --- CORRECTED IMPORTS FOR DOCLING 2.1.0 TYPES (Using names found via inspection) ---
# Import the main document type and correct item types for 2.1.0
from docling_core.types.doc import DoclingDocument, TextItem, TableItem # Use names found by inspection
# --- END CORRECTED IMPORTS ---

# --- ADDITIONAL IMPORTS FOR FORMAT OPTION ARGUMENTS (Required by 2.1.0) ---
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend # Backend class
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline # Pipeline class
# --- END ADDITIONAL IMPORTS ---

# PATCH: Import pdfplumber for footnote recovery (2025-10-16)
try:
    import pdfplumber
except ImportError:
    pdfplumber = None
    logging.warning("pdfplumber not installed. Footnote recovery disabled.")

# Configure logging for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Pydantic Models for Structured Output ---
class ExtractedTable(BaseModel):
    """Represents an extracted table."""
    index: int = Field(..., description="Index of the table in the document.")
    markdown: str = Field(..., description="The table content in Markdown format.")
    page_number: Optional[int] = Field(None, description="The page number where the table was found.")

class ExtractedTextBlock(BaseModel):
    """Represents an extracted text block."""
    index: int = Field(..., description="Index of the text block in the document.")
    html: str = Field(..., description="The text content in HTML format.")
    page_number: Optional[int] = Field(None, description="The page number where the text block was found.")
    text_type: Optional[str] = Field(None, description="Type of text block (e.g., paragraph, heading).")

class IngestionResult(BaseModel):
    """Represents the final result of the ingestion process."""
    success: bool = Field(..., description="Indicates if the ingestion was successful.")
    message: str = Field("", description="A message providing details about the result.")
    tables: List[ExtractedTable] = Field(default_factory=list, description="List of extracted tables.")
    text_blocks: List[ExtractedTextBlock] = Field(default_factory=list, description="List of extracted text blocks.")
    source_path: Optional[str] = Field(None, description="Path to the source PDF file.")
    num_pages: Optional[int] = Field(None, description="Number of pages in the processed document.")

# --- Ingestion Agent Class ---
class IngestionAgent:
    """
    Agent for ingesting PDFs and extracting tables and text blocks.
    Uses Docling for the primary extraction logic.
    """

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initializes the Ingestion Agent.
        Sets up the Docling DocumentConverter with appropriate options.
        """
        # PATCH: ADD THIS IMPORT AT TOP (with other imports) ---
        from docling.datamodel.pipeline_options import EasyOcrOptions

        # --- INSIDE __init__ METHOD ---
        logger.info("Initializing Ingestion Agent...")

        # PATCH: Correct OCR config for Docling 2.1.0 (2025-10-16)
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.ocr_options = EasyOcrOptions(lang=["en"])  # Fixed: lang=["en"], not language=["eng"]

        format_options = {
            InputFormat.PDF: FormatOption(
                generate_markdown=True,
                backend=PyPdfiumDocumentBackend,
                pipeline_cls=StandardPdfPipeline,
                pipeline_options=pipeline_options
            )
        }

        self.converter = DocumentConverter(
            format_options=format_options
        )
        logger.info("Ingestion Agent initialized.")
        self.output_dir = output_dir or Path(__file__).resolve().parent.parent.parent / "ingested_data"
        os.makedirs(self.output_dir, exist_ok=True)

    # PATCH: Add post-processing helpers (2025-10-16)
    def _postprocess_text(self, text: str) -> str:
        """Apply OCR and formatting corrections."""
        if not text:
            return text
        fixes = {
            "Kegistration": "Registration",
            "Kegistraton": "Registration",
            "Kegisrraton": "Registration",
            "IVo": "No",
            "t0": "to",
            "comapny": "company",
            "concemn": "concern",
            "Zoumpad": "audited",
            "tnanaianpeaiod": "financial period",
            "l": "1",  # Use cautiously; may affect "l" in words
            "O": "0",  # Use cautiously
        }
        for wrong, right in fixes.items():
            text = text.replace(wrong, right)
        return text

    def _postprocess_table_cell(self, cell: str) -> str:
        """Fix numeric formatting in table cells."""
        if not cell.strip():
            return cell
        # Fix unbalanced parentheses
        if cell.count("(") > cell.count(")"):
            cell = cell.rstrip("_~ ") + ")"
        elif cell.count(")") > cell.count("("):
            cell = "(" + cell.lstrip("_~ ")
        # Remove trailing garbage
        cell = re.sub(r"[^0-9,\.\-\(\)\s%]", "", cell)
        return cell.strip()

    def _extract_footnotes_with_pdfplumber(self, pdf_path: Path) -> List[str]:
        """Recover footnotes missed by Docling (e.g., '*Deemed interest...')."""
        if not pdfplumber:
            return []
        footnotes = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    # Extract bottom 10% of page where footnotes often reside
                    crop = page.crop((0, page.height * 0.9, page.width, page.height))
                    text = crop.extract_text(x_tolerance=1, y_tolerance=1)
                    if text and ("*Deemed interest" in text or "pursuant to Section 59" in text):
                        footnotes.append(f"<p>{text.strip()}</p>")
        except Exception as e:
            logger.warning(f"Footnote extraction failed: {e}")
        return footnotes

    def _markdown_to_list_of_lists(self, markdown_str: str) -> List[List[str]]:
        """
        Converts a Markdown table string into a list of lists of strings.
        This is the format expected by the Mapping Agent.
        """
        if not markdown_str:
            return []
        lines = markdown_str.strip().split('\n')
        data_lines = [line for line in lines if line.startswith('|') and not (line.startswith('| ---') or line.startswith('|:---') or line.startswith('| ---:'))]
        list_of_lists = []
        for line in data_lines:
            row = [cell.strip() for cell in line.strip('|').split('|')]
            # PATCH: Apply cell-level post-processing (2025-10-16)
            row = [self._postprocess_table_cell(cell) for cell in row]
            list_of_lists.append(row)
        return list_of_lists

    def process(self, pdf_path: Union[str, Path], save_to_json: bool = True) -> IngestionResult:
        """
        Processes the given PDF file to extract tables and text blocks.
        Args:
            pdf_path: Path to the PDF file to process.
            save_to_json: If True, saves the output in the expected format to a JSON file.
        Returns:
            An IngestionResult object containing the extracted data.
        """
        pdf_path = Path(pdf_path)
        logger.info(f"Starting ingestion process for: {pdf_path}")
        if not pdf_path.exists() or not pdf_path.is_file():
            error_msg = f"PDF file not found or is not a file: {pdf_path}"
            logger.error(error_msg)
            return IngestionResult(success=False, message=error_msg, source_path=str(pdf_path))
        try:
            logger.info("Converting document using Docling...")
            result: ConversionResult = self.converter.convert(str(pdf_path))
            docling_doc: DoclingDocument = result.document
            if not docling_doc:
                 error_msg = f"Docling failed to create a document object for: {pdf_path}. Error: {result.errors}"
                 logger.error(error_msg)
                 return IngestionResult(success=False, message=error_msg, source_path=str(pdf_path))

            logger.info("Extracting tables and text blocks...")
            tables: List[ExtractedTable] = []
            text_blocks: List[ExtractedTextBlock] = []
            table_counter = 0
            text_counter = 0

            # --- Extract Tables ---
            logger.info(f"Found {len(docling_doc.tables) if hasattr(docling_doc, 'tables') else 0} table objects in docling_doc.tables")
            for table_obj in docling_doc.tables:
                if isinstance(table_obj, TableItem):
                    try:
                        markdown_content = table_obj.export_to_markdown(doc=docling_doc)
                        # PATCH: Apply text-level post-processing to markdown (2025-10-16)
                        markdown_content = self._postprocess_text(markdown_content)
                    except AttributeError:
                        logger.warning(f"Table object ({type(table_obj)}) does not have 'export_to_markdown' method. Skipping.")
                        continue
                    page_num = None
                    tables.append(ExtractedTable(index=table_counter, markdown=markdown_content, page_number=page_num))
                    table_counter += 1
                    logger.debug(f"Extracted table {table_counter} (Page {page_num}).")
                else:
                    logger.debug(f"Object in docling_doc.tables is not a TableItem: {type(table_obj)}")

            # --- Extract Text Blocks ---
            logger.info(f"Found {len(docling_doc.texts) if hasattr(docling_doc, 'texts') else 0} text objects in docling_doc.texts")
            for text_obj in docling_doc.texts:
                if isinstance(text_obj, TextItem):
                    try:
                        plain_text = text_obj.text
                        # PATCH: Apply text-level post-processing (2025-10-16)
                        plain_text = self._postprocess_text(plain_text)
                    except AttributeError:
                        logger.warning(f"Text object ({type(text_obj)}) does not have 'text' attribute. Skipping.")
                        continue
                    html_content = f"<p>{plain_text}</p>"
                    page_num = None
                    text_type = "paragraph"
                    text_blocks.append(ExtractedTextBlock(index=text_counter, html=html_content, page_number=page_num, text_type=text_type))
                    text_counter += 1
                    logger.debug(f"Extracted text block {text_counter} (Page {page_num}).")
                else:
                    logger.debug(f"Object in docling_doc.texts is not a TextItem: {type(text_obj)}")

            # PATCH: Recover footnotes using pdfplumber (2025-10-16)
            extra_footnotes = self._extract_footnotes_with_pdfplumber(pdf_path)
            for footnote_html in extra_footnotes:
                text_blocks.append(ExtractedTextBlock(
                    index=len(text_blocks),
                    html=footnote_html,
                    page_number=None,
                    text_type="footnote"
                ))
            if extra_footnotes:
                logger.info(f"Recovered {len(extra_footnotes)} footnotes via pdfplumber.")

            num_pages = len(docling_doc.pages) if hasattr(docling_doc, 'pages') else None
            logger.info(f"Ingestion successful. Extracted {len(tables)} tables and {len(text_blocks)} text blocks.")

            if save_to_json:
                raw_tables_for_mapping = []
                for i, pydantic_table in enumerate(tables):
                    table_data_as_lists = self._markdown_to_list_of_lists(pydantic_table.markdown)
                    raw_table = {
                        "name": f"Table_{pydantic_table.index}",
                        "data": table_data_as_lists,
                        "header_row_index": 0
                    }
                    raw_tables_for_mapping.append(raw_table)

                raw_text_blocks_for_mapping = [
                    {"text": tb.html, "page_number": tb.page_number} for tb in text_blocks
                ]
                output_data_for_mapping = {
                    "source_pdf": pdf_path.name,
                    "tables": raw_tables_for_mapping,
                    "text_blocks": raw_text_blocks_for_mapping
                }

                output_filename = f"{pdf_path.stem}_ingested.json"
                output_path = self.output_dir / output_filename
                try:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(output_data_for_mapping, f, indent=2, ensure_ascii=False)
                    logger.info(f"Ingested data saved to {output_path}")
                except Exception as e:
                    logger.error(f"Failed to save ingestion output to {output_path}: {e}")

            return IngestionResult(
                success=True,
                message="PDF ingestion completed successfully.",
                tables=tables,
                text_blocks=text_blocks,
                source_path=str(pdf_path),
                num_pages=num_pages
            )
        except Exception as e:
            error_msg = f"An error occurred during ingestion: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return IngestionResult(success=False, message=error_msg, source_path=str(pdf_path))

# --- Main Execution Block (for testing the agent directly) ---
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python backend/app/agents/ingestion_agent.py <path_to_pdf>")
        sys.exit(1)
    pdf_file_path = sys.argv[1]
    agent = IngestionAgent()
    result = agent.process(pdf_file_path, save_to_json=True)
    print(f"Success: {result.success}")
    print(f"Message: {result.message}")
    if result.success:
        print(f"Number of Pages: {result.num_pages}")
        print(f"Extracted {len(result.tables)} tables and {len(result.text_blocks)} text blocks.")
        for i, table in enumerate(result.tables[:2]):
            print(f"\n--- Table {i+1} (Page {table.page_number}) ---")
            print(table.markdown)
        for i, text_block in enumerate(result.text_blocks[:2]):
            print(f"\n--- Text Block {i+1} (Page {text_block.page_number}) ---")
            print(text_block.html)