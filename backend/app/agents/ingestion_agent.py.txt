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

# Configure logging for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Pydantic Models for Structured Output ---
class ExtractedTable(BaseModel):
    """Represents an extracted table."""
    index: int = Field(..., description="Index of the table in the document.")
    markdown: str = Field(..., description="The table content in Markdown format.")
    # Optional: Add bounding box, page number, etc., if needed
    page_number: Optional[int] = Field(None, description="The page number where the table was found.")

class ExtractedTextBlock(BaseModel):
    """Represents an extracted text block."""
    index: int = Field(..., description="Index of the text block in the document.")
    html: str = Field(..., description="The text content in HTML format.")
    # Optional: Add type (paragraph, heading, etc.), page number, etc.
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
        logger.info("Initializing Ingestion Agent...")
        # Configure pipeline options for PDF processing (if applicable in 2.1.0)
        # Note: PdfPipelineOptions might be used differently or not needed
        # depending on how DocumentConverter is configured in this version.
        # Let's try initializing DocumentConverter without pipeline_options first.
        pipeline_options = PdfPipelineOptions()
        # Add any specific pipeline options here if needed (e.g., enable/disable features)
        # Example: pipeline_options.do_ocr = True # Usually handled by Docling internally if needed

        # Configure the DocumentConverter
        # Enable the formats we want to output: Markdown for tables, HTML for text
        format_options = {
            InputFormat.PDF: FormatOption(
                # Enable markdown for tables
                generate_markdown=True,
                # --- ADDED REQUIRED FIELDS FOR FormatOption (Required by 2.1.0) ---
                backend=PyPdfiumDocumentBackend, # Specify the backend class
                pipeline_cls=StandardPdfPipeline # Specify the pipeline class
                # --- END ADDED REQUIRED FIELDS ---
            )
        }

        # --- CORRECTED DocumentConverter Initialization for 2.1.0 ---
        # DO NOT include pipeline_options argument for DocumentConverter in 2.1.0
        self.converter = DocumentConverter(
            # pipeline_options=pipeline_options, # REMOVED - Not accepted by 2.1.0's DocumentConverter
            format_options=format_options
        )
        # --- END CORRECTED Initialization ---
        logger.info("Ingestion Agent initialized.")
        
        # Set output directory, defaulting to 'ingested_data' three levels up from the current script location
        # This assumes the script is located in 'backend/app/agents/'
        # So, 'Path(__file__).parent.parent.parent' moves up to 'backend/', and then we create 'ingested_data' there.
        self.output_dir = output_dir or Path(__file__).resolve().parent.parent.parent / "ingested_data"
        os.makedirs(self.output_dir, exist_ok=True) # Ensure directory exists


    def _markdown_to_list_of_lists(self, markdown_str: str) -> List[List[str]]:
        """
        Converts a Markdown table string into a list of lists of strings.
        This is the format expected by the Mapping Agent.
        """
        if not markdown_str:
            return []
        lines = markdown_str.strip().split('\n')
        # Filter out lines that are just separators (e.g., | --- | --- |)
        data_lines = [line for line in lines if line.startswith('|') and not (line.startswith('| ---') or line.startswith('|:---') or line.startswith('| ---:'))]
        list_of_lists = []
        for line in data_lines:
            # Remove leading/trailing pipe and split by pipe
            row = [cell.strip() for cell in line.strip('|').split('|')]
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
            # Convert the document using Docling
            logger.info("Converting document using Docling...")
            result: ConversionResult = self.converter.convert(str(pdf_path))
            docling_doc: DoclingDocument = result.document

            if not docling_doc:
                 error_msg = f"Docling failed to create a document object for: {pdf_path}. Error: {result.errors}"
                 logger.error(error_msg)
                 return IngestionResult(success=False, message=error_msg, source_path=str(pdf_path))

            # --- Extract Tables (Markdown) and Text Blocks (HTML) ---
            # --- ACCESS CONTENT FROM docling_doc.tables and docling_doc.texts (Corrected for 2.1.0) ---
            logger.info("Extracting tables and text blocks...")
            tables: List[ExtractedTable] = []
            text_blocks: List[ExtractedTextBlock] = []
            table_counter = 0
            text_counter = 0

            # --- Extract Tables ---
            logger.info(f"Found {len(docling_doc.tables) if hasattr(docling_doc, 'tables') else 0} table objects in docling_doc.tables")
            for table_obj in docling_doc.tables: # Iterate through tables in docling_doc.tables
                # Verify the object type is TableItem
                if isinstance(table_obj, TableItem): # Use imported TableItem class
                    # Docling's TableItem object should have a markdown representation
                    try:
                        # --- CORRECTED EXPORT CALL (Addressing deprecation warning) ---
                        # Pass the parent document (docling_doc) to the export function
                        markdown_content = table_obj.export_to_markdown(doc=docling_doc) # Use Docling's built-in method for TableItem with doc argument
                    except AttributeError:
                        logger.warning(f"Table object ({type(table_obj)}) does not have 'export_to_markdown' method. Skipping.")
                        continue # Skip if the method doesn't exist

                    # --- CORRECTED PAGE NUMBER ACCESS (Addressing AttributeError) ---
                    # The 'loc' attribute is not available on TableItem in this version.
                    # Page number might be accessed differently or might not be directly available from the TableItem object itself.
                    # Let's try accessing it via the 'parent' or 'source' if available, or set it to None initially.
                    # Common attributes for location/page might be 'page_no', 'page_index', or within a 'location' object.
                    # Since 'loc' failed, let's attempt a safe get using getattr.
                    # Check docling_core API documentation for 2.1.0 for the correct path.
                    # For now, let's assume page number is not directly on the TableItem or is accessed differently.
                    # A common alternative in Docling is that the page info might be part of the item's provenance or not easily accessible here.
                    # For this version, let's log the warning and set page_number to None.
                    page_num = None # Default if location cannot be determined easily
                    # Example of trying a different attribute (uncomment if such an attribute exists):
                    # page_num = getattr(table_obj, 'page_no', None) # Try 'page_no' attribute
                    # Or check if there's a location-like object with page info:
                    # loc_obj = getattr(table_obj, 'location', None) # Try 'location' attribute
                    # if loc_obj and hasattr(loc_obj, 'page_no'):
                    #     page_num = loc_obj.page_no
                    # For now, we'll proceed with page_num = None and address location later if critical.

                    tables.append(ExtractedTable(index=table_counter, markdown=markdown_content, page_number=page_num))
                    table_counter += 1
                    logger.debug(f"Extracted table {table_counter} (Page {page_num}).")
                else:
                    logger.debug(f"Object in docling_doc.tables is not a TableItem: {type(table_obj)}")

            # --- Extract Text Blocks ---
            logger.info(f"Found {len(docling_doc.texts) if hasattr(docling_doc, 'texts') else 0} text objects in docling_doc.texts")
            for text_obj in docling_doc.texts: # Iterate through texts in docling_doc.texts
                # Verify the object type is TextItem
                if isinstance(text_obj, TextItem): # Use imported TextItem class
                    # Docling's TextItem object usually has a text attribute.
                    try:
                        plain_text = text_obj.text
                    except AttributeError:
                        logger.warning(f"Text object ({type(text_obj)}) does not have 'text' attribute. Skipping.")
                        continue # Skip if the attribute doesn't exist

                    # To get HTML, we might need to reconstruct it or rely on Docling's internal structure if available.
                    # For now, let's wrap the plain text in basic HTML paragraph tags as a placeholder.
                    # A more sophisticated approach might involve parsing Docling's layout structure.
                    html_content = f"<p>{plain_text}</p>"
                    # --- CORRECTED PAGE NUMBER ACCESS (Addressing AttributeError) ---
                    # Similar to TableItem, 'loc' attribute is likely not available on TextItem either.
                    page_num = None # Default if location cannot be determined easily
                    # Example of trying a different attribute (uncomment if such an attribute exists):
                    # page_num = getattr(text_obj, 'page_no', None) # Try 'page_no' attribute
                    # Or check if there's a location-like object with page info:
                    # loc_obj = getattr(text_obj, 'location', None) # Try 'location' attribute
                    # if loc_obj and hasattr(loc_obj, 'page_no'):
                    #     page_num = loc_obj.page_no
                    # For now, we'll proceed with page_num = None and address location later if critical.

                    # Attempt to infer text type (basic heuristic based on Docling's item properties if available)
                    text_type = "paragraph" # Default
                    # Example: if text_obj.has_property('is_heading'): text_type = "heading"
                    # This requires checking Docling's API for available properties.

                    text_blocks.append(ExtractedTextBlock(index=text_counter, html=html_content, page_number=page_num, text_type=text_type))
                    text_counter += 1
                    logger.debug(f"Extracted text block {text_counter} (Page {page_num}).")
                else:
                    logger.debug(f"Object in docling_doc.texts is not a TextItem: {type(text_obj)}")

            num_pages = len(docling_doc.pages) if hasattr(docling_doc, 'pages') else None

            logger.info(f"Ingestion successful. Extracted {len(tables)} tables and {len(text_blocks)} text blocks.")

            # --- Prepare Output for Mapping Agent ---
            if save_to_json:
                raw_tables_for_mapping = []
                for i, pydantic_table in enumerate(tables):
                    table_data_as_lists = self._markdown_to_list_of_lists(pydantic_table.markdown)
                    raw_table = {
                        "name": f"Table_{pydantic_table.index}",
                        "data": table_data_as_lists,
                        "header_row_index": 0 # Assuming first row is header, adjust if needed based on analysis
                    }
                    raw_tables_for_mapping.append(raw_table)

                # Text blocks can also be included if the mapping agent needs them later
                raw_text_blocks_for_mapping = [
                    {"text": tb.html, "page_number": tb.page_number} for tb in text_blocks
                ]

                output_data_for_mapping = {
                    "source_pdf": pdf_path.name,
                    "tables": raw_tables_for_mapping,
                    "text_blocks": raw_text_blocks_for_mapping
                }

                # --- Save Output to JSON File ---
                output_filename = f"{pdf_path.stem}_ingested.json" # e.g., example.pdf -> example_ingested.json
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
            logger.error(error_msg, exc_info=True) # Log the full traceback
            return IngestionResult(success=False, message=error_msg, source_path=str(pdf_path))


# --- Main Execution Block (for testing the agent directly) ---
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python backend/app/agents/ingestion_agent.py <path_to_pdf>")
        sys.exit(1)

    pdf_file_path = sys.argv[1]
    agent = IngestionAgent() # Uses default output directory
    result = agent.process(pdf_file_path, save_to_json=True) # Ensure save_to_json is True

    print(f"Success: {result.success}")
    print(f"Message: {result.message}")
    if result.success:
        print(f"Number of Pages: {result.num_pages}")
        print(f"Extracted {len(result.tables)} tables and {len(result.text_blocks)} text blocks.")
        # Optionally print first few results for quick inspection
        for i, table in enumerate(result.tables[:2]): # Print first 2 tables
            print(f"\n--- Table {i+1} (Page {table.page_number}) ---")
            print(table.markdown)
        for i, text_block in enumerate(result.text_blocks[:2]): # Print first 2 text blocks
            print(f"\n--- Text Block {i+1} (Page {text_block.page_number}) ---")
            print(text_block.html)
