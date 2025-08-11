#!/usr/bin/env python3
"""
Amendment Processing Pipeline Main Script

PURPOSE:
This script orchestrates the amendment processing pipeline that processes pre-extracted
OCR data and enriches contract amendment information.

PIPELINE FLOW:
1. Load input files: external ID, customer name, markdown, and PDF
2. Check if unmatched (external_id == "no_external_id" OR markdown == "unmatched")
   - If UNMATCHED: Create minimal output with Amendment Logs only
   - If MATCHED: Continue with full extraction pipeline
3. Load JSON data and convert to output_records format
4. Contract Extraction (using markdown content and PDF for table validation)
5. Data Enrichment (Salesforce, DocV, Watchlist, Fraud, Workflow, e-ID, KYB, Discount, Validation, Standard Check)
6. Amendment Processing (add Amendment Logs)
7. Save outputs (JSON, CSV, XLSX)

INPUT REQUIREMENTS:
- external_id_file: Path to .txt file containing contract external ID (or "no_external_id" if unmatched)
- customer_name_file: Path to .txt file containing customer name
- markdown_file: Path to .md file containing extracted markdown content (or "unmatched" if unmatched)
- pdf_file: Path to the original PDF amendment document
- input_json_path: Path to the original JSON file containing existing contract data
- output_folder: Directory where output files will be saved

OUTPUT FILES:
- JSON file: Complete enriched contract data with amendment information
- CSV files: Individual tables exported as CSV (one per table)
- XLSX file: All tables combined in a single Excel workbook

MATCHING LOGIC:
- If external_id == "no_external_id" OR markdown == "unmatched" → Process as unmatched amendment
- If matched → Process full amendment pipeline with all enrichments

RETURN VALUES:
- SUCCESS (MATCHED): Dictionary with status="success", customer_name, contract_external_id, output_folder, processing_time, json_data
- SUCCESS (UNMATCHED): Dictionary with status="success", customer_name, contract_external_id="no_external_id", note about unmatched, json_data (original + amendment log)
- ERROR: Dictionary with status="error", error_type, error_message, timestamp

USAGE:
- As module: result = main(external_id_file, customer_name_file, markdown_file, pdf_file, input_json_path, output_folder)
- Standalone: python amd_main.py (uses hardcoded paths in if __name__ == "__main__")
"""

import time

# Import the required classes
from amd_contract import ContractExtractor
from amd_salesforce_sub import SalesforceEnricher
from amd_docv import DocVExtractor
from amd_watchlist import WatchlistExtractor
from amd_fraud import FraudExtractor
from amd_workflow import WorkflowExtractor
from amd_electronic_id import ElectronicIDExtractor
from amd_kyb import KYBExtractor
from amd_discount_schedule import DiscountScheduleExtractor
from amd_validation import ValidationExtractor
from amd_std_check import StandardCheckExtractor
from amd_logs import AmendmentLogsExtractor

# Import helper functions
from amd_helper import (
    load_input_files, create_unmatched_output, load_and_convert_json,
    extract_contract_data, apply_all_enrichments, add_amendment_logs, create_error_result
)


class AmendmentPipeline:
    def __init__(self):
        """Initialize the amendment processing pipeline."""
        self.contract_extractor = ContractExtractor()
        self.salesforce_enricher = SalesforceEnricher()
        self.docv_extractor = DocVExtractor()
        self.watchlist_extractor = WatchlistExtractor()
        self.fraud_extractor = FraudExtractor()
        self.workflow_extractor = WorkflowExtractor()
        self.electronic_id_extractor = ElectronicIDExtractor()
        self.kyb_extractor = KYBExtractor()
        self.discount_schedule_extractor = DiscountScheduleExtractor()
        self.validation_extractor = ValidationExtractor()
        self.standard_check_extractor = StandardCheckExtractor()
        self.amendment_logs_extractor = AmendmentLogsExtractor()
    
    async def run_pipeline(self, external_id_file, customer_name_file, markdown_file, pdf_file, fileName, input_json_path):
        """Run the amendment processing pipeline."""
        start_time = time.time()
        
        try:
            # STEP 1: Load input files
            # contract_external_id, customer_name, markdown_content = load_input_files(
            #     external_id_file, customer_name_file, markdown_file
            # )
            
            if external_id_file is None:
                return create_error_result("Input file loading failed", "Could not load input files")
            
            # STEP 2: Check if unmatched
            if external_id_file == "no_external_id" or markdown_file == "unmatched":
                return create_unmatched_output(fileName, customer_name_file, start_time)
            
            # # STEP 3: Load and convert JSON data
            # json_data = load_and_convert_json(input_json_path)
            
            # STEP 4: Contract Extraction
            all_json = await extract_contract_data(self.contract_extractor, markdown_file, pdf_file, fileName, external_id_file)
            
            # STEP 5: Data Enrichment
            extractors = {
                'salesforce': self.salesforce_enricher,
                'docv': self.docv_extractor,
                'watchlist': self.watchlist_extractor,
                'fraud': self.fraud_extractor,
                'workflow': self.workflow_extractor,
                'electronic_id': self.electronic_id_extractor,
                'kyb': self.kyb_extractor,
                'discount_schedule': self.discount_schedule_extractor,
                'validation': self.validation_extractor,
                'standard_check': self.standard_check_extractor
            }
            all_json = await apply_all_enrichments(all_json, markdown_file, pdf_file, extractors)
            
            # STEP 6: Amendment Processing
            all_json = add_amendment_logs(self.amendment_logs_extractor, all_json, fileName, start_time)
            
            # Pipeline completion
            total_time = time.time() - start_time
            
            return all_json
            
        except Exception as e:
            return create_error_result("Pipeline execution failed", str(e))


# def main(external_id_file, customer_name_file, markdown_file, pdf_file, input_json_path, output_folder):
#     """Main function to run the amendment processing pipeline."""
    
#     # Validate input files
#     for file_path in [external_id_file, customer_name_file, markdown_file, pdf_file, input_json_path]:
#         if not os.path.exists(file_path):
#             print(f"❌ Error: File not found: {file_path}")
#             return None
    
#     # Create and run pipeline
#     pipeline = AmendmentPipeline()
#     result = asyncio.run(pipeline.run_pipeline(
#         external_id_file=external_id_file,
#         customer_name_file=customer_name_file,
#         markdown_file=markdown_file,
#         pdf_file=pdf_file,
#         input_json_path=input_json_path,
#         output_folder=output_folder
#     ))
    
#     return result


# if __name__ == "__main__":
#     # Hardcoded input paths
#     external_id_file = "BACKEND/THIRDV/amendments_files_08_04/output_json/Kore_Cit_2_OFS_external_id.txt"
#     customer_name_file = "BACKEND/THIRDV/amendments_files_08_04/output_json/Kore_Cit_2_OFS_customer_name.txt"
#     markdown_file = "BACKEND/THIRDV/amendments_files_08_04/output_json/Kore_Cit_2_OFS_unmatched.md"
#     pdf_file = "BACKEND/THIRDV/amendments_files_08_04/Kore_Cit_2_OFS.pdf"
#     input_json_path = "BACKEND/THIRDV/amendments_files_08_04/extracted_data.json"
#     output_folder = "BACKEND/THIRDV/amendments_files_08_04/output"
    
#     # Run pipeline
#     result = main(external_id_file, customer_name_file, markdown_file, pdf_file, input_json_path, output_folder)
    
#     # Handle results
#     if result and result.get("status") == "success":
#         print(f"✅ Pipeline completed successfully!")
#         if "note" in result:
#             print(f"📝 Note: {result['note']}")
        
#         # Save outputs
#         if "json_data" in result:
#             try:
#                 os.makedirs(output_folder, exist_ok=True)
#                 json_data = result["json_data"]
#                 base_filename = os.path.splitext(os.path.basename(markdown_file))[0]
                
#                 # Save JSON
#                 json_filename = f"json_{base_filename}.json"
#                 json_path = os.path.join(output_folder, json_filename)
#                 with open(json_path, 'w', encoding='utf-8') as f:
#                     json.dump(json_data, f, indent=2, ensure_ascii=False)
#                 print(f"✅ JSON saved: {json_path}")
                
#                 # Save CSV files
#                 csv_count = 0
#                 for table in json_data.get("output_records", []):
#                     if table["data"]:
#                         df = pd.DataFrame(table["data"])
#                         csv_path = os.path.join(output_folder, f"{base_filename}_{table['name']}.csv")
#                         df.to_csv(csv_path, index=False)
#                         csv_count += 1
                
#                 # Save top-level sections
#                 for section_name, section_data in json_data.items():
#                     if section_name != "output_records" and section_data:
#                         if isinstance(section_data, dict):
#                             df = pd.DataFrame([section_data])
#                         elif isinstance(section_data, list):
#                             df = pd.DataFrame(section_data)
#                         else:
#                             continue
                        
#                         csv_path = os.path.join(output_folder, f"{base_filename}_{section_name}.csv")
#                         df.to_csv(csv_path, index=False)
#                         csv_count += 1
                
#                 print(f"✅ {csv_count} CSV files saved")
                
#                 # Save XLSX
#                 xlsx_path = os.path.join(output_folder, f"{os.path.splitext(json_filename)[0]}.xlsx")
#                 with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
#                     for section_name, section_data in json_data.items():
#                         if isinstance(section_data, list) and section_data:
#                             df = pd.DataFrame(section_data)
#                             df.to_excel(writer, sheet_name=section_name[:31], index=False)
#                         elif isinstance(section_data, dict):
#                             df = pd.DataFrame([section_data])
#                             df.to_excel(writer, sheet_name=section_name[:31], index=False)
                
#                 print(f"✅ XLSX saved: {xlsx_path}")
                
#             except Exception as e:
#                 print(f"❌ Error saving outputs: {e}")
#     else:
#         print("⚠️  Pipeline did not complete successfully") 