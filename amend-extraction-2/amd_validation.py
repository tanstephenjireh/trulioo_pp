import pdfplumber
import json
import re
import io

class ValidationExtractor:
    def __init__(self):
        """Initialize the Validation extractor with keyword sets for different product types."""
        # ========== KEYWORD SETS ==========
        self.WORKFLOW_EXACT_KEYWORDS = {
            "Workflow",
            "Navigator & Training Materials",
            "Navigator & Training Material",
            "Workflow Orchestration",
            "Orchestration",
            "Navigator and Training Material",
            "Document Data Extraction (Business)",
            "Document Data Extraction (Individual)",
            "Workflow Studio"
        }

        self.DOCV_EXACT_KEYWORDS = {
            "Identity Document Verification - Verification with Face Biometrics",
            "Identity Document Verification - Verification With Face Biometrics",
            "Identity Verification",
            "DocV - Verification With Face Biometrics"
        }

        self.WATCHLIST_EXACT_KEYWORDS = {
            "Watchlist - One-Time Search",
            "Watchlist - Ongoing Monitoring",
            "Watchlist - One-Time Search (Screening)",
            "Watchlist - Screening",
            "Watchlist - Ongoing Screening"
        }

        self.FRAUD_EXACT_KEYWORDS = {
            "Fraud Intelligence – Person Fraud"
        }

        self.EID_EXACT_KEYWORDS = {
            "e-ID"
        }

    def extract_all_table_rows(self, pdf_path):
        """Extract all table rows from the PDF."""
        all_rows = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_path)) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if row:
                                all_rows.append(row)
        except Exception as e:
            print(f"⚠️  Warning: Could not process PDF file '{pdf_path}': {e}")
            print(f"   This might not be a valid PDF file or could be corrupted.")
            print(f"   Table-based validation will be skipped for this file.")
        return all_rows

    def count_exact_matches(self, rows, keyword_set):
        """Count rows where any cell exactly matches a keyword."""
        count = 0
        for row in rows:
            if any(cell.strip() in keyword_set for cell in row if cell):
                count += 1
        return count

    def get_matching_rows(self, rows, keyword_set):
        """Return rows where any cleaned cell exactly matches a keyword."""
        matching_rows = []
        for row in rows:
            cleaned_row = [' '.join(cell.replace('\n', ' ').split()).strip() if cell else '' for cell in row]
            if any(cell in keyword_set for cell in cleaned_row):
                matching_rows.append(cleaned_row)
        return matching_rows

    def get_extracted_counts_from_json_dict(self, json_data):
        """Get extracted counts from JSON data using amd_ prefixes."""
        extracted_workflow_count = 0
        extracted_docv_count = 0
        extracted_watchlist_count = 0
        extracted_fraud_count = 0
        extracted_eid_count = 0
        extracted_kyb_count = 0

        if "output_records" in json_data:
            for record in json_data["output_records"]:
                if record.get("name") == "Subscription" and "data" in record:
                    for entry in record["data"]:
                        sub_id = entry.get("subExternalId", "")
                        if sub_id.startswith("amd_workflow_sub_"):
                            extracted_workflow_count += 1
                        elif sub_id.startswith("amd_docv_sub_"):
                            extracted_docv_count += 1
                        elif sub_id.startswith("amd_watchlist_sub_"):
                            extracted_watchlist_count += 1
                        elif sub_id.startswith("amd_fraud_sub_"):
                            extracted_fraud_count += 1  
                        elif sub_id.startswith("amd_eid_sub_"):
                            extracted_eid_count += 1
                        elif sub_id.startswith("amd_kyb_sub_"):
                            extracted_kyb_count += 1

        return extracted_workflow_count, extracted_docv_count, extracted_watchlist_count, extracted_fraud_count, extracted_eid_count, extracted_kyb_count

    def get_product_matching_counts_from_json_dict(self, json_data):
        """Get product matching counts from JSON data using amd_ prefixes."""
        matched_workflow_count = 0
        matched_docv_count = 0
        matched_watchlist_count = 0
        matched_fraud_count = 0
        matched_eid_count = 0
        matched_kyb_count = 0

        if "output_records" in json_data:
            for record in json_data["output_records"]:
                if record.get("name") == "Subscription" and "data" in record:
                    for entry in record["data"]:
                        sub_id = entry.get("subExternalId", "")
                        product_id = entry.get("ProductId")
                        
                        # Only count if ProductId is not null
                        if product_id is not None:
                            if sub_id.startswith("amd_workflow_sub_"):
                                matched_workflow_count += 1
                            elif sub_id.startswith("amd_docv_sub_"):
                                matched_docv_count += 1
                            elif sub_id.startswith("amd_watchlist_sub_"):
                                matched_watchlist_count += 1
                            elif sub_id.startswith("amd_fraud_sub_"):
                                matched_fraud_count += 1
                            elif sub_id.startswith("amd_eid_sub_"):
                                matched_eid_count += 1
                            elif sub_id.startswith("amd_kyb_sub_"):
                                matched_kyb_count += 1

        return matched_workflow_count, matched_docv_count, matched_watchlist_count, matched_fraud_count, matched_eid_count, matched_kyb_count

    def update_json_with_counts(
        self,
        json_data,
        actual_workflow_cnt, extracted_workflow_cnt,
        actual_docv_cnt, extracted_docv_cnt,
        actual_watchlist_cnt, extracted_watchlist_cnt,
        actual_fraud_cnt, extracted_fraud_cnt,
        actual_eid_cnt, extracted_eid_cnt,
        matched_workflow_cnt, matched_docv_cnt, matched_watchlist_cnt, matched_fraud_cnt, matched_eid_cnt,
        actual_kyb_cnt=None, extracted_kyb_cnt=None, matched_kyb_cnt=None, kyb_matching_rate=None, kyb_confidence_score=None
    ):
        """Update JSON with validation counts and metrics."""
        # Preserve existing subscription and line item counts from contract extractor and salesforce enricher
        # Only add these if they don't already exist
        if "ActualSubCnt" not in json_data:
            json_data["ActualSubCnt"] = 0
        if "ActualLisCnt" not in json_data:
            json_data["ActualLisCnt"] = 0
        if "ExtractedSubCnt" not in json_data:
            json_data["ExtractedSubCnt"] = 0
        if "ExtractedLisCnt" not in json_data:
            json_data["ExtractedLisCnt"] = 0
        if "MatchedSubCnt" not in json_data:
            json_data["MatchedSubCnt"] = 0
        if "MatchedLisCnt" not in json_data:
            json_data["MatchedLisCnt"] = 0
            
        json_data["ActualWorkflowCnt"] = actual_workflow_cnt
        json_data["ExtractedWorkflowCnt"] = extracted_workflow_cnt
        json_data["MatchedWorkflowCnt"] = matched_workflow_cnt
        json_data["ActualDocVCnt"] = actual_docv_cnt
        json_data["ExtractedDocVCnt"] = extracted_docv_cnt
        json_data["MatchedDocVCnt"] = matched_docv_cnt
        json_data["ActualWatchlistCnt"] = actual_watchlist_cnt
        json_data["ExtractedWatchlistCnt"] = extracted_watchlist_cnt
        json_data["MatchedWatchlistCnt"] = matched_watchlist_cnt
        json_data["ActualFraudCnt"] = actual_fraud_cnt
        json_data["ExtractedFraudCnt"] = extracted_fraud_cnt
        json_data["MatchedFraudCnt"] = matched_fraud_cnt
        json_data["ActualEidCnt"] = actual_eid_cnt
        json_data["ExtractedEidCnt"] = extracted_eid_cnt
        json_data["MatchedEidCnt"] = matched_eid_cnt
        
        # Workflow Matching Rate
        if actual_workflow_cnt == 0 and extracted_workflow_cnt == 0:
            json_data["WorkflowMatchingRate"] = "NA"
        else:
            json_data["WorkflowMatchingRate"] = f"{(matched_workflow_cnt / extracted_workflow_cnt * 100):.2f}%" if extracted_workflow_cnt > 0 else "0.00%"
        
        # DocV Matching Rate
        if actual_docv_cnt == 0 and extracted_docv_cnt == 0:
            json_data["DocVMatchingRate"] = "NA"
        else:
            json_data["DocVMatchingRate"] = f"{(matched_docv_cnt / extracted_docv_cnt * 100):.2f}%" if extracted_docv_cnt > 0 else "0.00%"
        
        # Watchlist Matching Rate
        if actual_watchlist_cnt == 0 and extracted_watchlist_cnt == 0:
            json_data["WatchlistMatchingRate"] = "NA"
        else:
            json_data["WatchlistMatchingRate"] = f"{(matched_watchlist_cnt / extracted_watchlist_cnt * 100):.2f}%" if extracted_watchlist_cnt > 0 else "0.00%"
        
        # Fraud Matching Rate
        if actual_fraud_cnt == 0 and extracted_fraud_cnt == 0:
            json_data["FraudMatchingRate"] = "NA"
        else:
            json_data["FraudMatchingRate"] = f"{(matched_fraud_cnt / extracted_fraud_cnt * 100):.2f}%" if extracted_fraud_cnt > 0 else "0.00%"
        
        # Eid Matching Rate
        if actual_eid_cnt == 0 and extracted_eid_cnt == 0:
            json_data["EidMatchingRate"] = "NA"
        else:
            json_data["EidMatchingRate"] = f"{(matched_eid_cnt / extracted_eid_cnt * 100):.2f}%" if extracted_eid_cnt > 0 else "0.00%"
        
        # Workflow Confidence Score
        if actual_workflow_cnt == 0 and extracted_workflow_cnt == 0:
            json_data["WorkflowConfidenceScore"] = "NA"
        else:
            workflow_score = (1 - abs(actual_workflow_cnt - extracted_workflow_cnt) / max(actual_workflow_cnt, extracted_workflow_cnt, 1)) * 100
            json_data["WorkflowConfidenceScore"] = f"{workflow_score:.2f}%"
        
        # DocV Confidence Score
        if actual_docv_cnt == 0 and extracted_docv_cnt == 0:
            json_data["DocVConfidenceScore"] = "NA"
        else:
            docv_score = (1 - abs(actual_docv_cnt - extracted_docv_cnt) / max(actual_docv_cnt, extracted_docv_cnt, 1)) * 100
            json_data["DocVConfidenceScore"] = f"{docv_score:.2f}%"
        
        # Watchlist Confidence Score
        if actual_watchlist_cnt == 0 and extracted_watchlist_cnt == 0:
            json_data["WatchlistConfidenceScore"] = "NA"
        else:
            watchlist_score = (1 - abs(actual_watchlist_cnt - extracted_watchlist_cnt) / max(actual_watchlist_cnt, extracted_watchlist_cnt, 1)) * 100
            json_data["WatchlistConfidenceScore"] = f"{watchlist_score:.2f}%"
        
        # Fraud Confidence Score
        if actual_fraud_cnt == 0 and extracted_fraud_cnt == 0:
            json_data["FraudConfidenceScore"] = "NA"
        else:
            fraud_score = (1 - abs(actual_fraud_cnt - extracted_fraud_cnt) / max(actual_fraud_cnt, extracted_fraud_cnt, 1)) * 100
            json_data["FraudConfidenceScore"] = f"{fraud_score:.2f}%"
        
        # Eid Confidence Score
        if actual_eid_cnt == 0 and extracted_eid_cnt == 0:
            json_data["EidConfidenceScore"] = "NA"
        else:
            eid_score = (1 - abs(actual_eid_cnt - extracted_eid_cnt) / max(actual_eid_cnt, extracted_eid_cnt, 1)) * 100
            json_data["EidConfidenceScore"] = f"{eid_score:.2f}%"
        
        # KYB Metrics (optional)
        if actual_kyb_cnt is not None:
            json_data["ActualKYBCnt"] = actual_kyb_cnt
        if extracted_kyb_cnt is not None:
            json_data["ExtractedKYBCnt"] = extracted_kyb_cnt
        if matched_kyb_cnt is not None:
            json_data["MatchedKYBCnt"] = matched_kyb_cnt
        if kyb_matching_rate is not None:
            json_data["KYBMatchingRate"] = kyb_matching_rate
        if kyb_confidence_score is not None:
            json_data["KYBConfidenceScore"] = kyb_confidence_score
        
        return json_data

    def save_updated_json(self, json_data, output_path):
        """Save updated JSON to file."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=4)

    def extract_kyb_counts_from_md_text(self, md_text):
        """Extract KYB counts from markdown text by counting the number of Group rows and multiplying by the number of price columns present (Search, Essentials, Insights, Complete)."""
        try:
            print("=== DEBUG: Starting KYB extraction (new logic) ===")
            print(f"[DEBUG] Input markdown length: {len(md_text)}")
            
            # Step 1: Find the KYB section
            header_pattern = r'# Selected Services (?:and|&) Pricing: Business Verification'
            header_match = re.search(header_pattern, md_text, re.IGNORECASE)
            if not header_match:
                print("✗ KYB section header NOT FOUND")
                all_headers = re.findall(r'# .*', md_text)
                print(f"  [DEBUG] Found headers in text: {all_headers}")
                return 0
            print(f"✓ Found KYB section header at position {header_match.start()}-{header_match.end()}")
            kyb_start = header_match.start()
            remaining_text = md_text[kyb_start:]
            next_section_match = re.search(r'\n# [^B]', remaining_text)
            if next_section_match:
                kyb_end = kyb_start + next_section_match.start()
                print(f"[DEBUG] KYB section ends at position: {kyb_end}")
            else:
                kyb_end = len(md_text)
                print(f"[DEBUG] KYB section extends to end of file: {kyb_end}")
            kyb_section = md_text[kyb_start:kyb_end]
            print(f"  [DEBUG] KYB section length: {len(kyb_section)} characters")
            print(f"  [DEBUG] First 300 chars of KYB section: {kyb_section[:300]}")
            
            # Step 2: Find the table header row with price columns
            lines = kyb_section.split('\n')
            header_row = None
            for line in lines:
                print(f"[DEBUG] Checking line for header row: {line}")
                if line.strip().startswith('|') and (
                    'Search' in line or 'Essentials' in line or 'Insights' in line or 'Complete' in line
                ):
                    print(f"[DEBUG] --> This line is a header row with price columns!")
                    header_row = line
                    break
                else:
                    print(f"[DEBUG] --> Not a header row with price columns.")
            if not header_row:
                print("✗ No header row with price columns found")
                return 0
            print(f"✓ Found header row: {header_row}")
            header_cells = [cell.strip() for cell in header_row.split('|') if cell.strip()]
            print(f"  [DEBUG] Header cells: {header_cells}")
            
            # Step 3: Identify which price columns exist
            price_columns = []
            for col in ['Search', 'Essentials', 'Insights', 'Complete']:
                if any(col.lower() in cell.lower() for cell in header_cells):
                    price_columns.append(col)
            print(f"✓ Found price columns: {price_columns}")
            
            # Step 4: Count the number of Group rows
            group_rows = []
            for line in lines:
                if line.strip().startswith('|') and re.search(r'Group \d+', line):
                    group_rows.append(line)
            group_count = len(group_rows)
            print(f"✓ Found {group_count} Group rows")
            for i, row in enumerate(group_rows):
                print(f"  [DEBUG] Group row {i+1}: {row}")
            
            # Step 5: Calculate total KYB count
            kyb_count = group_count * len(price_columns)
            print(f"=== DEBUG: KYB extraction complete. Calculation: {group_count} groups × {len(price_columns)} price columns = {kyb_count} ===")
            return kyb_count
        except Exception as e:
            print(f"Error processing markdown text: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def get_extracted_kyb_count_from_json_dict(self, json_data):
        """Get extracted KYB count from JSON data using amd_ prefixes."""
        extracted_kyb_count = 0
        if "output_records" in json_data:
            for record in json_data["output_records"]:
                if record.get("name") == "Subscription" and "data" in record:
                    for entry in record["data"]:
                        sub_id = entry.get("subExternalId", "")
                        if sub_id.startswith("amd_kyb_sub_"):
                            extracted_kyb_count += 1
        return extracted_kyb_count

    def get_matched_kyb_count_from_json_dict(self, json_data):
        """Get matched KYB count from JSON data using amd_ prefixes."""
        matched_kyb_count = 0
        if "output_records" in json_data:
            for record in json_data["output_records"]:
                if record.get("name") == "Subscription" and "data" in record:
                    for entry in record["data"]:
                        sub_id = entry.get("subExternalId", "")
                        product_id = entry.get("ProductId")
                        if product_id is not None and sub_id.startswith("amd_kyb_sub_"):
                            matched_kyb_count += 1
        return matched_kyb_count

    def update_json_with_kyb_counts(self, json_data, actual_kyb_cnt, extracted_kyb_cnt, matched_kyb_cnt):
        """Update JSON with KYB-specific counts and metrics."""
        json_data["ActualKYBCnt"] = actual_kyb_cnt
        json_data["ExtractedKYBCnt"] = extracted_kyb_cnt
        json_data["MatchedKYBCnt"] = matched_kyb_cnt
        if actual_kyb_cnt == 0 and extracted_kyb_cnt == 0:
            json_data["KYBMatchingRate"] = "NA"
        else:
            json_data["KYBMatchingRate"] = f"{(matched_kyb_cnt / extracted_kyb_cnt * 100):.2f}%" if extracted_kyb_cnt > 0 else "0.00%"
        if actual_kyb_cnt == 0 and extracted_kyb_cnt == 0:
            json_data["KYBConfidenceScore"] = "NA"
        else:
            kyb_score = (1 - abs(actual_kyb_cnt - extracted_kyb_cnt) / max(actual_kyb_cnt, extracted_kyb_cnt, 1)) * 100
            json_data["KYBConfidenceScore"] = f"{kyb_score:.2f}%"
        return json_data

    def run_validation(self, pdf_path, json_data):
        """Run validation against PDF file."""
        # Step 1: Extract all rows from PDF tables
        all_rows = self.extract_all_table_rows(pdf_path)

        # Step 2: Use the cleaned matching function
        matching_workflow_rows = self.get_matching_rows(all_rows, self.WORKFLOW_EXACT_KEYWORDS)
        matching_docv_rows = self.get_matching_rows(all_rows, self.DOCV_EXACT_KEYWORDS)
        matching_watchlist_rows = self.get_matching_rows(all_rows, self.WATCHLIST_EXACT_KEYWORDS)
        matching_fraud_rows = self.get_matching_rows(all_rows, self.FRAUD_EXACT_KEYWORDS)
        matching_eid_rows = [
            row for row in all_rows
            if len(row) > 2 and (row[2] or '').strip() in self.EID_EXACT_KEYWORDS
        ]

        actual_workflow_count = len(matching_workflow_rows)
        actual_docv_count = len(matching_docv_rows)
        actual_watchlist_count = len(matching_watchlist_rows)
        actual_fraud_count = len(matching_fraud_rows)
        actual_eid_count = len(matching_eid_rows)

        # Step 3: Extract subExternalId counts from JSON data
        extracted_workflow_count, extracted_docv_count, extracted_watchlist_count, extracted_fraud_count, extracted_eid_count, extracted_kyb_count = self.get_extracted_counts_from_json_dict(json_data)

        # Step 3.5: Get product matching counts from JSON data
        matched_workflow_count, matched_docv_count, matched_watchlist_count, matched_fraud_count, matched_eid_count, matched_kyb_count = self.get_product_matching_counts_from_json_dict(json_data)

        # Step 4: Update JSON with all counts
        updated_json = self.update_json_with_counts(
            json_data,
            actual_workflow_count, extracted_workflow_count,
            actual_docv_count, extracted_docv_count,
            actual_watchlist_count, extracted_watchlist_count,
            actual_fraud_count, extracted_fraud_count,
            actual_eid_count, extracted_eid_count,
            matched_workflow_count, matched_docv_count, matched_watchlist_count, matched_fraud_count, matched_eid_count
        )
        
        # Console output
        print(f"ActualWorkflowCnt: {actual_workflow_count}")
        print(f"ExtractedWorkflowCnt: {extracted_workflow_count}")
        print(f"WorkflowConfidenceScore: {updated_json['WorkflowConfidenceScore']}")
        print(f"WorkflowMatchingRate: {updated_json['WorkflowMatchingRate']}")
        print(f"ActualDocVCnt: {actual_docv_count}")
        print(f"ExtractedDocVCnt: {extracted_docv_count}")
        print(f"DocVConfidenceScore: {updated_json['DocVConfidenceScore']}")
        print(f"DocVMatchingRate: {updated_json['DocVMatchingRate']}")
        print(f"ActualWatchlistCnt: {actual_watchlist_count}")
        print(f"ExtractedWatchlistCnt: {extracted_watchlist_count}")
        print(f"WatchlistConfidenceScore: {updated_json['WatchlistConfidenceScore']}")
        print(f"WatchlistMatchingRate: {updated_json['WatchlistMatchingRate']}")
        print(f"ActualFraudCnt: {actual_fraud_count}")
        print(f"ExtractedFraudCnt: {extracted_fraud_count}")
        print(f"FraudConfidenceScore: {updated_json['FraudConfidenceScore']}")
        print(f"FraudMatchingRate: {updated_json['FraudMatchingRate']}")
        print(f"ActualEidCnt: {actual_eid_count}")
        print(f"ExtractedEidCnt: {extracted_eid_count}")
        print(f"EidConfidenceScore: {updated_json['EidConfidenceScore']}")
        print(f"EidMatchingRate: {updated_json['EidMatchingRate']}")

        # Step 5: Return the updated JSON
        return updated_json

    def run_validation_with_md_text(self, md_text, json_data):
        """Run validation against markdown text for KYB."""
        actual_kyb_count = self.extract_kyb_counts_from_md_text(md_text)
        extracted_kyb_count = self.get_extracted_kyb_count_from_json_dict(json_data)
        matched_kyb_count = self.get_matched_kyb_count_from_json_dict(json_data)
        updated_json = self.update_json_with_kyb_counts(json_data, actual_kyb_count, extracted_kyb_count, matched_kyb_count)
        print(f"ActualKYBCnt: {actual_kyb_count}")
        print(f"ExtractedKYBCnt: {extracted_kyb_count}")
        print(f"MatchedKYBCnt: {matched_kyb_count}")
        print(f"KYBConfidenceScore: {updated_json['KYBConfidenceScore']}")
        print(f"KYBMatchingRate: {updated_json['KYBMatchingRate']}")
        return updated_json

    def extract_validation_data(self, json_data, md_text=None, pdf_path=None):
        """
        Main method to extract validation data and update JSON with metrics.
        """
        if not isinstance(json_data, dict):
            raise TypeError("json_data must be a Python dict.")

        print("Starting Validation extraction process...")
        
        updated_json = json_data
        ran_any = False

        if pdf_path:
            updated_json = self.run_validation(pdf_path, updated_json)
            print(f"PDF validation complete for: {pdf_path}")
            ran_any = True

        if md_text:
            updated_json = self.run_validation_with_md_text(md_text, updated_json)
            print(f"KYB validation complete from markdown input.")
            print(f"ActualKYBCnt: {updated_json.get('ActualKYBCnt')}")
            print(f"ExtractedKYBCnt: {updated_json.get('ExtractedKYBCnt')}")
            print(f"MatchedKYBCnt: {updated_json.get('MatchedKYBCnt')}")
            print(f"KYBMatchingRate: {updated_json.get('KYBMatchingRate')}")
            print(f"KYBConfidenceScore: {updated_json.get('KYBConfidenceScore')}")
            ran_any = True

        if not ran_any:
            print("No PDF or markdown input provided. Only standard JSON loaded, no validation performed.")

        print("Validation extraction completed successfully!")
        return updated_json


# if __name__ == "__main__":
#     # Test the class
#     json_path = "BACKEND/THIRDV/output_single_json/HarbourFront_Wealth_-_PM_DocV_-_Canada-Trulioo-17750-12-May-2025-15-8-52-signed.json"
#     pdf_path = "BACKEND/THIRDV/input_single/HarbourFront_Wealth_-_PM_DocV_-_Canada-Trulioo-17750-12-May-2025-15-8-52-signed.pdf"
#     md_path = "BACKEND/THIRDV/output_single_json/parsed_HarbourFront_Wealth_-_PM_DocV_-_Canada-Trulioo-17750-12-May-2025-15-8-52-signed.md"

#     # Load JSON
#     with open(json_path, 'r', encoding='utf-8') as jf:
#         json_data = json.load(jf)

#     # Load markdown if file exists
#     md_text = None
#     if os.path.exists(md_path):
#         with open(md_path, 'r', encoding='utf-8') as mf:
#             md_text = mf.read()

#     # Create extractor instance and process data
#     extractor = ValidationExtractor()
#     updated_json = extractor.extract_validation_data(json_data, md_text, pdf_path)

#     # Save the updated JSON
#     output_path = json_path.replace('.json', '_amd_validated.json')
#     with open(output_path, 'w', encoding='utf-8') as out_f:
#         json.dump(updated_json, out_f, indent=2, ensure_ascii=False)
#     print(f"Updated JSON saved to: {output_path}") 