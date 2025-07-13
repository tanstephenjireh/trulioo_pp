import pdfplumber
import json
import io
class Validation:

    def __init__(self):
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

    def extract_all_table_rows(self, pdf_path):
        """Extract all table rows from the PDF."""
        all_rows = []
        with pdfplumber.open(io.BytesIO(pdf_path)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if row:
                            all_rows.append(row)
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
        extracted_workflow_count = 0
        extracted_docv_count = 0
        extracted_watchlist_count = 0
        extracted_fraud_count = 0

        if "output_records" in json_data:
            for record in json_data["output_records"]:
                if record.get("name") == "Subscription" and "data" in record:
                    for entry in record["data"]:
                        sub_id = entry.get("subExternalId", "")
                        if sub_id.startswith("wfstudio_sub_"):
                            extracted_workflow_count += 1
                        elif sub_id.startswith("docv_sub_"):
                            extracted_docv_count += 1
                        elif sub_id.startswith("watchlist_sub_"):
                            extracted_watchlist_count += 1
                        elif sub_id.startswith("fraud_sub_"):
                            extracted_fraud_count += 1

        return extracted_workflow_count, extracted_docv_count, extracted_watchlist_count, extracted_fraud_count
    

    def get_product_matching_counts_from_json_dict(self, json_data):
        matched_workflow_count = 0
        matched_docv_count = 0
        matched_watchlist_count = 0
        matched_fraud_count = 0

        if "output_records" in json_data:
            for record in json_data["output_records"]:
                if record.get("name") == "Subscription" and "data" in record:
                    for entry in record["data"]:
                        sub_id = entry.get("subExternalId", "")
                        product_id = entry.get("ProductId")
                        
                        # Only count if ProductId is not null
                        if product_id is not None:
                            if sub_id.startswith("wfstudio_sub_"):
                                matched_workflow_count += 1
                            elif sub_id.startswith("docv_sub_"):
                                matched_docv_count += 1
                            elif sub_id.startswith("watchlist_sub_"):
                                matched_watchlist_count += 1
                            elif sub_id.startswith("fraud_sub_"):
                                matched_fraud_count += 1

        return matched_workflow_count, matched_docv_count, matched_watchlist_count, matched_fraud_count


    def update_json_with_counts(
        self,
        json_data,
        actual_workflow_cnt, extracted_workflow_cnt,
        actual_docv_cnt, extracted_docv_cnt,
        actual_watchlist_cnt, extracted_watchlist_cnt,
        actual_fraud_cnt, extracted_fraud_cnt,
        matched_workflow_cnt, matched_docv_cnt, matched_watchlist_cnt, matched_fraud_cnt
    ):
        json_data["ActualWorkflowCnt"] = actual_workflow_cnt
        json_data["ExtractedWorkflowCnt"] = extracted_workflow_cnt
        json_data["ActualDocVCnt"] = actual_docv_cnt
        json_data["ExtractedDocVCnt"] = extracted_docv_cnt
        json_data["ActualWatchlistCnt"] = actual_watchlist_cnt
        json_data["ExtractedWatchlistCnt"] = extracted_watchlist_cnt
        json_data["ActualFraudCnt"] = actual_fraud_cnt
        json_data["ExtractedFraudCnt"] = extracted_fraud_cnt
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


        return json_data
    
    def save_updated_json(self, json_data, output_path):
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=4)

    def run_validation(self, pdf_path, json_data):
        # Step 1: Extract all rows from PDF tables
        all_rows = self.extract_all_table_rows(pdf_path)

        # Step 2: Use the cleaned matching function
        matching_workflow_rows = self.get_matching_rows(all_rows, self.WORKFLOW_EXACT_KEYWORDS)
        matching_docv_rows = self.get_matching_rows(all_rows, self.DOCV_EXACT_KEYWORDS)
        matching_watchlist_rows = self.get_matching_rows(all_rows, self.WATCHLIST_EXACT_KEYWORDS)
        matching_fraud_rows = self.get_matching_rows(all_rows, self.FRAUD_EXACT_KEYWORDS)

        actual_workflow_count = len(matching_workflow_rows)
        actual_docv_count = len(matching_docv_rows)
        actual_watchlist_count = len(matching_watchlist_rows)
        actual_fraud_count = len(matching_fraud_rows)

        # Step 3: Extract subExternalId counts from JSON data
        extracted_workflow_count, extracted_docv_count, extracted_watchlist_count, extracted_fraud_count = self.get_extracted_counts_from_json_dict(json_data)

        # Step 3.5: Get product matching counts from JSON data
        matched_workflow_count, matched_docv_count, matched_watchlist_count, matched_fraud_count = self.get_product_matching_counts_from_json_dict(json_data)

        # Step 4: Update JSON with all counts
        updated_json = self.update_json_with_counts(
            json_data,
            actual_workflow_count, extracted_workflow_count,
            actual_docv_count, extracted_docv_count,
            actual_watchlist_count, extracted_watchlist_count,
            actual_fraud_count, extracted_fraud_count,
            matched_workflow_count, matched_docv_count, matched_watchlist_count, matched_fraud_count
        )
        
            # Console output
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
        #print(f"Updated JSON saved to: {output_path}")

        # Step 5: Return the updated JSON
        return updated_json