#!/usr/bin/env python3
"""
Amendment Logs Script

This script adds entries to "Amendment Logs" for matched amendments.
It copies the structure from the existing "Logs" section and adds validation data
from the validation extractor, with a "Note" column set to "Amended".

Input: JSON data with validation metrics and contract information
Output: Updated JSON with Amendment Logs entry for matched amendments
"""

from datetime import datetime
from typing import Dict, Any


class AmendmentLogsExtractor:
    def __init__(self):
        """Initialize the Amendment Logs extractor."""
        pass

    def extract_validation_metrics(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract validation metrics from JSON data."""
        metrics = {}
        
        # Extract counts and rates from validation data
        metrics.update({
            "ActualSubCnt": json_data.get("ActualSubCnt", 0),
            "ExtractedSubCnt": json_data.get("ExtractedSubCnt", 0),
            "MatchedSubCnt": json_data.get("MatchedSubCnt", 0),
            "ActualLisCnt": json_data.get("ActualLisCnt", 0),
            "ExtractedLisCnt": json_data.get("ExtractedLisCnt", 0),
            "MatchedLisCnt": json_data.get("MatchedLisCnt", 0),
            "ActualWorkflowCnt": json_data.get("ActualWorkflowCnt", 0),
            "ExtractedWorkflowCnt": json_data.get("ExtractedWorkflowCnt", 0),
            "MatchedWorkflowCnt": json_data.get("MatchedWorkflowCnt", 0),
            "ActualDocVCnt": json_data.get("ActualDocVCnt", 0),
            "ExtractedDocVCnt": json_data.get("ExtractedDocVCnt", 0),
            "MatchedDocVCnt": json_data.get("MatchedDocVCnt", 0),
            "ActualWatchlistCnt": json_data.get("ActualWatchlistCnt", 0),
            "ExtractedWatchlistCnt": json_data.get("ExtractedWatchlistCnt", 0),
            "MatchedWatchlistCnt": json_data.get("MatchedWatchlistCnt", 0),
            "ActualFraudCnt": json_data.get("ActualFraudCnt", 0),
            "ExtractedFraudCnt": json_data.get("ExtractedFraudCnt", 0),
            "MatchedFraudCnt": json_data.get("MatchedFraudCnt", 0),
            "ActualEidCnt": json_data.get("ActualEidCnt", 0),
            "ExtractedEidCnt": json_data.get("ExtractedEidCnt", 0),
            "MatchedEidCnt": json_data.get("MatchedEidCnt", 0),
            "ActualKYBCnt": json_data.get("ActualKYBCnt", 0),
            "ExtractedKYBCnt": json_data.get("ExtractedKYBCnt", 0),
            "MatchedKYBCnt": json_data.get("MatchedKYBCnt", 0),
        })
        
        # Extract confidence scores and matching rates
        metrics.update({
            "PersonMatchExtractionConfidenceScore": json_data.get("PersonMatchExtractionConfidenceScore", "NA"),
            "PersonMatchMatchingRate": json_data.get("PersonMatchMatchingRate", "NA"),
            "SubExtractionConfidenceScore": json_data.get("% Sub Extraction Confidence Score", "NA"),
            "SubMatchingRate": json_data.get("% Sub Matching Rate", "NA"),
            "LISExtractionConfidenceScore": json_data.get("% LIS Extraction Confidence Score", "NA"),
            "LISMatchingRate": json_data.get("% LIS Matching Rate", "NA"),
            "WorkflowExtractionConfidenceScore": json_data.get("WorkflowConfidenceScore", "NA"),
            "WorkflowMatchingRate": json_data.get("WorkflowMatchingRate", "NA"),
            "DocVExtractionConfidenceScore": json_data.get("DocVConfidenceScore", "NA"),
            "DocVMatchingRate": json_data.get("DocVMatchingRate", "NA"),
            "WatchlistExtractionConfidenceScore": json_data.get("WatchlistConfidenceScore", "NA"),
            "WatchlistMatchingRate": json_data.get("WatchlistMatchingRate", "NA"),
            "FraudExtractionConfidenceScore": json_data.get("FraudConfidenceScore", "NA"),
            "FraudMatchingRate": json_data.get("FraudMatchingRate", "NA"),
            "EidConfidenceScore": json_data.get("EidConfidenceScore", "NA"),
            "EidMatchingRate": json_data.get("EidMatchingRate", "NA"),
            "KYBConfidenceScore": json_data.get("KYBConfidenceScore", "NA"),
            "KYBMatchingRate": json_data.get("KYBMatchingRate", "NA"),
        })
        
        return metrics

    def extract_contract_info(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract contract information from JSON data."""
        contract_info = {}
        
        # Look for contract information in different possible locations
        if "Contract" in json_data and isinstance(json_data["Contract"], list) and json_data["Contract"]:
            # Use the first contract if multiple exist
            contract = json_data["Contract"][0]
            contract_info.update({
                "Contractid": contract.get("ContractExternalId", ""),
                "AccountName": contract.get("AccountName", ""),
            })
        elif "output_records" in json_data:
            # Look in output_records for contract data
            for record in json_data["output_records"]:
                if record.get("name") == "Contract" and record.get("data"):
                    contract = record["data"][0]  # Use first contract
                    contract_info.update({
                        "Contractid": contract.get("ContractExternalId", ""),
                        "AccountName": contract.get("AccountName", ""),
                    })
                    break
        
        return contract_info

    def create_amendment_log_entry(self, json_data: Dict[str, Any], fileName: str, processing_time: float, customer_name: str = None) -> Dict[str, Any]:
        """Create an amendment log entry with all required fields."""
        
        # Extract validation metrics
        metrics = self.extract_validation_metrics(json_data)
        
        # Extract contract information
        contract_info = self.extract_contract_info(json_data)
        
        # Use provided customer_name for unmatched cases, otherwise use contract info
        account_name = customer_name if customer_name else contract_info.get("AccountName", "")
        
        # For unmatched cases, don't process ContractExternalId or IsStandard
        if customer_name:
            # Unmatched case - no contract ID, no StandardCheck
            amendment_log_entry = {
                "TimeStamp": datetime.now().isoformat(),
                "FileName": fileName,
                "Contractid": "",  # No contract ID for unmatched
                "AccountName": account_name,
                "Note": "Amended",  # This will be overridden for unmatched cases
                "IsStandardPaperWork": "",  # No StandardCheck for unmatched
                "TotalRunTime (s)": processing_time,
            }
        else:
            # Matched case - normal processing
            # Extract StandardCheck information
            is_standard_paperwork = False
            if "StandardCheck" in json_data and isinstance(json_data["StandardCheck"], dict):
                is_standard_paperwork = json_data["StandardCheck"].get("IsStandard", False)
            
            # Create the amendment log entry
            amendment_log_entry = {
                "TimeStamp": datetime.now().isoformat(),
                "FileName": fileName,
                "Contractid": contract_info.get("Contractid", ""),
                "AccountName": account_name,
                "Note": "Amended",
                "IsStandardPaperWork": is_standard_paperwork,
                "TotalRunTime (s)": processing_time,
            }
        
        # Add all validation metrics
        amendment_log_entry.update(metrics)
        
        return amendment_log_entry

    def add_amendment_log_entry(self, json_data: Dict[str, Any], fileName: str, processing_time: float, customer_name: str = None) -> Dict[str, Any]:
        """
        Add an amendment log entry to the JSON data.
        
        Args:
            json_data: The JSON data to update
            pdf_filename: Name of the PDF file that was processed
            processing_time: Total processing time in seconds
            customer_name: Customer name for unmatched cases (optional)
            
        Returns:
            Updated JSON data with Amendment Logs entry
        """
        # Create the amendment log entry
        amendment_log_entry = self.create_amendment_log_entry(json_data, fileName, processing_time, customer_name)
        
        # Initialize Amendment Logs section if it doesn't exist
        if "Amendment Logs" not in json_data:
            json_data["Amendment Logs"] = []
        
        # Add the entry to Amendment Logs
        json_data["Amendment Logs"].append(amendment_log_entry)
        
        print(f"✅ Added Amendment Log entry for: {fileName}")
        print(f"   Contract ID: {amendment_log_entry.get('Contractid', 'N/A')}")
        print(f"   Account Name: {amendment_log_entry.get('AccountName', 'N/A')}")
        print(f"   Processing Time: {processing_time:.2f} seconds")
        
        return json_data

    def extract_amendment_logs_data(self, json_data: Dict[str, Any], fileName: str, processing_time: float, customer_name: str = None) -> Dict[str, Any]:
        """
        Main method to extract amendment logs data and add entry to JSON.
        
        Args:
            json_data: The JSON data to update
            pdf_filename: Name of the PDF file that was processed
            processing_time: Total processing time in seconds
            customer_name: Customer name for unmatched cases (optional)
            
        Returns:
            Updated JSON data with Amendment Logs entry
        """
        if not isinstance(json_data, dict):
            raise TypeError("json_data must be a Python dict.")
        
        print("Starting Amendment Logs extraction process...")
        
        # Add amendment log entry
        updated_json = self.add_amendment_log_entry(json_data, fileName, processing_time, customer_name)
        
        print("Amendment Logs extraction completed successfully!")
        return updated_json


# if __name__ == "__main__":
#     # Test the class
#     test_json_path = "BACKEND/THIRDV/amendments/extracted_data.json"
#     test_pdf_filename = "test_amendment.pdf"
#     test_processing_time = 120.5
    
#     # Load test JSON
#     with open(test_json_path, 'r', encoding='utf-8') as f:
#         test_json_data = json.load(f)
    
#     # Create extractor instance and process data
#     extractor = AmendmentLogsExtractor()
#     updated_json = extractor.extract_amendment_logs_data(test_json_data, test_pdf_filename, test_processing_time)
    
#     # Save the updated JSON
#     output_path = test_json_path.replace('.json', '_with_amendment_logs.json')
#     with open(output_path, 'w', encoding='utf-8') as f:
#         json.dump(updated_json, f, indent=4, ensure_ascii=False)
    
#     print(f"Updated JSON saved to: {output_path}") 