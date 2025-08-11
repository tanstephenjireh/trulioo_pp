#!/usr/bin/env python3
"""
Helper functions for the Amendment Processing Pipeline

This file contains all helper functions used by amd_main.py to keep the main file clean and simple.
"""

import os
import json
import time
from datetime import datetime


def load_input_files(external_id_file, customer_name_file, markdown_file):
    """Load the three input files and return their contents."""
    try:
        # Load external ID
        with open(external_id_file, 'r', encoding='utf-8') as f:
            contract_external_id = f.read().strip()
        
        # Load customer name
        with open(customer_name_file, 'r', encoding='utf-8') as f:
            customer_name = f.read().strip()
        
        # Load markdown content
        with open(markdown_file, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        
        return contract_external_id, customer_name, markdown_content
        
    except FileNotFoundError as e:
        print(f"❌ Error loading input file: {e}")
        return None, None, None
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return None, None, None


def create_unmatched_output(pdf_file, customer_name, start_time):
    """Create minimal output for unmatched amendments."""
    processing_time = time.time() - start_time
    
    # Create minimal output structure with just Amendment Logs
    minimal_output = {
        "Amendment Logs": [
            {
                "TimeStamp": datetime.now().isoformat(),
                "FileName": pdf_file,
                "AccountName": customer_name,
                "Note": f"Account Name '{customer_name}' not found in the processed order forms"
            }
        ]
    }
    
    return minimal_output

    # return {
    #     "status": "success",
    #     "customer_name": customer_name,
    #     "contract_external_id": "no_external_id",
    #     "output_folder": "output",
    #     "processing_time": processing_time,
    #     "note": f"Unmatched amendment - minimal output created for customer '{customer_name}'.",
    #     "json_data": minimal_output
    # }


def load_and_convert_json(input_json_path):
    """Load JSON data and convert to output_records format."""
    try:
        with open(input_json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        
        # Convert to output_records format if needed
        if "output_records" not in json_data:
            output_records = []
            for section_name, section_data in json_data.items():
                if isinstance(section_data, list):
                    output_records.append({
                        "name": section_name,
                        "data": section_data
                    })
            
            converted_json = {"output_records": output_records}
            
            # Preserve other top-level fields
            for key, value in json_data.items():
                if not isinstance(value, list):
                    converted_json[key] = value
            
            json_data = converted_json
        
        return json_data
        
    except Exception as e:
        print(f"❌ Error loading JSON data: {e}")
        raise e


async def extract_contract_data(contract_extractor, markdown_content, pdf_file, fileName, contract_external_id):
    """Extract contract data from markdown and PDF."""
    try:
        all_json = await contract_extractor.extract_contract_pipeline_from_md(
            markdown_content, 
            file_path=pdf_file,
            fileName=fileName,
            contract_external_id=contract_external_id
        )
        return all_json
    except Exception as e:
        print(f"❌ Error in contract extraction: {e}")
        raise e


async def apply_all_enrichments(all_json, markdown_content, pdf_file, extractors):
    """Apply all data enrichments in sequence."""
    try:
        # Salesforce enrichment (synchronous)
        all_json = extractors['salesforce'].enrich_contract_data(all_json)
        
        # DocV extraction (async)
        all_json = await extractors['docv'].extract_docv_data(markdown_content, all_json)
        
        # Watchlist extraction (async)
        all_json = await extractors['watchlist'].extract_watchlist_data(markdown_content, all_json)
        
        # Fraud extraction (async)
        all_json = await extractors['fraud'].extract_fraud_data(markdown_content, all_json)
        
        # Workflow extraction (async)
        all_json = await extractors['workflow'].extract_workflow_data(markdown_content, all_json)
        
        # Electronic ID extraction (async)
        all_json = await extractors['electronic_id'].extract_electronic_id_data(markdown_content, all_json)
        
        # KYB extraction (async)
        all_json = await extractors['kyb'].extract_kyb_data(markdown_content, all_json)
        
        # Discount schedule extraction (async)
        all_json = await extractors['discount_schedule'].extract_discount_schedule_data(markdown_content, all_json)
        
        # Validation extraction (synchronous)
        all_json = extractors['validation'].extract_validation_data(all_json, md_text=markdown_content, pdf_path=pdf_file)
        
        # Standard check extraction (synchronous)
        all_json = extractors['standard_check'].extract_standard_check_data(all_json, markdown_content)
        
        return all_json
        
    except Exception as e:
        print(f"❌ Error in data enrichment: {e}")
        raise e


def add_amendment_logs(amendment_logs_extractor, all_json, fileName, start_time):
    """Add amendment logs to the JSON data."""
    try:
        processing_time = time.time() - start_time
        all_json = amendment_logs_extractor.extract_amendment_logs_data(
            all_json, 
            fileName, 
            processing_time
        )
        return all_json
    except Exception as e:
        print(f"❌ Error in amendment processing: {e}")
        raise e


def create_error_result(error_type, error_message):
    """Create error result dictionary."""
    return {
        "status": "error",
        "error_type": error_type,
        "error_message": error_message,
        "timestamp": datetime.now().isoformat()
    } 