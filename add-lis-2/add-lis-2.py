## add-lis-1.py

import boto3
import json
import pickle
import logging
import asyncio
import base64
from typing import Dict
from datetime import datetime, timezone
from cmpr_lis_add import main as comp_add_lis_main
from io import BytesIO
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## 1. Get the dataframe.json file saved in S3
## 2. Parse the amendment if account name match



def create_excel_file(dataframes: Dict, amendment: bool) -> bytes:
    """Convert dataframes to Excel file"""
    try:
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        if amendment == True:
            # Define sheet mappings
            sheet_mappings = {
                'Contract': 'Contract',
                'Subscription': 'Subscription',
                'LineItemSource': 'LineItemSource',
                'subConsumptionSchedule': 'subConsumptionSchedule',
                'subConsumptionRate': 'subConsumptionRate',
                'lisConsumptionSchedule': 'lisConsumptionSchedule',
                'lisConsumptionRate': 'lisConsumptionRate',
                'discountSchedule': 'discountSchedule',
                'discountRate': 'discountRate',
                'Logs': 'Logs',
                'Amendment Logs': 'Amendment Logs'
            }
        
        elif amendment == False:
            # Define sheet mappings
            sheet_mappings = {
                'Contract': 'Contract',
                'Subscription': 'Subscription',
                'LineItemSource': 'LineItemSource',
                'subConsumptionSchedule': 'subConsumptionSchedule',
                'subConsumptionRate': 'subConsumptionRate',
                'lisConsumptionSchedule': 'lisConsumptionSchedule',
                'lisConsumptionRate': 'lisConsumptionRate',
                'discountSchedule': 'discountSchedule',
                'discountRate': 'discountRate',
                'Logs': 'Logs'
            }
        
        for df_name, sheet_name in sheet_mappings.items():
            if df_name in dataframes:
                df = dataframes[df_name]
                # Fix: Use proper DataFrame checking instead of "is not None"
                if df is not None and hasattr(df, 'empty') and not df.empty:
                    try:
                        df.to_excel(writer, index=False, sheet_name=sheet_name)
                        
                        workbook = writer.book
                        worksheet = writer.sheets[sheet_name]
                        format_num = workbook.add_format({'num_format': '0.00'})
                        worksheet.set_column('A:A', None, format_num)
                        
                        print(f"✅ Added sheet '{sheet_name}' with {len(df)} rows")
                        
                    except Exception as sheet_error:
                        print(f"⚠️ Failed to add sheet '{sheet_name}': {sheet_error}")
                        continue
                else:
                    print(f"⚠️ Skipping '{sheet_name}' - DataFrame is None or empty")

        writer.close()
        processed_data = output.getvalue()
        
        if len(processed_data) == 0:
            raise Exception("Generated Excel file is empty")
            
        print(f"✅ Excel file created successfully, size: {len(processed_data)} bytes")
        return processed_data
        
    except Exception as e:
        print(f"❌ Error creating Excel file: {str(e)}")
        raise Exception(f"Error creating Excel file: {str(e)}")

    except Exception as e:
        print(f"Error creating Excel file: {str(e)}")
        return b""

def generate_excel_file(dataframes: Dict, amendment: bool) -> bytes:
    """Generate Excel file from processed dataframes"""
    try:
        logger.info("📊 Generating Excel file...")
        
        # Convert JSON dataframes back to pandas DataFrames if needed
        if isinstance(dataframes, dict):
            import pandas as pd
            pandas_dataframes = {}
            
            for name, df_data in dataframes.items():
                try:
                    if isinstance(df_data, pd.DataFrame):
                        # Already a DataFrame
                        pandas_dataframes[name] = df_data
                        logger.info(f"✅ Using existing DataFrame for {name}: {len(df_data)} rows")
                    elif df_data is not None and isinstance(df_data, dict) and 'data' in df_data and df_data['data']:
                        # JSON format with data array
                        pandas_dataframes[name] = pd.DataFrame(df_data['data'])
                        logger.info(f"✅ Created DataFrame from JSON for {name}: {len(df_data['data'])} rows")
                    elif df_data is not None and isinstance(df_data, list):
                        # Direct list of records
                        pandas_dataframes[name] = pd.DataFrame(df_data)
                        logger.info(f"✅ Created DataFrame from list for {name}: {len(df_data)} rows")
                    else:
                        logger.warning(f"⚠️ Skipping {name} - invalid format: {type(df_data)}")
                        continue
                except Exception as df_error:
                    logger.error(f"❌ Error processing dataframe {name}: {df_error}")
                    continue
            
            dataframes = pandas_dataframes
        
        # Check if we have any valid dataframes
        if not dataframes:
            raise Exception("No valid dataframes to export")
        
        logger.info(f"📊 Creating Excel file with {len(dataframes)} sheets: {list(dataframes.keys())}")
        
        # Use existing PDF processor to create Excel
        excel_content = create_excel_file(dataframes, amendment)
        
        if not excel_content:
            raise Exception("PDF processor returned empty Excel content")
        
        logger.info(f"✅ Excel file generated successfully, size: {len(excel_content)} bytes")
        return excel_content
        
    except Exception as e:
        logger.error(f"❌ Error generating Excel file: {str(e)}")
        import traceback
        logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        raise Exception(f"Error generating Excel file: {str(e)}")


async def add_lis(event, context):
    """
    Amendment Lambda handler - processes ONE file at a time
    """
    
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        s3_client = boto3.client('s3')

        # Extract single file parameters (not files array)
        bucket = event.get('bucket')
        
        # Extract amendment context
        extraction_id = event.get('extraction_id')
        user_id = event.get('user_id')
        filename = event.get('filename')
        lis_name = event.get('lis_name')
        subscription_name = event.get('subscription_name')

        
        ##### GET json in S3 #####
        
        print("\n📋 STEP 1: Loading JSON data...")

        # Construct the dataframe.json S3 path
        dataframe_path = f"user-sessions/{user_id}/extractions/{extraction_id}/dataframe.json"

        s3_response = s3_client.get_object(
            Bucket=bucket,
            Key=dataframe_path
        )

        # Parse JSON and extract base64 data
        dataframe_json = json.loads(s3_response['Body'].read().decode('utf-8'))
        pickle_base64 = dataframe_json['dataframe_data']

        # Decode base64 and unpickle
        pickle_data = base64.b64decode(pickle_base64)
        dataframes_dict = pickle.loads(pickle_data)

        # 🆕 Convert DataFrames to JSON format
        json_dataframes = {}
        for df_name, df in dataframes_dict.items():
            # Convert DataFrame to JSON (records format)
            json_dataframes[df_name] = df.to_dict('records')

        print("✅ JSON data loaded")

        # STEP 2: Salesforce extraction and Match salesforce file id and created date with all json account names
        comp_result_s3_key = f"user-sessions/{user_id}/extractions/{extraction_id}/comparison_accounts_results.json"

        comparison_response = s3_client.get_object(Bucket=bucket, Key=comp_result_s3_key)
        json_content = comparison_response['Body'].read().decode('utf-8')
        comparison_results = json.loads(json_content)


        check_final_account_names = list(comparison_results.keys())

        # # Check if AmWager exists, then access subscriptions
        # if "AmWager" in comparison_results:
        #     subscriptions_amwager = comparison_results["AmWager"]["subscriptions"]
        # else:
        #     subscriptions_amwager = []



        # STEP 3: Compare accounts pipeline

        # input_comparison_results = comparison_results
        # input_comparison_results_type = type(comparison_results) 

        # input_filename = filename
        # input_filename_type = type(filename)

        # input_data = data
        # input_data_type = type(data)

        # inputs = [
        #     {
        #         "input_comparison_results": input_comparison_results,
        #         "input_comparison_results_type": type(comparison_results).__name__  # Changed
        #     },
        #     {
        #         "input_filename": input_filename,
        #         "input_filename_type": type(filename).__name__  # Changed
        #     },
        #     {
        #         "input_data": input_data,
        #         "input_data_type": type(data).__name__  # Changed
        #     }
        # ]

        new_output_records, new_comparison_records = comp_add_lis_main(
            json_dataframes,      # Pass the dict directly
            subscription_name,   # Pass the dict directly
            filename,
            lis_name,
            comparison_results
        )
        


        # STEP 4: Save the final json and comparison json accounts again

        # 🆕 Convert JSON records back to pandas DataFrames before saving
        import pandas as pd
        pandas_dataframes = {}

        for df_name, records in new_output_records.items():
            if isinstance(records, list) and len(records) > 0:
                pandas_dataframes[df_name] = pd.DataFrame(records)
            elif hasattr(records, 'empty'):  # Already a DataFrame
                pandas_dataframes[df_name] = records
            else:
                pandas_dataframes[df_name] = pd.DataFrame()

        # Now pickle the actual DataFrames
        pickled_data = pickle.dumps(pandas_dataframes)


        # Convert back to the original storage format:
        # 1. Pickle the dataframes dictionary
        # pickled_data = pickle.dumps(new_output_records)

        # 2. Encode as base64
        pickle_base64 = base64.b64encode(pickled_data).decode('utf-8')

        # Store back as JSON with metadata
        dataframe_json = {
            "dataframe_data": pickle_base64,
            "extraction_id": extraction_id,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "file_type": "dataframes",
            "dataframe_count": len(dataframes_dict),
            "dataframe_names": list(dataframes_dict.keys()),
            "size": len(pickle_data)
        }

        s3_client.put_object(
            Bucket=bucket,
            Key=dataframe_path,
            Body=json.dumps(dataframe_json, indent=2),
            ContentType='application/json'
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=comp_result_s3_key,
            Body=json.dumps(new_comparison_records, indent=2),
            ContentType='application/json'
        )






        # STEP 5: Update the Excel file with the new dataframe.json


        # Get the first-level keys (DataFrame names)
        first_level_keys = list(new_output_records.keys())
        print("First-level keys:", first_level_keys)

        if 'Amendment Logs' in first_level_keys:
            amendment = True
        else:
            amendment = False


        excel_content = generate_excel_file(new_output_records, amendment)

        s3_key_excel = f"user-sessions/{user_id}/extractions/{extraction_id}/excel.xlsx"

        excel_base64 = base64.b64encode(excel_content).decode('utf-8')

        # Store as JSON with metadata
        excel_json = {
            "excel_data": excel_base64,
            "extraction_id": extraction_id,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "file_type": "excel",
            "size": len(excel_content)
        }

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key_excel,
            Body=json.dumps(excel_json, indent=2),
            ContentType='application/json'
        )



        # Simulate processing result for single file
        result = {
            'status': 'success',
            'message': f'Successfully compared salesforce data to AI extracted data',
            'bucket': bucket,
            'extraction_id': extraction_id,
            'user_id': user_id,
            'compare_final_data_result': comp_result_s3_key,
            'check_final_account_names': check_final_account_names,
            'processing_time': 12.5,  # Updated to reflect actual time including delay
            'preprocessing_complete': True
        }
        
        logger.info("Preprocessing complete, passing files to next step")
        return result
        
    except Exception as e:
        logger.error(f"Error in preprocessing: {str(e)}")
        return {
            'status': 'error',
            'message': str(e),
            'extraction_id': event.get('extraction_id'),
            'user_id': event.get('user_id')
        }

# Lambda wrapper for async handler
def handler(event, context):
    """Wrapper to run async lambda_handler"""
    return asyncio.run(add_lis(event, context))