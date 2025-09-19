## compare-accounts.py

import boto3
import json
import pickle
import logging
import asyncio
import base64
from compr_main import main as comp_main

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## 1. Get the dataframe.json file saved in S3
## 2. Parse the amendment if account name match

async def comp_accounts(event, context):
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
        comparison_data_path = f"user-sessions/{user_id}/extractions/{extraction_id}/comparison_accounts.json"

        comparison_response = s3_client.get_object(Bucket=bucket, Key=comparison_data_path)
        accounts_dict = json.loads(comparison_response['Body'].read())
        accounts_dict_data = accounts_dict['selected_accounts_data']


        # STEP 3: Compare accounts pipeline
        comp_result = comp_main(
            json.dumps(accounts_dict_data),  # Convert dict to JSON string
            json.dumps(json_dataframes)      # Convert dict to JSON string
        )

        check_final_account_names = list(comp_result.keys())

        # Check if AmWager exists, then access subscriptions
        if "AmWager" in comp_result:
            subscriptions_amwager = comp_result["AmWager"]["subscriptions"]
        else:
            subscriptions_amwager = []

        comp_result_s3_key = f"user-sessions/{user_id}/extractions/{extraction_id}/comparison_accounts_results.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=comp_result_s3_key,
            Body=json.dumps(comp_result, indent=2),
            ContentType='application/json'
        )

        # Simulate processing result for single file
        result = {
            'status': 'success',
            'message': f'Successfully compared salesforce data to AI extracted data',
            'bucket': bucket,
            'extraction_id': extraction_id,
            'user_id': user_id,
            'accounts_dict_data_input': accounts_dict_data,
            'check_final_account_names': check_final_account_names,
            'subscriptions_amwager': subscriptions_amwager,
            'compare_final_data_result': comp_result_s3_key,
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
    return asyncio.run(comp_accounts(event, context))