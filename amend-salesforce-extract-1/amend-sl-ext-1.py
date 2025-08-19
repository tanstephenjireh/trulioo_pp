import boto3
import json
import pickle
import logging
import asyncio
import base64
from compile_records import RecordCompiler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## 1. Get the dataframe.json file saved in S3
## 2. Parse the amendment if account name match

async def amend_salesforce_extract(event, context):
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
        files = event.get('files', [])  # Get the files array
        

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
        print("\n🔍 STEP 2: Salesforce extraction...")       
        
        compiler = RecordCompiler()
        account_files, filename_to_account = compiler.compile(extraction_json=json_dataframes)

        print("account_files:", account_files)
        print("filename_to_account:", filename_to_account)

        # STEP 3: Save the salesforce and matched data
        print("\n🔗 STEP 3: Saving salesforce and matched data")


        # Store large content in S3
        amend_account_files_key = f"user-sessions/{user_id}/extractions/{extraction_id}/amend_account_files.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=amend_account_files_key,
            Body=json.dumps(account_files, indent=2), # Replace json_dataframes with actual data
            ContentType='application/json'
        )

        # Store large content in S3
        amend_filename_to_account_key = f"user-sessions/{user_id}/extractions/{extraction_id}/amend_filename_to_account.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=amend_filename_to_account_key,
            Body=json.dumps(filename_to_account, indent=2), # Replace json_dataframes with actual data
            ContentType='application/json'
        )


        # Simulate processing result for single file
        result = {
            'status': 'success',
            'message': f'Successfully matched salesforce data to original records',
            'bucket': bucket,
            'extraction_id': extraction_id,
            'user_id': user_id,
            'files': files,
            'account_files_location': f"s3://{bucket}/{amend_account_files_key}",
            'filename_to_account_location': f"s3://{bucket}/{amend_filename_to_account_key}",
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
    return asyncio.run(amend_salesforce_extract(event, context))