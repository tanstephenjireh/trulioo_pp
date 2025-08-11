import boto3
import json
import base64
import pickle
import logging
import asyncio
import pandas as pd
from datetime import datetime, timezone
from amd_overwrite import main

logger = logging.getLogger()
logger.setLevel(logging.INFO)

async def amend_sequential(event, context):
    try:
        # logger.info(f"Received event: {json.dumps(event, indent=2)}")

        s3_client = boto3.client('s3')

        # Extract parameters from Step Functions
        result_location = event.get('resultLocation')
        file_name = event.get('fileName')
        extraction_id = event.get('extraction_id')
        user_id = event.get('user_id')
        
        logger.info(f"Processing result for file: {file_name}")
        logger.info(f"Result location: {result_location}")

        # Parse S3 path
        if not result_location or not result_location.startswith('s3://'):
            raise ValueError(f"Invalid result location: {result_location}")
        
        s3_path = result_location.replace("s3://", "")
        bucket, key = s3_path.split("/", 1)

        ############################ Get the final appended json of an account name
        response = s3_client.get_object(Bucket=bucket, Key=key)

        json_content = response['Body'].read().decode('utf-8')
        all_json = json.loads(json_content)



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
            # print(f"📄 Converted {df_name}: {len(df)} rows")


        ############################# Overwrite the final json with the final appended account name
        # After you've modified your dataframes_dict or json_dataframes


        updated_json = main(
            original_json=json_dataframes,
            new_data=all_json
        )


        
        # Convert JSON back to DataFrames if needed
        updated_dataframes_dict = {}
        for df_name, records in updated_json.items():
            # Convert records back to DataFrame
            updated_dataframes_dict[df_name] = pd.DataFrame(records)

        # Convert back to the original storage format:
        # 1. Pickle the dataframes dictionary
        pickled_data = pickle.dumps(updated_dataframes_dict)

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

        # Return success result
        result = {
            'status': 'success',
            'message': f'Sequential processing completed for {file_name}',
            'fileName': file_name,
            'extraction_id': extraction_id,
            'user_id': user_id,
            'processing_time': 2.0,
            'processed_at': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Sequential processing complete for: {file_name}")
        return result
        
    except Exception as e:
        logger.error(f"Error in sequential processing: {str(e)}")
        
        return {
            'status': 'error',
            'message': str(e),
            'fileName': event.get('fileName', 'unknown'),
            'extraction_id': event.get('extraction_id'),
            'user_id': event.get('user_id'),
            'original_result_location': event.get('resultLocation'),
            'error_timestamp': datetime.utcnow().isoformat()
        }
    
# Lambda wrapper for async handler
def handler(event, context):
    """Wrapper to run async lambda_handler"""
    return asyncio.run(amend_sequential(event, context))