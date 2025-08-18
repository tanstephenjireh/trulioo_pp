import boto3
import json
import uuid
import pickle
import logging
import asyncio
import base64
import time
import random

from amd_main import AmendmentPipeline

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## 1. Get the dataframe.json file saved in S3
## 2. Run the whole extraction pipeline
## 3. Append the new items from appendents to the matched account name

async def run_pipeline_with_retry(amd_main, external_id_file, customer_name_file, markdown_file, pdf_file, fileName, input_json_path, max_retries=10):
    """
    Run amendment pipeline with OpenAI rate limit handling
    """
    base_delay = 1
    max_delay = 60
    
    for attempt in range(max_retries):
        try:
            # Add jitter to spread out requests from concurrent Lambdas
            if attempt > 0:
                jitter = random.uniform(0.5, 1.5)
                delay = min(base_delay * (1.5 ** attempt) * jitter, max_delay)
                logger.info(f"Rate limited in amendment pipeline, attempt {attempt + 1}/{max_retries}, waiting {delay:.2f} seconds")
                await asyncio.sleep(delay)
            
            # Your original pipeline call (unchanged)
            all_json = await amd_main.run_pipeline(
                external_id_file=external_id_file,
                customer_name_file=customer_name_file,
                markdown_file=markdown_file,
                pdf_file=pdf_file,
                fileName=fileName,
                input_json_path=input_json_path
            )
            
            # If successful, return the result
            if attempt > 0:
                logger.info(f"Successfully processed amendment pipeline on attempt {attempt + 1}")
            return all_json
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit error
            if any(keyword in error_str for keyword in ['rate', '429', 'too many requests', 'rate limit', 'quota']):
                if attempt == max_retries - 1:
                    logger.error(f"Rate limit exceeded after {max_retries} attempts in amendment pipeline")
                    raise Exception(f"OpenAI rate limit exceeded after {max_retries} attempts in amendment pipeline. Please try again later.")
                
                logger.warning(f"Rate limited in amendment pipeline on attempt {attempt + 1}/{max_retries}: {str(e)}")
                continue  # Retry with backoff
            else:
                # Non-rate-limit error, fail immediately
                logger.error(f"Non-rate-limit error in amendment pipeline: {str(e)}")
                raise e
    
    # This should never be reached, but just in case
    raise Exception(f"Max retries ({max_retries}) exceeded in amendment pipeline")

async def amend_extraction(event, context):
    """
    Amendment Lambda handler - processes ONE file at a time
    """
    start_time = time.time()

    try:
        
        logger.info(f"Received event: {json.dumps(event, indent=2)}")
        
        s3_client = boto3.client('s3')

        # Extract single file parameters (not files array)
        bucket = event.get('bucket')
        key = event.get('key') 
        fileName = event.get('fileName')
        parsed_location = event['parsedLocation']
        # size = event.get('size')
        
        # Extract amendment context
        extraction_id = event.get('extraction_id')
        user_id = event.get('user_id')
        contract_external_id = event.get('contract_external_id')
        customer_name = event.get('customer_name')
        
        # Download the PDF file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_content = response['Body'].read()
        
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

        # print(json.dumps(json_dataframes["Contract"], indent=4))

        ##### Extraction Pipeline with retry logic #####
        s3_path = parsed_location.replace("s3://", "")
        buckett, key = s3_path.split("/", 1)

        response = s3_client.get_object(Bucket=buckett, Key=key)
        parsed_content = response['Body'].read().decode('utf-8')
        
        # Delete the file after successful retrieval
        s3_client.delete_object(Bucket=buckett, Key=key)  

        amd_main = AmendmentPipeline()
        
        # Use the retry wrapper instead of direct call
        all_json = await run_pipeline_with_retry(
            amd_main,
            contract_external_id,
            customer_name,
            parsed_content,
            pdf_content,
            fileName,
            json_dataframes
        )

        # Store large content in S3
        result_key = f"amend_results/{uuid.uuid4()}_{fileName}_processed.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=result_key,
            Body=json.dumps(all_json, indent=2),
            ContentType='application/json'
        )

        # print("ALL JSON:", all_json)

        # Pipeline completion
        total_time = time.time() - start_time

        # Simulate processing result for single file
        result = {
            'status': 'success',
            'message': f'Amendment file {fileName} extracted successfully',
            'fileName': fileName,
            'extraction_id': extraction_id,
            'user_id': user_id,
            'contract_external_id': contract_external_id,
            'customer_name': customer_name,            
            'resultLocation': f"s3://{bucket}/{result_key}",
            'processing_time': round(total_time, 2)
        }
        
        logger.info(f"Single file processing complete: {fileName}")

        # Pipeline completion
        total_time = time.time() - start_time
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing amendment file: {str(e)}")
        
        return {
            'status': 'error',
            'message': str(e),
            'fileName': event.get('fileName', 'unknown'),
            'extraction_id': event.get('extraction_id'),
            'user_id': event.get('user_id')
        }
    
# Lambda wrapper for async handler
def handler(event, context):
    """Wrapper to run async lambda_handler"""
    return asyncio.run(amend_extraction(event, context))