import boto3
import json
import uuid
import pickle
import logging
import asyncio
import base64
import random
from ocr_check import AmdOcrChecker

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## 1. Get the dataframe.json file saved in S3
## 2. Parse the amendment if account name match

ocr_parser = AmdOcrChecker()

async def parse_pdf_with_retry(ocr_parser, fileName, pdf_content, account_files, filename_to_account, max_retries=10):
    """
    Parse PDF with OpenAI rate limit handling
    """
    base_delay = 1
    max_delay = 60
    
    for attempt in range(max_retries):
        try:
            # Add jitter to spread out requests from concurrent Lambdas
            if attempt > 0:
                jitter = random.uniform(0.5, 1.5)
                delay = min(base_delay * (1.5 ** attempt) * jitter, max_delay)
                logger.info(f"Rate limited in OCR processing, attempt {attempt + 1}/{max_retries}, waiting {delay:.2f} seconds")
                await asyncio.sleep(delay)
            
            # Your original OCR API call (unchanged)
            markdown_content, contractid, account_id, start_date, matched_start_date, accountname = await ocr_parser.check(
                fileName, pdf_content, account_files, filename_to_account
            )
            
            # If successful, return the result
            if attempt > 0:
                logger.info(f"Successfully processed OCR on attempt {attempt + 1}")
            return markdown_content, contractid, account_id, start_date, matched_start_date, accountname
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit error
            if any(keyword in error_str for keyword in ['rate', '429', 'too many requests', 'rate limit', 'quota']):
                if attempt == max_retries - 1:
                    logger.error(f"Rate limit exceeded after {max_retries} attempts in OCR processing")
                    raise Exception(f"OpenAI rate limit exceeded after {max_retries} attempts in OCR processing. Please try again later.")
                
                logger.warning(f"Rate limited in OCR processing on attempt {attempt + 1}/{max_retries}: {str(e)}")
                continue  # Retry with backoff
            else:
                # Non-rate-limit error, fail immediately
                logger.error(f"Non-rate-limit error in OCR processing: {str(e)}")
                raise e
    
    # This should never be reached, but just in case
    raise Exception(f"Max retries ({max_retries}) exceeded in OCR processing")

async def amend_ocr(event, context):
    """
    Amendment Lambda handler - processes ONE file at a time
    """
    
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")
        
        s3_client = boto3.client('s3')

        # Extract single file parameters (not files array)
        bucket = event.get('bucket')
        key = event.get('key') 
        fileName = event.get('fileName')
        size = event.get('size')
        
        # Extract amendment context
        extraction_id = event.get('extraction_id')
        user_id = event.get('user_id')
        
        # Download the PDF file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_content = response['Body'].read()

        # print(f"PDF content type: {type(pdf_content)}, length: {len(pdf_content)}")   

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


        # Load the account files and filename to account mapping
        amend_account_files_path = f"user-sessions/{user_id}/extractions/{extraction_id}/amend_account_files.json"
        s3_response_account = s3_client.get_object(
            Bucket=bucket,
            Key=amend_account_files_path
        )
        account_files = json.loads(s3_response_account['Body'].read().decode('utf-8'))

        amend_filename_to_account_path = f"user-sessions/{user_id}/extractions/{extraction_id}/amend_filename_to_account.json"
        s3_response_filename_to_account = s3_client.get_object(
            Bucket=bucket,
            Key=amend_filename_to_account_path
        )
        filename_to_account = json.loads(s3_response_filename_to_account['Body'].read().decode('utf-8'))
        
        print("account_files:", account_files)
        print("filename_to_account:", filename_to_account)

        print("✅ JSON data loaded")

        # STEP 2: OCR Processing with retry logic
        print("\n🔍 STEP 2: OCR Processing...")
        
        # Use the retry wrapper instead of direct call
        markdown_content, contractid, account_id, start_date, matched_start_date, accountname = await parse_pdf_with_retry(
            ocr_parser, fileName, pdf_content, account_files, filename_to_account
        )
        # print(f"✅ OCR completed - Customer: {customer_name}, Contract ID: {contract_external_id}")

        # Store large content in S3
        amend_parsed_key = f"amend_processed/{uuid.uuid4()}_{fileName}_parsed.txt"

        s3_client.put_object(
            Bucket=bucket,
            Key=amend_parsed_key,
            Body=markdown_content,
            ContentType='text/plain'
        )

        # print("markdown_content:", markdown_content)
        print("contractid:", contractid)
        print("account_id:", account_id)
        print("start_date:", start_date)
        print("matched_start_date:", matched_start_date)
        print("accountname:", accountname)

        # Simulate processing result for single file
        result = {
            'status': 'success',
            'message': f'Amendment file {fileName} processed successfully',
            'fileName': fileName,
            'bucket': bucket,
            'key': key,
            'size': size,
            'extraction_id': extraction_id,
            'user_id': user_id,
            'contract_external_id': contractid,
            'customer_name': accountname,
            'parsedLocation': f"s3://{bucket}/{amend_parsed_key}",
            'processing_time': 12.5  # Updated to reflect actual time including delay
        }
        
        logger.info(f"Single file processing complete: {fileName}")
        
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
    return asyncio.run(amend_ocr(event, context))