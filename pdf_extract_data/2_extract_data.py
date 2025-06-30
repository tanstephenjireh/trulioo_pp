import asyncio
import boto3
import json
import time
import uuid
import logging
from contract_extractor import ContractExtractor
from salesforce import SalesForce
from docv import DocV
from watchlist import Watchlist

logger = logging.getLogger(__name__)

async def extract_data(event, context):  # Make this async
    """
    Process a single PDF file using OpenAI OCR
    """
    
    # Get S3 details from the event
    bucket_name = event['bucket']
    file_key = event['key']
    file_name = event.get('fileName', file_key.split('/')[-1])
    parsed_location = event['parsedLocation']
    parsed_processingTime = event['processingTime']
    
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        logger.info(f"Starting to extract: {file_name}")
        
        # Download the PDF file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        pdf_content = response['Body'].read()
        
        # Initialize important classes
        extractor = ContractExtractor()
        sf = SalesForce()
        doc_v = DocV()
        watchlist = Watchlist()


        # Extract the parsed bucket and key from s3://bucket/key format
        s3_path = parsed_location.replace("s3://", "")
        bucket, key = s3_path.split("/", 1)
        # Download content from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parsed_content = response['Body'].read().decode('utf-8')

        # Delete the file after successful retrieval
        s3_client.delete_object(Bucket=bucket, Key=key)        



        # Process the PDF
        start_time = time.time()
        # If any of these also have async methods, add await
        all_json = await extractor.extract_contract_pipeline(  # Add await if needed
            input_pdf=pdf_content,
            extracted_text=parsed_content,
            file_name=file_name
        )

        output_all_json = sf.main(data=all_json)  # Add await if needed
        
        docv_all_json = doc_v.main(  # Add await if needed
            parsed_input=parsed_content,
            output_all_json=output_all_json
        )
        
        watchlist_all_json = watchlist.main(  # Add await if needed
            parsed_input=parsed_content,
            output_all_json=docv_all_json
        )
        
        processing_time = time.time() - start_time

        # Store large content in S3
        result_key = f"results/{uuid.uuid4()}_{file_name}_processed.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=result_key,
            Body=json.dumps(watchlist_all_json, indent=2),
            ContentType='application/json'
        )

        # Return only small metadata with S3 reference
        results = {
            'fileName': file_name,
            'status': 'success',
            'processingTime': round(processing_time, 2)+parsed_processingTime,
            'resultLocation': f"s3://{bucket_name}/{result_key}",
            'processedAt': context.aws_request_id
        }
        
        logger.info(f"Successfully extracted text from: {file_name}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}")
        return {
            'fileName': file_name,
            'status': 'error',
            'error': str(e)
        }

# Lambda wrapper for async handler
def handler(event, context):
    """Wrapper to run async lambda_handler"""
    return asyncio.run(extract_data(event, context))