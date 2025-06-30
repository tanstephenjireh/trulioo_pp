import asyncio
import boto3
import time
import uuid
import json
import logging
from urllib.parse import unquote_plus
from pdf_parser import PDFParser
from contract_extractor import ContractExtractor
from salesforce import SalesForce
from docv import DocV
from watchlist import Watchlist

logger = logging.getLogger(__name__)

async def lambda_handler(event, context):  # Make this async
    """
    Process a single PDF file using OpenAI OCR
    """
    
    # Get S3 details from the event
    bucket_name = event['bucket']
    file_key = event['key']
    file_name = event.get('fileName', file_key.split('/')[-1])
    
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        logger.info(f"Starting to process: {file_name}")
        
        # Download the PDF file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        pdf_content = response['Body'].read()
        
        # Initialize important classes
        pdf_parser = PDFParser()
        extractor = ContractExtractor()
        sf = SalesForce()
        doc_v = DocV()
        watchlist = Watchlist()
        
        # Process the PDF
        start_time = time.time()
        parsed_content = await pdf_parser.parse_pdf_from_s3(pdf_content)  # Add await
        
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
            'processingTime': round(processing_time, 2),
            'resultLocation': f"s3://{bucket_name}/{result_key}",
            'contentSize': len(json.dumps(watchlist_all_json)),
            'processedAt': context.aws_request_id
        }
        
        logger.info(f"Successfully processed: {file_name}")
        
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
    return asyncio.run(lambda_handler(event, context))