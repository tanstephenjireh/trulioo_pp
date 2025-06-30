import asyncio
import boto3
import time
import uuid
import logging
from pdf_parser import PDFParser

logger = logging.getLogger(__name__)

async def process_pdf(event, context):  # Make this async
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
        
        # Process the PDF
        start_time = time.time()
        parsed_content = await pdf_parser.parse_pdf_from_s3(pdf_content)  # Add await
        
        processing_time = time.time() - start_time

        # Store large content in S3
        result_key = f"processed/{uuid.uuid4()}_{file_name}_parsed.txt"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=result_key,
            Body=parsed_content,
            ContentType='text/plain'
        )

        # Return only small metadata with S3 reference
        results = {
            'fileName': file_name,
            'status': 'success',
            'bucket': bucket_name,
            'key': file_key,
            'parsedLocation': f"s3://{bucket_name}/{result_key}",
            'processingTime': round(processing_time, 2),
            'stage': 'text_extraction_complete'
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
    return asyncio.run(process_pdf(event, context))