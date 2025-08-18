import asyncio
import boto3
import time
import uuid
import logging
import random
from pdf_parser import PDFParser

logger = logging.getLogger(__name__)

async def process_pdf_with_retry(pdf_parser, pdf_content, max_retries=10):
    """
    Process PDF with OpenAI rate limit handling
    """
    base_delay = 1
    max_delay = 60
    
    for attempt in range(max_retries):
        try:
            # Add jitter to spread out requests from concurrent Lambdas
            if attempt > 0:
                jitter = random.uniform(0.5, 1.5)
                delay = min(base_delay * (1.5 ** attempt) * jitter, max_delay)
                logger.info(f"Rate limited, attempt {attempt + 1}/{max_retries}, waiting {delay:.2f} seconds")
                await asyncio.sleep(delay)  # Use asyncio.sleep for async function
            
            # Your original OpenAI API call (unchanged)
            parsed_content = await pdf_parser.parse_pdf(pdf_content)
            
            # If successful, return the result
            logger.info(f"Successfully processed PDF on attempt {attempt + 1}")
            return parsed_content
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit error
            if any(keyword in error_str for keyword in ['rate', '429', 'too many requests', 'rate limit', 'quota']):
                if attempt == max_retries - 1:
                    logger.error(f"Rate limit exceeded after {max_retries} attempts")
                    raise Exception(f"OpenAI rate limit exceeded after {max_retries} attempts. Please try again later.")
                
                logger.warning(f"Rate limited on attempt {attempt + 1}/{max_retries}: {str(e)}")
                continue  # Retry with backoff
            else:
                # Non-rate-limit error, fail immediately (don't retry for other errors)
                logger.error(f"Non-rate-limit error occurred: {str(e)}")
                raise e
    
    # This should never be reached, but just in case
    raise Exception(f"Max retries ({max_retries}) exceeded")

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
        
        # Process the PDF with retry logic
        start_time = time.time()
        
        # Use the new retry wrapper instead of direct call
        parsed_content = await process_pdf_with_retry(pdf_parser, pdf_content)
        print(f"Parsed content from stage 1: {parsed_content}")

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