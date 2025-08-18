import asyncio
import boto3
import json
import time
import uuid
import logging
import random
from contract_extractor import ContractExtractor
from salesforce import SalesForce
from docv import DocV
from watchlist import Watchlist
from fraud import Fraud
from workflow import Wflow
from kyb import KYB
from electronic_id import ElectronicId
from discount_schedule import DiscountSchedule
from validation import Validation

logger = logging.getLogger(__name__)

async def execute_with_retry(func, *args, max_retries=10, step_name="Unknown"):
    """
    Execute any async function with OpenAI rate limit handling
    """
    base_delay = 1
    max_delay = 60
    
    for attempt in range(max_retries):
        try:
            # Add jitter to spread out requests from concurrent Lambdas
            if attempt > 0:
                jitter = random.uniform(0.5, 1.5)
                delay = min(base_delay * (1.5 ** attempt) * jitter, max_delay)
                logger.info(f"Rate limited in {step_name}, attempt {attempt + 1}/{max_retries}, waiting {delay:.2f} seconds")
                await asyncio.sleep(delay)
            
            # Execute the original function
            result = await func(*args)
            
            # If successful, return the result
            if attempt > 0:
                logger.info(f"Successfully executed {step_name} on attempt {attempt + 1}")
            return result
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit error
            if any(keyword in error_str for keyword in ['rate', '429', 'too many requests', 'rate limit', 'quota']):
                if attempt == max_retries - 1:
                    logger.error(f"Rate limit exceeded after {max_retries} attempts in {step_name}")
                    raise Exception(f"OpenAI rate limit exceeded after {max_retries} attempts in {step_name}. Please try again later.")
                
                logger.warning(f"Rate limited in {step_name} on attempt {attempt + 1}/{max_retries}: {str(e)}")
                continue  # Retry with backoff
            else:
                # Non-rate-limit error, fail immediately
                logger.error(f"Non-rate-limit error in {step_name}: {str(e)}")
                raise e
    
    # This should never be reached, but just in case
    raise Exception(f"Max retries ({max_retries}) exceeded in {step_name}")

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
        fraud = Fraud()
        Workflow = Wflow()
        kyb = KYB()
        electronic_id = ElectronicId()
        discount_schedule = DiscountSchedule()
        validation = Validation()

        # Extract the parsed bucket and key from s3://bucket/key format
        s3_path = parsed_location.replace("s3://", "")
        bucket, key = s3_path.split("/", 1)
        # Download content from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parsed_content = response['Body'].read().decode('utf-8')

        print(f"Parsed content from stage 2: {parsed_content}")

        # Delete the file after successful retrieval
        s3_client.delete_object(Bucket=bucket, Key=key)        

        # Process the PDF
        start_time = time.time()
        
        # Execute each step with retry logic for rate limits
        try:
            print("STEP: ContractSubscription")
            all_json = await execute_with_retry(
                extractor.extract_contract_pipeline_from_md,
                pdf_content,
                parsed_content,
                file_name,
                step_name="ContractSubscription"
            )
        except Exception as e:
            print(f"ERROR in ContractSubscription step: {e}")
            raise

        try:
            print("STEP: SalesforceEnrichment")
            # Note: sf.main is synchronous, so no retry wrapper needed
            output_all_json = sf.main(data=all_json)
        except Exception as e:
            print(f"ERROR in SalesforceEnrichment step: {e}")
            raise
        
        try:
            print("STEP: DOCV")
            docv_all_json = await execute_with_retry(
                doc_v.main,
                parsed_content,
                output_all_json,
                step_name="DOCV"
            )
        except Exception as e:
            print(f"ERROR in DOCV step: {e}")
            raise
        
        try:
            print("STEP: Watchlist")
            watchlist_all_json = await execute_with_retry(
                watchlist.main,
                parsed_content,
                docv_all_json,
                step_name="Watchlist"
            )
        except Exception as e:
            print(f"ERROR in Watchlist step: {e}")
            raise

        try:
            print("STEP: FraudIntelligence")
            fraud_all_json = await execute_with_retry(
                fraud.main,
                parsed_content,
                watchlist_all_json,
                step_name="FraudIntelligence"
            )
        except Exception as e:
            print(f"ERROR in FraudIntelligence step: {e}")
            raise

        try:
            print("STEP: WorkflowStudio")
            workflow_all_json = await execute_with_retry(
                Workflow.main,
                parsed_content,
                fraud_all_json,
                step_name="WorkflowStudio"
            )
        except Exception as e:
            print(f"ERROR in WorkflowStudio step: {e}")
            raise

        try:
            print("STEP: KYB")
            kyb_json = await execute_with_retry(
                kyb.main,
                parsed_content,
                workflow_all_json,
                step_name="KYB"
            )
        except Exception as e:
            print(f"ERROR in KYB step: {e}")
            raise

        try:
            print("STEP: ElectronicID")
            electronic_id_json = await execute_with_retry(
                electronic_id.main,
                parsed_content,
                kyb_json,
                step_name="ElectronicID"
            )
        except Exception as e:
            print(f"ERROR in ElectronicID step: {e}")
            raise

        try:
            print("STEP: Discount Schedule")
            discount_json = await execute_with_retry(
                discount_schedule.main,
                parsed_content,
                electronic_id_json,
                step_name="DiscountSchedule"
            )
        except Exception as e:
            print(f"ERROR in Discount Schedule step: {e}")
            raise

        try:
            print("STEP: Validation")
            # Note: validation.main is synchronous, so no retry wrapper needed
            final_json = validation.main(discount_json, parsed_content, pdf_content)
        except Exception as e:
            print(f"ERROR in Validation step: {e}")
            raise
        
        processing_time = time.time() - start_time

        # Store large content in S3
        result_key = f"results/{uuid.uuid4()}_{file_name}_processed.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=result_key,
            Body=json.dumps(final_json, indent=2),
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