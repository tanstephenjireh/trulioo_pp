import boto3
import json
import base64
import pickle
import logging
import asyncio
import pandas as pd
from datetime import datetime, timezone
from typing import Dict
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_excel_file(dataframes: Dict) -> bytes:
    """Convert dataframes to Excel file"""
    try:
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        # Define sheet mappings
        sheet_mappings = {
            'Contract': 'Contract',
            'Subscription': 'Subscription',
            'LineItemSource': 'LineItemSource',
            'subConsumptionSchedule': 'subConsumptionSchedule',
            'subConsumptionRate': 'subConsumptionRate',
            'lisConsumptionSchedule': 'lisConsumptionSchedule',
            'lisConsumptionRate': 'lisConsumptionRate',
            'discountSchedule': 'discountSchedule',
            'discountRate': 'discountRate',
            'Logs': 'Logs',
            'Amendment Logs': 'Amendment Logs'
        }
        
        for df_name, sheet_name in sheet_mappings.items():
            if df_name in dataframes:
                df = dataframes[df_name]
                # Fix: Use proper DataFrame checking instead of "is not None"
                if df is not None and hasattr(df, 'empty') and not df.empty:
                    try:
                        df.to_excel(writer, index=False, sheet_name=sheet_name)
                        
                        workbook = writer.book
                        worksheet = writer.sheets[sheet_name]
                        format_num = workbook.add_format({'num_format': '0.00'})
                        worksheet.set_column('A:A', None, format_num)
                        
                        print(f"✅ Added sheet '{sheet_name}' with {len(df)} rows")
                        
                    except Exception as sheet_error:
                        print(f"⚠️ Failed to add sheet '{sheet_name}': {sheet_error}")
                        continue
                else:
                    print(f"⚠️ Skipping '{sheet_name}' - DataFrame is None or empty")

        writer.close()
        processed_data = output.getvalue()
        
        if len(processed_data) == 0:
            raise Exception("Generated Excel file is empty")
            
        print(f"✅ Excel file created successfully, size: {len(processed_data)} bytes")
        return processed_data
        
    except Exception as e:
        print(f"❌ Error creating Excel file: {str(e)}")
        raise Exception(f"Error creating Excel file: {str(e)}")

    except Exception as e:
        print(f"Error creating Excel file: {str(e)}")
        return b""

def generate_excel_file(dataframes: Dict) -> bytes:
    """Generate Excel file from processed dataframes"""
    try:
        logger.info("📊 Generating Excel file...")
        
        # Convert JSON dataframes back to pandas DataFrames if needed
        if isinstance(dataframes, dict):
            import pandas as pd
            pandas_dataframes = {}
            
            for name, df_data in dataframes.items():
                try:
                    if isinstance(df_data, pd.DataFrame):
                        # Already a DataFrame
                        pandas_dataframes[name] = df_data
                        logger.info(f"✅ Using existing DataFrame for {name}: {len(df_data)} rows")
                    elif df_data is not None and isinstance(df_data, dict) and 'data' in df_data and df_data['data']:
                        # JSON format with data array
                        pandas_dataframes[name] = pd.DataFrame(df_data['data'])
                        logger.info(f"✅ Created DataFrame from JSON for {name}: {len(df_data['data'])} rows")
                    elif df_data is not None and isinstance(df_data, list):
                        # Direct list of records
                        pandas_dataframes[name] = pd.DataFrame(df_data)
                        logger.info(f"✅ Created DataFrame from list for {name}: {len(df_data)} rows")
                    else:
                        logger.warning(f"⚠️ Skipping {name} - invalid format: {type(df_data)}")
                        continue
                except Exception as df_error:
                    logger.error(f"❌ Error processing dataframe {name}: {df_error}")
                    continue
            
            dataframes = pandas_dataframes
        
        # Check if we have any valid dataframes
        if not dataframes:
            raise Exception("No valid dataframes to export")
        
        logger.info(f"📊 Creating Excel file with {len(dataframes)} sheets: {list(dataframes.keys())}")
        
        # Use existing PDF processor to create Excel
        excel_content = create_excel_file(dataframes)
        
        if not excel_content:
            raise Exception("PDF processor returned empty Excel content")
        
        logger.info(f"✅ Excel file generated successfully, size: {len(excel_content)} bytes")
        return excel_content
        
    except Exception as e:
        logger.error(f"❌ Error generating Excel file: {str(e)}")
        import traceback
        logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        raise Exception(f"Error generating Excel file: {str(e)}")

async def amend_update_excel(event, context):
    try:
        # logger.info(f"Received event: {json.dumps(event, indent=2)}")
        bucket = "trulioo-contract-extractor"

        s3_client = boto3.client('s3')

        # Extract parameters from Step Functions
        extraction_id = event.get('extraction_id')
        user_id = event.get('user_id')
        



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


        excel_content = generate_excel_file(json_dataframes)

        s3_key = f"user-sessions/{user_id}/extractions/{extraction_id}/excel.xlsx"

        excel_base64 = base64.b64encode(excel_content).decode('utf-8')

        # Store as JSON with metadata
        excel_json = {
            "excel_data": excel_base64,
            "extraction_id": extraction_id,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "file_type": "excel",
            "size": len(excel_content)
        }

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(excel_json, indent=2),
            ContentType='application/json'
        )


        # Return success result
        result = {
            'status': 'success',
            'message': f'Excel file successfully updated',
            'extraction_id': extraction_id,
            'user_id': user_id,
            'processing_time': 2.0,
            'excelLocation': f"s3://{bucket}/{s3_key}"
        }
        
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
    return asyncio.run(amend_update_excel(event, context))