import streamlit as st
import boto3
import json
import time
import uuid
from datetime import datetime
import pandas as pd
from io import BytesIO
from collections import defaultdict

# AWS Configuration
AWS_REGION = 'ap-southeast-1'  # Change to your region
S3_BUCKET = 'dev-test-pp'  # Change to your bucket
STEP_FUNCTION_ARN = 'arn:aws:states:ap-southeast-1:387082968990:stateMachine:pdf-processor-state-machine'  # Change to your Step Function ARN

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
stepfunctions_client = boto3.client('stepfunctions', region_name=AWS_REGION)

def upload_files_to_s3(uploaded_files):
    """Upload files to S3 and return their details"""
    uploaded_file_details = []
    
    for uploaded_file in uploaded_files:
        # Create unique file key
        file_key = f"uploads/{uuid.uuid4()}_{uploaded_file.name}"
        
        try:
            # Upload to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Body=uploaded_file.getvalue(),
                ContentType='application/pdf'
            )
            
            uploaded_file_details.append({
                'bucket': S3_BUCKET,
                'key': file_key,
                'fileName': uploaded_file.name,
                'size': len(uploaded_file.getvalue())
            })
            
        except Exception as e:
            st.error(f"Failed to upload {uploaded_file.name}: {str(e)}")
    
    return uploaded_file_details

def start_step_function_execution(file_details):
    """Start Step Function execution with file details"""
    execution_name = f"pdf-processing-{uuid.uuid4()}"
    
    # Prepare input for Step Function
    step_function_input = {
        "files": file_details
    }
    
    try:
        response = stepfunctions_client.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps(step_function_input)
        )
        
        return response['executionArn']
    
    except Exception as e:
        st.error(f"Failed to start Step Function execution: {str(e)}")
        return None

def cleanup_s3_files(file_details):
    """Delete uploaded files from S3"""
    try:
        for file_detail in file_details:
            s3_client.delete_object(
                Bucket=file_detail['bucket'],
                Key=file_detail['key']
            )
        return True
    except Exception as e:
        st.warning(f"Failed to cleanup some files: {str(e)}")
        return False

def check_execution_status(execution_arn):
    """Check the status of Step Function execution"""
    try:
        response = stepfunctions_client.describe_execution(
            executionArn=execution_arn
        )
        
        return {
            'status': response['status'],
            'output': response.get('output'),
            'startDate': response['startDate'],
            'stopDate': response.get('stopDate')
        }
    
    except Exception as e:
        st.error(f"Failed to check execution status: {str(e)}")
        return None
    
def to_excel(df_contract, df_subscription, 
             df_lineitemsource, 
             df_subconsumptionschedule, 
             df_subconsumptionrate, 
             df_lisconsmptionschedule, 
             df_lisconsumptionrate,
             df_logs):
    """Convert dataframes to Excel file"""
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Contract
    df_contract.to_excel(writer, index=False, sheet_name='Contract')
    workbook_1 = writer.book
    worksheet_1 = writer.sheets['Contract']
    format1 = workbook_1.add_format({'num_format': '0.00'}) 
    worksheet_1.set_column('A:A', None)

    # Subscription
    df_subscription.to_excel(writer, index=False, sheet_name='Subscription')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['Subscription']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    # Subscription
    df_lineitemsource.to_excel(writer, index=False, sheet_name='LineItemSource')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['LineItemSource']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    # Subscription
    df_subconsumptionschedule.to_excel(writer, index=False, sheet_name='subConsumptionSchedule')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['subConsumptionSchedule']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    # Subscription
    df_subconsumptionrate.to_excel(writer, index=False, sheet_name='subConsumptionRate')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['subConsumptionRate']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    # Subscription
    df_lisconsmptionschedule.to_excel(writer, index=False, sheet_name='lisConsmptionSchedule')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['lisConsmptionSchedule']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    # Subscription
    df_lisconsumptionrate.to_excel(writer, index=False, sheet_name='lisConsumptionRate')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['lisConsumptionRate']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    df_logs.to_excel(writer, index=False, sheet_name='Logs')
    workbook_2 = writer.book
    worksheet_2 = writer.sheets['Logs']
    format2 = workbook_2.add_format({'num_format': '0.00'}) 
    worksheet_2.set_column('A:A', None, format2)

    writer._save()
    processed_data = output.getvalue()
    return processed_data


def main():
    st.title("🔄 Parallel PDF Processor")
    st.write("Upload multiple PDF files and process them simultaneously using AWS Step Functions!")
    
    # File uploader
    uploaded_files = st.file_uploader(
        "Choose PDF files",
        accept_multiple_files=True,
        type=['pdf'],
        help="Select up to 10 PDF files to process in parallel"
    )
    
    if uploaded_files:
        st.write(f"📁 {len(uploaded_files)} files selected:")
        for file in uploaded_files:
            st.write(f"- {file.name} ({file.size} bytes)")
        
        # if len(uploaded_files) > 10:
        #     st.warning("⚠️ Please select no more than 10 files for optimal performance.")
        #     return
        
        # Process button
        if st.button("🚀 Start Parallel Processing", type="primary"):
            with st.spinner("Uploading files to S3..."):
                # Upload files to S3
                file_details = upload_files_to_s3(uploaded_files)
                
                if not file_details:
                    st.error("Failed to upload files. Please try again.")
                    return
                
                st.success(f"✅ Successfully uploaded {len(file_details)} files to S3!")
            
            with st.spinner("Starting Step Function execution..."):
                # Start Step Function execution
                execution_arn = start_step_function_execution(file_details)
                
                if not execution_arn:
                    st.error("Failed to start processing. Please try again.")
                    return
                
                st.success("✅ Step Function execution started!")
                st.info(f"Execution ARN: {execution_arn}")
            
            # Progress tracking
            st.write("### 📊 Processing Progress")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Poll for execution status
            start_time = time.time()
            while True:
                execution_status = check_execution_status(execution_arn)
                
                if not execution_status:
                    break
                
                status = execution_status['status']
                elapsed_time = time.time() - start_time
                
                status_text.text(f"Status: {status} | Elapsed time: {elapsed_time:.1f}s")
                
                if status == 'RUNNING':
                    # Estimate progress (this is a rough estimation)
                    estimated_progress = min(elapsed_time / (len(uploaded_files) * 3), 0.9)
                    progress_bar.progress(estimated_progress)
                    time.sleep(2)  # Check every 2 seconds
                
                elif status == 'SUCCEEDED':
                    progress_bar.progress(1.0)
                    status_text.text("✅ Processing completed successfully!")
                    
                    # Clean up uploaded files
                    with st.spinner("Cleaning up temporary files..."):
                        if cleanup_s3_files(file_details):
                            st.success("🗑️ Temporary files cleaned up successfully!")
                        else:
                            st.warning("⚠️ Some temporary files may not have been deleted")
                    
                    # Display results
                    if execution_status['output']:
                        st.write("### 📋 Results")
                        results = json.loads(execution_status['output'])
                        
                        # Initialize data_collections AND logs ONCE, outside the loop
                        data_collections = defaultdict(list)
                        logs = {
                            "TimeStamp": [],
                            "FileName": [],
                            "Contractid": [],
                            "AccountName": [],
                            "TotalRunTime (s)": [],
                            "ActualSubCnt": [],
                            "ExtractedSubCnt": [],
                            "MatchedSubCnt": [],
                            "% Sub Extraction Rate": [],
                            "% Sub Matching Rate": [],
                            "ActualLisCnt": [],
                            "ExtractedLisCnt": [],
                            "MatchedLisCnt": [],
                            "% LIS Extraction Rate": [],
                            "% LIS Matching Rate": []
                        }

                        for i, result in enumerate(results, 1):
                            # Handle both old format (with body wrapper) and new format (direct)
                            if 'body' in result:
                                result_data = result['body']
                            else:
                                result_data = result

                            # with st.expander("📊 Preview Excel File", expanded=True):
                            #     st.write(result_data.get('processingTime', 'N/A'))

                            # Collect data for each name
                            output_records = result_data.get('extractedContent', {}).get('output_records', [])
                            for item in output_records:
                                name = item['name']
                                item_data = item['data']
                                data_collections[name].extend(item_data)

                                

                            # Collect logs data for this iteration
                            response = result_data.get('extractedContent', {})

                            
                            
                            logs["TimeStamp"].append(response.get("TimeStamp", "N/A"))
                            logs["FileName"].append(response.get("FileName", "N/A"))
                            logs["Contractid"].append(response.get("Contractid", "N/A"))
                            logs["AccountName"].append(response.get("AccountName", "N/A"))
                            logs["TotalRunTime (s)"].append(result_data.get('processingTime', 'N/A'))
                            logs["ActualSubCnt"].append(response.get("ActualSubCnt", "N/A"))
                            logs["ExtractedSubCnt"].append(response.get("ExtractedSubCnt", "N/A"))
                            logs["MatchedSubCnt"].append(response.get("MatchedSubCnt", "N/A"))
                            logs["% Sub Extraction Rate"].append(response.get("% Sub Extraction Rate", "N/A"))
                            logs["% Sub Matching Rate"].append(response.get("% Sub Matching Rate", "N/A"))
                            logs["ActualLisCnt"].append(response.get("ActualLisCnt", "N/A"))
                            logs["ExtractedLisCnt"].append(response.get("ExtractedLisCnt", "N/A"))
                            logs["MatchedLisCnt"].append(response.get("MatchedLisCnt", "N/A"))
                            logs["% LIS Extraction Rate"].append(response.get("% LIS Extraction Rate", "N/A"))
                            logs["% LIS Matching Rate"].append(response.get("% LIS Matching Rate", "N/A"))

                        # After the loop, create DataFrames
                        dataframes = {}
                        for name, collected_data in data_collections.items():
                            if collected_data:
                                dataframes[name] = pd.DataFrame(collected_data)

                        # Access individual DataFrames
                        df_contract = dataframes.get('Contract')
                        df_subscription = dataframes.get('Subscription')
                        df_lineitemsource = dataframes.get('LineItemSource')
                        df_subconsumptionschedule = dataframes.get('subConsumptionSchedule')
                        df_subconsumptionrate = dataframes.get('subConsumptionRate')
                        df_lisconsmptionschedule = dataframes.get('lisConsmptionSchedule')
                        df_lisconsumptionrate = dataframes.get('lisConsumptionRate')
                        df_logs = pd.DataFrame(logs)

                            
                            # if result_data.get('status') == 'success':
                            #     st.success("✅ Processed successfully")



                            #     res = result_data.get('extractedContent', 'N/A')['output_records']
                            #     st.write(res)

                            #     st.write(logs)

                            #     df_contract = pd.DataFrame(res[0]["data"])
                            #     df_subscription = pd.DataFrame(res[1]["data"])
                            #     df_lineitemsource = pd.DataFrame(res[2]["data"])
                            #     df_subconsumptionschedule = pd.DataFrame(res[3]["data"])
                            #     df_subconsumptionrate = pd.DataFrame(res[4]["data"])
                            #     df_lisconsmptionschedule = pd.DataFrame(res[5]["data"])
                            #     df_lisconsumptionrate = pd.DataFrame(res[6]["data"])


                        df_xlsx = to_excel(
                            df_contract=df_contract, 
                            df_subscription=df_subscription,
                            df_lineitemsource=df_lineitemsource,
                            df_subconsumptionschedule=df_subconsumptionschedule,
                            df_subconsumptionrate=df_subconsumptionrate,
                            df_lisconsmptionschedule=df_lisconsmptionschedule,
                            df_lisconsumptionrate=df_lisconsumptionrate,
                            df_logs=df_logs
                            )

                        # Store DataFrames in session state
                        st.session_state.df_contract = df_contract
                        st.session_state.df_subscription = df_subscription
                        st.session_state.df_lineitemsource = df_lineitemsource
                        st.session_state.df_subconsumptionschedule = df_subconsumptionschedule
                        st.session_state.df_subconsumptionrate = df_subconsumptionrate
                        st.session_state.df_lisconsmptionschedule = df_lisconsmptionschedule
                        st.session_state.df_lisconsumptionrate = df_lisconsumptionrate
                        st.session_state.df_logs = df_logs

                        # Store the Excel data in session state
                        st.session_state.google_extraction_results = df_xlsx

                            #     if st.session_state.google_extraction_results:
                            #         st.download_button(
                            #             label='📥 Download Result',
                            #             data=st.session_state.google_extraction_results,
                            #             file_name='Legacy Data Line Items.xlsx',
                            #             use_container_width=True
                            #         ) 

                        # Display results
                        if st.session_state.google_extraction_results:
                            # Show preview of data
                            with st.expander("📊 Preview Excel File", expanded=True):
                                tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
                                    "Contract Data", 
                                    "Subscription Data",
                                    "Line Item Source",
                                    "Sub Consumption Schedule", 
                                    "Sub Consumption Rate",
                                    "LIS Consumption Schedule",
                                    "LIS Consumption Rate",
                                    "Logs"
                                ])
                                
                                with tab1:
                                    if 'df_contract' in st.session_state:
                                        st.dataframe(st.session_state.df_contract, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")
                                with tab2:
                                    if 'df_subscription' in st.session_state:
                                        st.dataframe(st.session_state.df_subscription, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")   
                                with tab3:
                                    if 'df_lineitemsource' in st.session_state:
                                        st.dataframe(st.session_state.df_lineitemsource, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")   
                                with tab4:
                                    if 'df_subconsumptionschedule' in st.session_state:
                                        st.dataframe(st.session_state.df_subconsumptionschedule, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")   
                                with tab5:
                                    if 'df_subconsumptionrate' in st.session_state:
                                        st.dataframe(st.session_state.df_subconsumptionrate, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")   
                                with tab6:
                                    if 'df_lisconsmptionschedule' in st.session_state:
                                        st.dataframe(st.session_state.df_lisconsmptionschedule, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")   
                                with tab7:
                                    if 'df_lisconsumptionrate' in st.session_state:
                                        st.dataframe(st.session_state.df_lisconsumptionrate, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")  
                                with tab8:
                                    if 'df_logs' in st.session_state:
                                        st.dataframe(st.session_state.df_logs, use_container_width=True, height=400)
                                    else:
                                        st.write("No data available")    

                    else:
                        st.error(f"❌ Processing failed: {result_data.get('error', 'Unknown error')}")
                    break
                
                elif status == 'FAILED':
                    progress_bar.progress(0)
                    status_text.text("❌ Processing failed!")
                    st.error("Step Function execution failed. Please check the logs.")
                    break
                
                elif status == 'TIMED_OUT':
                    progress_bar.progress(0)
                    status_text.text("⏰ Processing timed out!")
                    st.error("Step Function execution timed out.")
                    break

if __name__ == "__main__":
    main()