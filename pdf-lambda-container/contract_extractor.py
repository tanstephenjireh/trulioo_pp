import io
import re
import json
import uuid
import logging
# import requests
from openai import AsyncOpenAI
from datetime import datetime
from config import get_ssm_param
import pandas as pd
import pdfplumber
import asyncio

# Configure logging
logger = logging.getLogger(__name__)

class ContractExtractor:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.SUBSCRIPTION_INSTRUCTIONS = get_ssm_param("/myapp/subscription_prompt")
        self.CONTRACT_INSTRUCTIONS = get_ssm_param("/myapp/parser_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)
    

    # ========== FIELD EXTRACTION LOGIC ==========

#############################################################################################
    def extract_contract_sections(self, text):
        canonical_sections = [
            "Customer Information",
            "Fees and Payment Terms",
            "General Service Fees",
            "General Terms and Conditions"
        ]
        result = {sec: "" for sec in canonical_sections}
        any_header_re = re.compile(r"^# ([^\n:]+):?\s*$", re.MULTILINE)
        all_headers = [(m.start(), m.group(1).strip()) for m in any_header_re.finditer(text)]
        all_headers.append((len(text), None))  # Sentinel
        canon_map = {sec.lower(): sec for sec in canonical_sections}
        for i in range(len(all_headers) - 1):
            start, header = all_headers[i]
            end, _ = all_headers[i + 1]
            if header is not None and header.lower() in canon_map:
                section_name = canon_map[header.lower()]
                section_lines = text[start:end].splitlines()
                if section_lines and section_lines[0].strip().startswith("# "):
                    section_lines = section_lines[1:]
                content = "\n".join(section_lines).strip()
                result[section_name] = content
        return result

    def build_contract_prompt(self, section_map):
        order = [
            "Customer Information",
            "Fees and Payment Terms",
            "General Service Fees",
            "General Terms and Conditions"
        ]
        parts = []
        for section in order:
            content = section_map.get(section, "")
            if content:
                parts.append(f"# {section}\n{content}")
        
        result = "\n\n".join(parts)
        # print(f"Contract prompt result: {result}")

        # logger.info(f"Contract prompt built successfully. Length: {len(result)} characters")
        # logger.debug(f"Full contract prompt: {result}")
        return result

    async def extract_contract_fields(self, text):
        contract_sections = self.extract_contract_sections(text)
        prompt = self.build_contract_prompt(contract_sections)

        # Add delay before API call
        await asyncio.sleep(1)
        response = await self.openai.chat.completions.create(
            model="gpt-4.1",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": self.CONTRACT_INSTRUCTIONS + " Please respond with valid JSON."},
                {"role": "user", "content": prompt}
            ]
        )
        return json.loads(response.choices[0].message.content.strip())
    
###################################################################################################

    def split_by_table_chunks(self, full_text):
        lines = full_text.splitlines()
        chunks = []
        i = 0
        while i < len(lines):
            line = lines[i].replace(" ", "")
            if all(k in line for k in ["|Name", "|Type", "|FeeperQuery", "|Comments"]):
                start = max(0, i - 6)
                chunk_lines = lines[start:]
                table_end = next(
                    (j for j, l in enumerate(chunk_lines[6:], start=6) if not l.strip().startswith("|") and l.strip() != ""),
                    len(chunk_lines)
                )
                chunk = "\n".join(chunk_lines[:table_end]).strip()
                chunks.append(chunk)
                i = start + table_end
            else:
                i += 1
        return chunks

    async def extract_subscriptions(self, text):
        country_blocks = self.split_by_table_chunks(text)
        subscriptions = []
        for idx, block in enumerate(country_blocks):
            try:
                # Add delay before API call
                await asyncio.sleep(1)
                response = await self.openai.chat.completions.create(
                    model="gpt-4.1",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.SUBSCRIPTION_INSTRUCTIONS},
                        {"role": "user", "content": block}
                    ]
                )
                subscriptions.append(json.loads(response.choices[0].message.content.strip()))
            except Exception as e:
                print(f"Error processing block {idx+1}: {e}")
        return subscriptions

    # ========== ENRICHMENT ==========
    def enrich_llm_response(self, llm_response):
        contractExternalId = f"{str(uuid.uuid4())}"
        llm_response["contractExternalId"] = contractExternalId

        subscriptions = llm_response.get("subscriptions", [])
        for i, subscription in enumerate(subscriptions, start=1):
            sub_external_id = f"sub{i}_{contractExternalId}"
            subcs_external_id = f"subcs{i}_{contractExternalId}"
            subcr_external_id = f"subcr{i}_{contractExternalId}"

            subscription["subExternalId"] = sub_external_id
            subscription["subCsExternalId"] = subcs_external_id
            subscription["subCrExternalId"] = subcr_external_id

            non_base_count = 1
            for item in subscription.get("listitemsource", []):
                if item.get("lisName", "").strip() == "Base Configuration":
                    continue
                item["lisExternalId"] = f"lis{non_base_count}_{sub_external_id}"
                item["scsExternalId"] = f"scs{non_base_count}_{sub_external_id}"
                item["scrExternalId"] = f"scr{non_base_count}_{sub_external_id}"
                non_base_count += 1

        contractExternalId = llm_response.get("contractExternalId", "NA")
        # docv_items = llm_response.get("docv", [])

        # for i, docv in enumerate(docv_items, start=1):
        #     # For each docv block (should be only one in most cases)
        #     item_blocks = docv.get("ItemName", [])
        #     for j, block in enumerate(item_blocks, start=1):
        #         docv_external_id = f"docv_{j}_{contractExternalId}"
        #         block["docvExternalId"] = docv_external_id

        #         # Now assign docv_subcr to each tier (as before)
        #         for idx, tier in enumerate(block.get("tier", []), start=1):
        #             docv_subcr = f"docv_subcr_{idx}_{contractExternalId}"
        #             tier["docv_subcr"] = docv_subcr


        return llm_response

    # === CREATION OF DATAFRAMES (NOW RETURNS LISTS OF DICTS) ===
    def create_contract_dataframe(self, llm_response):
        contract_fields = {
            "AccountId": "",
            "AccountName": llm_response.get("AccountName", "NA"),
            "ContractExternalId": llm_response.get("contractExternalId", "NA"),  
            "BillingCity": llm_response.get("BillingCity", "NA"),
            "BillingCountry": llm_response.get("BillingCountry", "NA"),
            "BillingPostalCode": llm_response.get("BillingPostalCode", "NA"),
            "BillingState": llm_response.get("BillingState", "NA"),
            "BillingStreet": llm_response.get("BillingStreet", "NA"),
            "ContractTerm": llm_response.get("ContractTerm", "NA"),
            "CurrencyIsoCode": llm_response.get("CurrencyIsoCode", "NA"),
            "Description": "Test Data Load",
            "Minimum_Monthly__c": llm_response.get("Minimum_Monthly__c", "NA"),
            "OwnerId": "",
            "PaymentMethod__c": llm_response.get("PaymentMethod__c", "NA"),
            "PrepaidCredits__c": llm_response.get("PrepaidCredits__c", "NA"),
            "ImplementationFee": llm_response.get("ImplementationFee", "NA"),
            "RecordTypeId": "",
            "SBQQ__RenewalOpportunity__c": "",
            "SBQQ__RenewalTerm__c": "",
            "ShippingCity": "",
            "ShippingCountry": "",
            "ShippingPostalCode": "",
            "ShippingState": "",
            "ShippingStreet": "",
            "StartDate": llm_response.get("StartDate", "NA"),
            "Status": "",
            "Pricebook2Id": "",
            "SBQQ__AmendmentPricebookId__c": "",
            "SBQQ__AmendmentRenewalBehavior__c": "",
            "SBQQ__DefaultRenewalContactRoles__c": "",
            "SBQQ__DefaultRenewalPartners__c": "",
            "SBQQ__DisableAmendmentCoTerm__c": "",
            "SBQQ__Evergreen__c": "",
            "SBQQ__MasterContract__c": "",
            "SBQQ__PreserveBundleStructureUponRenewals__c": "",
            "SBQQ__RenewalForecast__c": "",
            "SBQQ__RenewalPricebookId__c": "",
            "SBQQ__RenewalQuoted__c": ""
        }
        return pd.DataFrame([contract_fields])


    def create_subscription_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])
        

        records = []
        for entry in subscriptions:

            record = {
                "subExternalId": entry.get("subExternalId", "NA"),
                "ProductName": entry.get("subscriptionName", "NA"),
                "ContractExternalId": llm_response.get("contractExternalId", "NA"),
                "ContractName": llm_response.get("AccountName", "NA"),
                "CurrencyIsoCode": entry.get("CurrencyIsoCode", "NA"),
                "LicenseFee": llm_response.get("LicenseFee", "NA"), ## New change
                "_InvoiceSchedule__c": "",
                "OwnerId": "",
                "SBQQ__Account__c": "",
                "SBQQ__Contract__c": "",
                "SBQQ__Discount__c": "",
                "SBQQ__ListPrice__c": "",
                "SBQQ__Number__c": "",
                "SBQQ__OrderProduct__c": "",
                "SBQQ__PricingMethod__c": "",
                "SBQQ__Product__c": "",
                "SBQQ__ProductSubscriptionType__c": "",
                "SBQQ__Quantity__c": "",
                "SBQQ__SubscriptionEndDate__c": "",
                "SBQQ__SubscriptionStartDate__c": llm_response.get("StartDate", "NA"),
                "SBQQ__BillingFrequency__c": "",
                "SBQQ__BillingType__c": "",
                "SBQQ__Bundle__c": "",
                "SBQQ__Bundled__c": "",
                "SBQQ__ChargeType__c": "",
                "SBQQ__NetPrice__c": "",
                "SBQQ__ProrateMultiplier__c": "",
                "SBQQ__RegularPrice__c": "",
                "SBQQ__RenewalQuantity__c": "",
                "SBQQ__RequiredById__c": "",
                "SBQQ__RequiredByProduct__c": "",
                "SBQQ__SubscriptionPricing__c": "",
                "SBQQ__SubscriptionType__c": ""
            }
            records.append(record)

        return pd.DataFrame(records)

    def create_line_item_source_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])

        records = []
        for sub in subscriptions:
            list_items = sub.get("listitemsource", [])
            for item in list_items:
                if item.get("lisName") == "Base Configuration":
                    continue 

                record = {
                    "lisExternalId": item.get("lisExternalId", "NA"),
                    "lisName": item.get("lisName", "NA"),
                    "subExternalId": sub.get("subExternalId", "NA"),
                    "subscriptionName": sub.get("subscriptionName", "NA"),
                    "BaseAddon__c": item.get("BaseAddon__c", "NA"),
                    "CurrencyIsoCode": item.get("CurrencyIsoCode", "NA"),
                    "Description__c": item.get("Description__c", "NA"),
                    "EndDate__c": "",
                    "Included__c": item.get("Included__c", "NA"),
                    "OwnerId": "",
                    "Product__c": "",
                    "ProductCode__c": "",
                    "SortOrder__c": "",
                    "StartDate__c": llm_response.get("StartDate", "NA"),
                    "Subscription__c": "",
                    "SubscriptionTerm__c": llm_response.get("ContractTerm", "NA")
                }
                records.append(record)

        return pd.DataFrame(records)

    def create_subscription_consumption_schedule_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])

        records = []
        for sub in subscriptions:
            record = {
                "subCsExternalId": sub.get("subCsExternalId", "NA"),
                "subCsName": sub.get("subCsName", "NA"),
                "subExternalId": sub.get("subExternalId", "NA"),
                "subscriptionName": sub.get("subscriptionName", "NA"),
                "CurrencyIsoCode": sub.get("CurrencyIsoCode", "NA"),
                "LineItemSource__c": "",
                "RatingMethod__c": "Tier",
                "Type__c": "Range"
            }
            records.append(record)

        return pd.DataFrame(records)

    def create_subscription_consumption_rate_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])

        records = []
        for sub in subscriptions:
            record = {
                "subCrExternalId": sub.get("subCrExternalId", "NA"),
                "subCrName": sub.get("subCrName", "NA"),
                "subExternalId": sub.get("subExternalId", "NA"),
                "subscriptionName": sub.get("subscriptionName", "NA"),
                "CurrencyIsoCode": sub.get("CurrencyIsoCode", "NA"),
                "OriginalPrice__c": sub.get("OriginalPrice__c", "NA"),
                "Price__c": sub.get("Price__c", "NA"),
                "PricingMethod__c": "PerUnit",
                "ProcessingOrder__c": "1",
                "QuoteLineConsumptionSchedule__c": "",
                "LowerBound__c": "1",
                "UpperBound__c": ""
            }
            records.append(record)

        return pd.DataFrame(records)

    def create_source_consumption_schedule_dataframe(self, enriched_llm_response):
        subscriptions = enriched_llm_response.get("subscriptions", [])
        records = []

        for sub in subscriptions:
            list_items = sub.get("listitemsource", [])

            for item in list_items:
                if item.get("lisName") == "Base Configuration":
                    continue 
                record = {
                    "scsExternalId": item.get("scsExternalId", "NA"),
                    "scsName": item.get("scsName", "NA"),  
                    "subExternalId": sub.get("subExternalId", "NA"),
                    "subscriptionName": sub.get("subscriptionName", "NA"),
                    "lisExternalId": item.get("lisExternalId", "NA"),
                    "lisName": item.get("lisName", "NA"),
                    "CurrencyIsoCode": item.get("CurrencyIsoCode", "NA"),
                    "LineItemSource__c": "",
                    "RatingMethod__c": "Tier",
                    "Type__c": "Range"
                }
                records.append(record)

        return pd.DataFrame(records)

    def create_source_consumption_rate_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])
        records = []

        for sub in subscriptions:
            list_items = sub.get("listitemsource", [])

            for item in list_items:
                if item.get("lisName") == "Base Configuration":
                    continue 
                record = {
                    "scrExternalId": item.get("scrExternalId", "NA"),
                    "scrName": item.get("scrName", "NA"),
                    "lisExternalId": item.get("lisExternalId", "NA"),
                    "lisName": item.get("lisName", "NA"),
                    "subExternalId": sub.get("subExternalId", "NA"),
                    "subscriptionName": sub.get("subscriptionName", "NA"),
                    "CurrencyIsoCode": item.get("CurrencyIsoCode", "NA"),
                    "OriginalPrice__c": "",
                    "Price__c": item.get("Price__c", "NA"),
                    "PricingMethod__c": "PerUnit",
                    "ProcessingOrder__c": "1",
                    "QuoteLineConsumptionSchedule__c": "",
                    "LowerBound__c": "1",
                    "UpperBound__c": ""
                }
                records.append(record)

        return pd.DataFrame(records)

    # === TOP-LEVEL EXTRACTION ===

    async def extract_fields_from_text(self, full_text):
        contract_fields = await self.extract_contract_fields(full_text)
        raw_subs = await self.extract_subscriptions(full_text)
        flattened_subs = []
        for item in raw_subs:
            if isinstance(item, dict) and "subscriptions" in item:
                flattened_subs.extend(item["subscriptions"])
            else:
                print("Warning: Skipping item without 'subscriptions' key:", item)
        contract_fields["subscriptions"] = flattened_subs
        # docv_chunk = extract_full_docv_chunk(full_text)
        # docv_fields = extract_docv_consumption_from_llm(docv_chunk)
        # contract_fields["docv"] = docv_fields
        return contract_fields
    
    # def extract_fields_from_text(self, full_text, url_contract, jwt_token):
    #     # Initialize contract_fields with default values
    #     contract_fields = {}
        
    #     # Use API for contract fields
    #     headers = {
    #         "Content-Type": "application/json",
    #         "Authorization": f"Bearer {jwt_token}"
    #     }

    #     # Request payload
    #     payload = {
    #         "text": full_text
    #     }
        
    #     try:
    #         response = requests.post(url_contract, headers=headers, json=payload)
    #         # st.write("trying the api...")
    #         if response.status_code == 200:
    #             contract_fields = response.json()
    #             # st.write(contract_fields)
    #             # st.write("api_successful...")


    #             raw_subs = self.extract_subscriptions(full_text)
    #             flattened_subs = []
    #             for item in raw_subs:
    #                 if isinstance(item, dict) and "subscriptions" in item:
    #                     flattened_subs.extend(item["subscriptions"])
    #                 else:
    #                     print("Warning: Skipping item without 'subscriptions' key:", item)
    #             contract_fields["subscriptions"] = flattened_subs
    #             return contract_fields
    #             # st.write(contract_fields)
    #             # st.write("api_successful...")
    #         else:
    #             logger.error(f"API Error {response.status_code}: {response.text}")
    #             st.error(f"API Error {response.status_code}: {response.text}")
    #             st.stop()  # Gracefully stops Streamlit execution
    #             # Keep the empty dict as fallback
                
    #     except requests.exceptions.RequestException as e:
    #         logger.error(f"Request failed: {e}")
    #         # Keep the empty dict as fallback


    #     contract_fields = self.extract_contract_fields(full_text)

    #==============VALIDATION=======================
    def get_lineitemsource_count(self, pdf):
        pm_list = []

        with pdfplumber.open(io.BytesIO(pdf)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                pm_list.append(tables)

        keywords = {'Base', 'In Base', 'In base', 'Additional', 'In\nAdditional', 'In Additional', 'In additional'}
        filtered_rows = []

        for plist in pm_list:
            if len(plist) > 0:
                for pst in plist:
                    for pt in pst:
                        if any(item in keywords for item in pt):
                            if len(pt) >= 4:
                                if pt[0] != "Base Configuration" and pt[0] != "Identity Document Verification - Verification with Face\nBiometrics" and pt[0] != "Identity Document Verification - Verification\nwith Face Biometrics" and pt[0] != "Identity Document Verification - Verification with\nFace Biometrics":
                                    filtered_rows.append(pt)

        return filtered_rows

    def extract_subscription_rows(self, pdf):
        pm_list = []
        with pdfplumber.open(io.BytesIO(pdf)) as pdf_obj:
            for page in pdf_obj.pages:
                tables = page.extract_tables()
                pm_list.append(tables)
        subscription_rows = []
        for plist in pm_list:
            if len(plist) > 0:
                for pst in plist:
                    for pt in pst:
                        pt_cleaned = [item.replace('\n', ' ') if item else item for item in pt]
                        if any("Comments" in (item or "") for item in pt_cleaned):
                            subscription_rows.append(pt_cleaned)
        return subscription_rows

    #==============Main Pipeline===============

    async def extract_contract_pipeline(self, input_pdf, extracted_text, file_name):
        """
        Takes a PDF path as input, parses it, extracts contract & subscription fields,
        enriches them, and returns a single JSON object containing only the requested fields.
        """
        # result = self.extract_fields_from_text(full_text=extracted_text, url_contract=url_contract, jwt_token=jwt_token)
        result = await self.extract_fields_from_text(full_text=extracted_text)


        lineitem_rows = self.get_lineitemsource_count(input_pdf)
        comments_rows = self.extract_subscription_rows(input_pdf)
        actual_lis_cnt = len(lineitem_rows)
        actual_sub_cnt = len(comments_rows)

        if not result:
            return {
                "TimeStamp": datetime.now().isoformat(),
                "FileName": file_name,
                "Contractid": "",
                "AccountName": "",
                "ActualSubCnt": "",
                "ActualLisCnt": "",
                "ExtractedSubCnt": "",
                "ExtractedLisCnt":"",
                "MatchedSubCnt": "",
                "MatchedLisCnt": "",
                "%Accuracy": "",
                "output_records": []
            }

        enriched_result = self.enrich_llm_response(result)

        Contract = self.create_contract_dataframe(enriched_result)
        Subscription = self.create_subscription_dataframe(enriched_result)
        LineItemSource = self.create_line_item_source_dataframe(enriched_result)
        subConsumptionSchedule = self.create_subscription_consumption_schedule_dataframe(enriched_result)
        subConsumptionRate = self.create_subscription_consumption_rate_dataframe(enriched_result)
        lisConsmptionSchedule = self.create_source_consumption_schedule_dataframe(enriched_result)
        lisConsumptionRate = self.create_source_consumption_rate_dataframe(enriched_result)

        output_json = {
            "TimeStamp": datetime.now().isoformat(),
            "FileName": file_name,
            "Contractid": enriched_result.get("contractExternalId", ""),
            "AccountName": enriched_result.get("AccountName", ""),
            "ActualSubCnt": actual_sub_cnt,
            "ExtractedSubCnt": "",    
            "MatchedSubCnt": "",       
            "% Sub Extraction Rate": "",            
            "% Sub Matching Rate": "",              
            "ActualLisCnt": actual_lis_cnt,
            "ExtractedLisCnt":"",
            "MatchedLisCnt": "",
            "% LIS Extraction Rate": "",        
            "% LIS Matching Rate": "",
            "output_records": [
                {"name": "Contract", "data": Contract.to_dict(orient="records")},
                {"name": "Subscription", "data": Subscription.to_dict(orient="records")},
                {"name": "LineItemSource", "data": LineItemSource.to_dict(orient="records")},
                {"name": "subConsumptionSchedule", "data": subConsumptionSchedule.to_dict(orient="records")},
                {"name": "subConsumptionRate", "data": subConsumptionRate.to_dict(orient="records")},
                {"name": "lisConsmptionSchedule", "data": lisConsmptionSchedule.to_dict(orient="records")},
                {"name": "lisConsumptionRate", "data": lisConsumptionRate.to_dict(orient="records")}
            ]
        }

        return output_json