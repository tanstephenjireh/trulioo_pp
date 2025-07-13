import json
import logging
import asyncio
from openai import AsyncOpenAI
from config import get_ssm_param
from simple_salesforce import Salesforce


# Configure logging
logger = logging.getLogger(__name__)

class DocV:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.DOCV_INSTRUCTION = get_ssm_param("/myapp/docv_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")

        self.DOCV_INSTRUCTION = """
        You are an intelligent field extractor. You are given a chunk of a document that may contain two relevant sections:
            1. Selected Services and Pricing: Identity Document Verification
            2. Identity Document Verification - Tier Pricing
        If a value is not present or no chunk was provided, return "NA". Do not leave any field blank.

        ## `subscription` (subscription level fields)
        From Section: Selected Services and Pricing: Identity Document Verification
            - `ProductName`: Extract Item Name listed in this section. There is only one ItemName, under the column Item Name
        From Section: Identity Document Verification - Tier Pricing
            - `CurrencyIsoCode`: get the ISO currency code from the “Price Per Query” table. 
                - If "$", it is automatically USD. 

        ##  `scr` (subscription consumption rate for this subscription)      
        From Section: Identity Document Verification - Tier Pricing: For the Item Name above, find the corresponding tier pricing table.
        For each row in the 'Tier Pricing' table, extract:
            - `subCrName`: The name or identifier of the tier (if present, else "NA").
            - `LowerBound__c`: The monthly transaction volume lower bound. "1" if none.
            - `UpperBound__c`: The monthly transaction volume upper bound. 
            - `Price__c`: The value`: from the "Price per Query" column, extract only the value without the currency.
            - `CurrencyIsoCode`: the currency under the "Fee per Query" column. If "$", it is automatically USD. 
            
        Return the extracted data as a structured JSON object, formatted as follows:
        ```json
        {
        "subscription": [
            {
            <Subscription-level fields>,
            "scr": [ ...subscription consumption rate for this subscription... ]
            }
        ]
        }
        ```

        """


    # ========== DOCV EXTRACTION LOGIC ==========
    ## New Function ## <delete the old chunking>
    async def call_llm_for_docv_boundaries(self, full_text):
        """
        Uses GPT-4o to find the start and end line (as exact line text) of the Identity Document Verification section, usually with a header "# Selected Services & Pricing: Identity Document Verification".
        Returns a dict: {"start_line": "...", "end_line": "..."} or None if not found.
        """
        system_prompt = (
            "You are an expert contract parser. "
            "Your job is to find the exact line text that marks the START and END of the Identity Document Verification section in the contract below."
        )
        user_prompt = f"""
    # Instructions:
    ### START OF DOC V ###
    - Find the line that marks the START of the DOCV block. This is the line containing 'Selected Services and Pricing: Identity Document Verification' (match headers like '# Selected Services and Pricing: Identity Document Verification', 'Selected Services and Pricing: Identity Document Verification', etc.).
        - A DOCV block may also be associated with # Identity Document Verification - Tier Pricing, and # Identity Document Verification Tier Pricing Table sections
    ### END OF DOCV
    - Find the line that marks the END of the DOCV block or when the section is no longer about Identity Verficiation or its pricing. 
        - This is the first line AFTER the DOCV section that is clearly a new section (such as '# Selected Services & Pricing: Business Verification', '# Selected Services and Pricing: Workflow Studio', etc.).
    ### Output
    - Output a JSON object with two fields: "start_line" (the exact text of the start line), and "end_line" (the exact text of the end line; use "" if there is no subsequent section).
    - If the DOCV section does not exist, output {{"start_line": "", "end_line": ""}}.

    DOCUMENT:
    ---
    {full_text}
        """
        # Add delay before API call
        await asyncio.sleep(1)

        response = await self.openai.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        data = json.loads(response.choices[0].message.content)
        return data
    ## --end of new function-- ##

    ## New funcion ##
    async def extract_full_docv_chunk_by_llm(self, full_text):
        """
        Uses GPT-4o-mini boundary finder to return the DOCV chunk text (or "" if not found).
        """
        boundaries = await self.call_llm_for_docv_boundaries(full_text)
        start_line = boundaries.get("start_line", "").strip()
        end_line = boundaries.get("end_line", "").strip()

        if not start_line:
            return ""  # No DOCV block found

        # Split document into lines for precise search
        lines = full_text.splitlines()
        try:
            start_idx = next(i for i, line in enumerate(lines) if line.strip() == start_line)
        except StopIteration:
            print(f"Start line not found: {start_line}")
            return ""
        
        if end_line:
            try:
                end_idx = next(i for i, line in enumerate(lines) if line.strip() == end_line)
            except StopIteration:
                print(f"End line not found: {end_line}. Using end of document.")
                end_idx = len(lines)
        else:
            end_idx = len(lines)

        chunk = "\n".join(lines[start_idx:end_idx]).strip()
        return chunk
    ## --end of new function-- ##

    async def extract_docv_consumption_from_llm(self, docv_chunk):
        outputs = []
        if docv_chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(1)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.DOCV_INSTRUCTION},
                        {"role": "user", "content": docv_chunk}
                    ]
                )
                outputs.append(json.loads(response.choices[0].message.content.strip()))
            except Exception as e:
                print(f"Error processing DOCV chunk: {e}")
        else:
            print("No DOCV chunk found.")
        return outputs
    
    def transform_to_flat_records(self, outputs):
        subscriptions = []
        sub_cs = []
        sub_cr = []

        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
                    # Make sure these fields are included!
                    sub_flat = {
                        "subExternalId": subscription.get("subExternalId", ""),
                        "ProductName": subscription.get("ProductName", ""),
                        "ContractExternalId": subscription.get("ContractExternalId", ""),
                        "ContractName": subscription.get("ContractName", ""),
                        "CurrencyIsoCode": subscription.get("CurrencyIsoCode", ""),
                        "SBQQ__SubscriptionStartDate__c": subscription.get("SBQQ__SubscriptionStartDate__c", ""),
                        "ProductId": subscription.get("ProductId", ""),
                        "Note": subscription.get("Note", "")
                    }
                    subscriptions.append(sub_flat)

                    # SUB CONSUMPTION SCHEDULE (one per subscription)
                    sub_cs_flat = {
                        "subCsExternalId": subscription.get("subCsExternalId", ""),
                        "subExternalId": subscription.get("subExternalId", ""),
                        "subscriptionName": subscription.get("ProductName", ""),
                        "subCsName": subscription.get("ProductName", "") + " Direct Consumption Schedule",
                        "CurrencyIsoCode": subscription.get("CurrencyIsoCode", ""),
                        "RatingMethod__c":"Tier",
                        "Type__c": "Range"
                    }
                    sub_cs.append(sub_cs_flat)

                    # SUB CONSUMPTION RATE (from SCR)
                    for i, scr in enumerate(subscription.get("scr", []), 1):
                        sub_cr_flat = {
                            "subCrExternalId": scr.get("subCrExternalId", ""),
                            "subCrName": scr.get("subCrName", ""),
                            "subExternalId": subscription.get("subExternalId", ""),
                            "subscriptionName": subscription.get("ProductName", ""),
                            "CurrencyIsoCode": scr.get("CurrencyIsoCode", ""),
                            "Price__c": scr.get("Price__c", ""),
                            "LowerBound__c": scr.get("LowerBound__c", ""), 
                            "UpperBound__c": scr.get("UpperBound__c", ""), 
                        }
                        sub_cr.append(sub_cr_flat)

        # Format as requested
        return [
            {"name": "Subscription", "data": subscriptions},
            {"name": "subConsumptionSchedule", "data": sub_cs},
            {"name": "subConsumptionRate", "data": sub_cr}
        ]
    
    def merge_into_contract_json(self, contract_json, extracted_output):
        # Build a map for fast lookup of output_records by 'name'
        section_map = {rec["name"]: rec for rec in contract_json.get("output_records", [])}

        for section in extracted_output:
            section_name = section["name"]
            section_data = section["data"]
            if section_name in section_map:
                # Extend the existing section's data
                section_map[section_name]["data"].extend(section_data)
            else:
                # Add new section if not present
                contract_json["output_records"].append({
                    "name": section_name,
                    "data": section_data
                })

        return contract_json
    
    async def main(self, parsed_input, output_all_json):
        if not isinstance(output_all_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")

        # Use markdown_text directly:
        full_text = parsed_input 

        contract_data = output_all_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")


        # 2. Try to query Salesforce (and ALWAYS set ProductId and Note)
        try:
            sf = Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            docv_product_result = sf.query(
                "SELECT Id, CreatedDate FROM Product2 WHERE Family = 'DocV' AND IsActive=true AND ProductCode = 'DOCV2-LGC-DEFAULT' "
                "ORDER BY CreatedDate DESC, ProductCode DESC"
            )
            total = docv_product_result["totalSize"]
            if total > 1:
                docv_product_id = docv_product_result["records"][0]["Id"]
                note = "Duplicate products found. Id obtained from latest CreatedDate"
            elif total == 1:
                docv_product_id = docv_product_result["records"][0]["Id"]
                note = "Successfully Matched"
            else:
                docv_product_id = None
                note = "DocV Product not found"
        except Exception as e:
            docv_product_id = None
            note = "Could not extract ProductId from Salesforce"

        
        ## Updated lines
        # 4. Extract the DOCV chunk
        #docv_chunk = extract_full_docv_chunk(full_text)
        docv_chunk = await self.extract_full_docv_chunk_by_llm(full_text)


        # 5. Get LLM extraction
        #outputs = extract_docv_consumption_from_llm(docv_chunk)
        outputs = await self.extract_docv_consumption_from_llm(docv_chunk)

        ## --end of updated lines 
        # 6. Add custom fields (these will ALWAYS be present)
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    subscription["subExternalId"] = f"docv_sub_{contractExternalId}"
                    subscription["ContractExternalId"] = contractExternalId
                    subscription["ContractName"] = ContractName
                    subscription["SBQQ__SubscriptionStartDate__c"] = StartDate
                    subscription["subCsExternalId"] = f"docv_subcs_{contractExternalId}"
                    subscription["ProductId"] = docv_product_id
                    subscription["Note"] = note
                    if "scr" in subscription:
                        for i, scr in enumerate(subscription["scr"], 1):
                            scr["subCrExternalId"] = f"doc_v_subcr{i}_{contractExternalId}"

        # 7. Transform to requested output structure
        transformed = self.transform_to_flat_records(outputs)

        # 8. Merge extracted data into contract_json
        merged = self.merge_into_contract_json(output_all_json, transformed)

        return merged 