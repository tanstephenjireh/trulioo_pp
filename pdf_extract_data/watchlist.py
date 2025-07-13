import re
import json
import logging
import asyncio
from openai import AsyncOpenAI
from config import get_ssm_param
from simple_salesforce import Salesforce

# Configure logging
logger = logging.getLogger(__name__)

class Watchlist:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.WATCHLIST_INSTRUCTION = get_ssm_param("/myapp/watchlist_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")

        self.WATCHLIST_INSTRUCTION = """
        You are an intelligent field extractor. You are given a chunk of a document that may contain two relevant sections:
            1. Selected Services and Pricing: Watchlist
            2. Watchlist Tier Pricing
        If a value is not present or no chunk was provided, return "NA". Do not leave any field blank.

        ## `subscription` (subscription level fields).
        Do this for each Name. One Name is one Subscription. 
        From Section: Selected Services and Pricing: Watchlist
            - `ProductName`: Extract the Name listed in this section. 
            - `CurrencyIsoCode`: get the ISO currency code from the “Price Per Query” column or From Section: Watchlist Tier Pricing whicever is the price of the Name. 
                - If "$", it is automatically USD. 
                - NA if there is no price.
                
        ##  `scr` (subscription consumption rate for this subscription)      
        For the Item Name above, find the corresponding prices. It may be dependent on tier pricing table under Watchlist Tier Pricing: .
            - `subCrName`: The name or identifier of the price. 
                - Assigned "<Name> Consumption Rate" if the price is not tiered.
                - Assigned "<Name Tier <<n>> Consumption Rate" if the price is tiered.
            - `LowerBound__c`: The monthly volume lower bound for tiered. "1" if none.
            - `UpperBound__c`: The monthly volume upper bound for tiered. NA if none.
            - `Price__c`: The value`: 
                - from the "Price per Query" column for tiered, 
                - from the "Fee per Qeuery" if non tiered.
                - extract only the value without the currency.
            - `CurrencyIsoCode`: get the ISO currency code from the “Price Per Query” column or From Section: Watchlist Tier Pricing whicever is the price of the Name. 
                - If "$", it is automatically USD. 
                - NA if there is no price.
            
        Return the extracted data as a structured JSON object, formatted as follows:
        ```json
        {
        "subscription": [
            {
            <Subscription1-level fields>,
            "scr": [ ...subscription consumption rate for this subscription... ]
            },
            {
            <Subscription2-level fields>,
            "scr": [ ...subscription consumption rate for this subscription... ]
            },
        ]
        }
        ```

        """

    async def call_llm_for_watchlist_boundaries(self, full_text):
        system_prompt = (
            "You are an expert contract parser. "
            "Your job is to find the exact line text that marks the START and END of the Watchlist section in the contract below."
        )
        user_prompt = f"""
    # Instructions:
    ### START OF WATCHLIST ###
    - Find the line that marks the START of the Watchlist block. This may be a header containing ' # Selected Services and Pricing: Watchlist' (or similar such as "# Selected Services & Pricing: Watchlist". There may be slight naming variations such as but not limited to spacing and ampersand). 
        - A Watchlist block may also be associated with a section '# Watchlist Tier Pricing'
    - The END of the Watchlist block is the first line AFTER the Watchlist section that is clearly a new section (such as another "# ..." section header about a different product, e.g. '# Selected Services and Pricing: Identity Document Verification', '# Selected Services and Pricing: Workflow Studio', etc.), or the end of the document if nothing follows.
    ### Output
    - Output a JSON object with two fields: "start_line" (the exact text of the start line), and "end_line" (the exact text of the end line; use "" if there is no subsequent section).
    - If the Watchlist section does not exist, output {{"start_line": "", "end_line": ""}}.

    DOCUMENT:
    ---
    {full_text}
        """
        # Add delay before API call
        await asyncio.sleep(1)

        response = await self.openai.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        data = json.loads(response.choices[0].message.content)
        print(data)
        return data

    async def extract_full_watchlist_chunk_by_llm(self, full_text):
        boundaries = await self.call_llm_for_watchlist_boundaries(full_text)
        start_line = boundaries.get("start_line", "").strip()
        end_line = boundaries.get("end_line", "").strip()
        if not start_line:
            return ""
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
        print(chunk)
        return chunk
    
    async def extract_watchlist_consumption_from_llm(self, watchlist_chunk):
        outputs = []
        if watchlist_chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(1)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.WATCHLIST_INSTRUCTION},
                        {"role": "user", "content": watchlist_chunk}
                    ]
                )
                outputs.append(json.loads(response.choices[0].message.content.strip()))
            except Exception as e:
                print(f"Error processing Watchlist chunk: {e}")
        else:
            print("No Watchlist chunk found.")
        print(outputs)
        return outputs
    
    def transform_to_flat_records(self, outputs):
        subscriptions = []
        sub_cs = []
        sub_cr = []
        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
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
        return [
            {"name": "Subscription", "data": subscriptions},
            {"name": "subConsumptionSchedule", "data": sub_cs},
            {"name": "subConsumptionRate", "data": sub_cr}
        ]
    
    def merge_into_contract_json(self, contract_json, extracted_output):
        section_map = {rec["name"]: rec for rec in contract_json.get("output_records", [])}
        for section in extracted_output:
            section_name = section["name"]
            section_data = section["data"]
            if section_name in section_map:
                section_map[section_name]["data"].extend(section_data)
            else:
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
        # 1. LLM boundary chunk
        watchlist_chunk = await self.extract_full_watchlist_chunk_by_llm(full_text)
        # 2. LLM extract
        outputs = await self.extract_watchlist_consumption_from_llm(watchlist_chunk)
        # 3. Batch Salesforce lookup
        all_names = []
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    name = subscription.get("ProductName", "")
                    if name:
                        all_names.append(name)
        all_names = list(set(all_names))
        product_id_map = {}
        note_map = {}
        if all_names:
            try:
                sf = Salesforce(
                    username=self.username,
                    password=self.password,
                    security_token=self.security_token,
                    domain=self.domain
                )
                soql_names = ", ".join(["'%s'" % name.replace("'", r"\'") for name in all_names])
                soql = f"SELECT Id, Name FROM Product2 WHERE Name IN ({soql_names}) AND IsActive = true"
                res = sf.query(soql)
                for record in res["records"]:
                    product_id_map[record["Name"]] = record["Id"]
                    note_map[record["Name"]] = "Successfully Matched"
                for name in all_names:
                    if name not in product_id_map:
                        product_id_map[name] = None
                        note_map[name] = "Product not found"
            except Exception as e:
                for name in all_names:
                    product_id_map[name] = None
                    note_map[name] = "Could not extract ProductId from Salesforce"
        sub_count = 1
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    name = subscription.get("ProductName", "")
                    subscription["subExternalId"] = f"watchlist_sub_{sub_count}_{contractExternalId}"
                    subscription["ContractExternalId"] = contractExternalId
                    subscription["ContractName"] = ContractName
                    subscription["SBQQ__SubscriptionStartDate__c"] = StartDate
                    subscription["subCsExternalId"] = f"watchlist_subcs_{sub_count}_{contractExternalId}"
                    subscription["ProductId"] = product_id_map.get(name)
                    subscription["Note"] = note_map.get(name)
                    if "scr" in subscription:
                        for i, scr in enumerate(subscription["scr"], 1):
                            scr["subCrExternalId"] = f"watchlist_subcr{sub_count}_{i}_{contractExternalId}"
                    sub_count += 1
        # 5. Output format and merge
        transformed = self.transform_to_flat_records(outputs)
        merged = self.merge_into_contract_json(output_all_json, transformed)
        return merged