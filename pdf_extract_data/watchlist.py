import json
import asyncio
from openai import AsyncOpenAI
from simple_salesforce.api import Salesforce
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from config import get_ssm_param
import logging


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

        self.gs_url = 'https://docs.google.com/spreadsheets/d/1uDA-59DhXE5rld3UBawrLb5iERNsLL8Nyd8kweSuqaw/edit?gid=309153661#gid=309153661'
        self.creds_path = 'trulioo-413bdb6f7cd9.json'

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
        content = response.choices[0].message.content
        if content is None:
            return {"start_line": "", "end_line": ""}
        data = json.loads(content)
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
                content = response.choices[0].message.content
                if content is not None:
                    outputs.append(json.loads(content.strip()))
            except Exception as e:
                print(f"Error processing Watchlist chunk: {e}")
        else:
            print("No Watchlist chunk found.")
        print(outputs)
        return outputs
    
    def transform_to_flat_records(self, outputs, contractExternalId, contractName, subscriptionStartDate):
        subscriptions = []
        sub_cs = []
        sub_cr = []
        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
                    sub_external_id = subscription.get("subExternalId") or f"watchlist_sub_{idx}_{contractExternalId}"
                    sub_flat = {
                        "subExternalId": sub_external_id,
                        "ProductName": subscription.get("ProductName", ""),
                        "ContractExternalId": contractExternalId,
                        "ContractName": contractName,
                        "CurrencyIsoCode": "USD",
                        "SBQQ__SubscriptionStartDate__c": subscriptionStartDate,
                        "ProductId": subscription.get("ProductId", ""),
                        "ProductCode": subscription.get("ProductCode", ""),
                        "SBQQ__BillingFrequency__c": subscription.get("SBQQ__BillingFrequency__c", ""),
                        "SBQQ__BillingType__c": subscription.get("SBQQ__BillingType__c", ""),
                        "SBQQ__ChargeType__c": subscription.get("SBQQ__ChargeType__c", ""),
                        "SBQQ__SubscriptionPricing__c": subscription.get("SBQQ__SubscriptionPricing__c", ""),
                        "SBQQ__SubscriptionType__c": subscription.get("SBQQ__SubscriptionType__c", ""),
                        "Note": subscription.get("Note", "")
                    }
                    subscriptions.append(sub_flat)
                    sub_cs_flat = {
                        "subCsExternalId": f"watchlist_subcs_{idx}_{contractExternalId}",
                        "subExternalId": sub_external_id,
                        "subscriptionName": subscription.get("ProductName", ""),
                        "subCsName": subscription.get("ProductName", "") + " Direct Consumption Schedule",
                        "CurrencyIsoCode": "USD",
                        "RatingMethod__c":"Tier",
                        "Type__c": "Range"
                    }
                    sub_cs.append(sub_cs_flat)
                    for i, scr in enumerate(subscription.get("scr", []), 1):
                        sub_cr_flat = {
                            "subCrExternalId": f"watchlist_subcr_{idx}_{contractExternalId}",
                            "subCrName": scr.get("subCrName", ""),
                            "subExternalId": sub_external_id,  # always use the parent subscription's subExternalId
                            "subscriptionName": subscription.get("ProductName", ""),
                            "CurrencyIsoCode": "USD",
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
    
    # --- Google Sheet logic for Watchlist ProductCode mapping ---
    def get_watchlist_productcodes_from_gsheet(self, product_name, gsheet_url, creds_json_path):
        """
        Looks up the ProductName in the 'WL and DocV' sheet (first column). Returns a list of all ProductCodes (second column) for all matching rows.
        Returns [] if not found or error.
        """
        try:
            scopes = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive',
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scopes)  # type: ignore
            client = gspread.authorize(creds)  # type: ignore
            sheet = client.open_by_url(gsheet_url).worksheet('WL and DocV')
            data = sheet.get_all_values()
            codes = []
            for row in data[1:]:  # skip header
                if row and row[0].strip() == product_name.strip() and len(row) > 1:
                    codes.append(row[1].strip())
            return codes
        except Exception as e:
            print(f"[Watchlist GSheet] Error: {e}")
        return []
    
    async def main(self, markdown_text, contract_json):
        if not isinstance(contract_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")
        # Use markdown_text directly:
        full_text = markdown_text
        contract_data = contract_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")
        # 1. LLM boundary chunk
        watchlist_chunk = await self.extract_full_watchlist_chunk_by_llm(full_text)
        # 2. LLM extract
        outputs = await self.extract_watchlist_consumption_from_llm(watchlist_chunk)
        # 3. Batch Salesforce lookup
        # Google Sheet config (same as docv.py)
        all_names = []
        productcode_map = {}
        productcode_multi_map = {}  # name -> list of product codes
        all_product_codes = set()
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    name = subscription.get("ProductName", "")
                    if name:
                        all_names.append(name)
                        product_codes = self.get_watchlist_productcodes_from_gsheet(name, self.gs_url, self.creds_path)
                        if product_codes:
                            productcode_map[name] = product_codes[0]  # for backward compatibility
                            productcode_multi_map[name] = product_codes
                            all_product_codes.update(product_codes)
        all_names = list(set(all_names))
        all_product_codes = list(all_product_codes)
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
                # --- Batch query by ProductCode ---
                if all_product_codes:
                    codes_str = ", ".join([f"'{code}'" for code in all_product_codes])
                    soql = f"SELECT Id, Name, ProductCode, SBQQ__BillingFrequency__c, SBQQ__PricingMethod__c, SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c, SBQQ__BillingType__c, CreatedDate FROM Product2 WHERE ProductCode IN ({codes_str}) AND IsActive = true AND Id IN (SELECT Product2Id FROM PricebookEntry WHERE IsActive = true AND Pricebook2.Name = 'CPQ Pricebook') ORDER BY ProductCode, CreatedDate DESC"
                    print(f"[DEBUG] BATCH SOQL QUERY (ProductCode): {soql}")
                    res = sf.query_all(soql)
                    print(f"[DEBUG] BATCH QUERY RESULT (ProductCode): {json.dumps(res, indent=2, ensure_ascii=False)}")
                    # Map by ProductName||ProductCode
                    for r in res["records"]:
                        key = f"{r['Name']}||{r['ProductCode']}"
                        if key not in product_id_map:
                            product_id_map[key] = r["Id"]
                            note_map[key] = "Successfully Matched"
                        else:
                            # If duplicate, keep the latest (first due to DESC)
                            note_map[key] = "Duplicate products found. Id obtained from latest CreatedDate"
                # --- Batch query by Name (for those without ProductCode) ---
                names_without_codes = [name for name in all_names if name not in productcode_multi_map]
                if names_without_codes:
                    # Escape single quotes in names for SOQL
                    names_escaped = [n.replace("'", "\\'") for n in names_without_codes]
                    names_str = ", ".join([f"'{n}'" for n in names_escaped])
                    soql = (f"SELECT Id, Name, ProductCode, SBQQ__BillingFrequency__c, SBQQ__PricingMethod__c, "
                            f"SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c, SBQQ__BillingType__c, CreatedDate "
                            f"FROM Product2 WHERE Name IN ({names_str}) AND IsActive = true ORDER BY Name, CreatedDate DESC")
                    print(f"[DEBUG] BATCH SOQL QUERY (Name): {soql}")
                    res = sf.query_all(soql)
                    print(f"[DEBUG] BATCH QUERY RESULT (Name): {json.dumps(res, indent=2, ensure_ascii=False)}")
                    for r in res["records"]:
                        name = r["Name"]
                        if name not in product_id_map:
                            product_id_map[name] = r["Id"]
                            note_map[name] = "Successfully Matched"
                        else:
                            note_map[name] = "Duplicate products found. Id obtained from latest CreatedDate"
            except Exception as e:
                for name in all_names:
                    product_id_map[name] = None
                    note_map[name] = "Could not extract ProductId from Salesforce"
        # 1. After Salesforce query, build a lookup for extra fields
        # (Add this after each Salesforce query result processing, for both ProductCode and Name lookups)
        product_fields_map = {}
        if 'res' in locals():
            for r in res["records"]:
                key = f"{r['Name']}||{r['ProductCode']}"
                product_fields_map[key] = {
                    "SBQQ__BillingFrequency__c": r.get("SBQQ__BillingFrequency__c", ""),
                    "SBQQ__BillingType__c": r.get("SBQQ__BillingType__c", ""),
                    "SBQQ__ChargeType__c": r.get("SBQQ__ChargeType__c", ""),
                    "SBQQ__SubscriptionPricing__c": r.get("SBQQ__SubscriptionPricing__c", ""),
                    "SBQQ__SubscriptionType__c": r.get("SBQQ__SubscriptionType__c", "")
                }
                # Also allow lookup by just name
                product_fields_map[r["Name"]] = {
                    "SBQQ__BillingFrequency__c": r.get("SBQQ__BillingFrequency__c", ""),
                    "SBQQ__BillingType__c": r.get("SBQQ__BillingType__c", ""),
                    "SBQQ__ChargeType__c": r.get("SBQQ__ChargeType__c", ""),
                    "SBQQ__SubscriptionPricing__c": r.get("SBQQ__SubscriptionPricing__c", ""),
                    "SBQQ__SubscriptionType__c": r.get("SBQQ__SubscriptionType__c", "")
                }

        # Expand subscriptions for multiple product codes
        expanded_outputs = []
        for doc in outputs:
            if "subscription" in doc:
                new_doc = {"subscription": []}
                for subscription in doc["subscription"]:
                    name = subscription.get("ProductName", "")
                    codes = productcode_multi_map.get(name, [])
                    if codes:
                        for code in codes:
                            sub_copy = subscription.copy()
                            sub_copy["ProductCode"] = code
                            sub_copy["_ProductCodeFromSheet"] = code  # for debug/tracing
                            new_doc["subscription"].append(sub_copy)
                    else:
                        sub_copy = subscription.copy()
                        sub_copy["ProductCode"] = ""
                        new_doc["subscription"].append(sub_copy)
                expanded_outputs.append(new_doc)
            else:
                expanded_outputs.append(doc)
        outputs = expanded_outputs

        # 2. In enrichment loop, set these fields for each subscription
        sub_count = 1
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    name = subscription.get("ProductName", "")
                    code = subscription.get("ProductCode", "")
                    if code:
                        key = f"{name}||{code}"
                        subscription["ProductId"] = product_id_map.get(key)
                        if key in note_map:
                            subscription["Note"] = note_map.get(key)
                        else:
                            subscription["Note"] = "ProductCode not found"
                        fields = product_fields_map.get(key, {})
                    else:
                        subscription["ProductId"] = product_id_map.get(name)
                        if name in note_map:
                            subscription["Note"] = note_map.get(name)
                        else:
                            subscription["Note"] = "ProductCode not found"
                        fields = product_fields_map.get(name, {})
                    for field in [
                        "SBQQ__BillingFrequency__c",
                        "SBQQ__BillingType__c",
                        "SBQQ__ChargeType__c",
                        "SBQQ__SubscriptionPricing__c",
                        "SBQQ__SubscriptionType__c"
                    ]:
                        subscription[field] = fields.get(field, "")
                    sub_count += 1
        # 5. Output format and merge
        transformed = self.transform_to_flat_records(outputs, contractExternalId, ContractName, StartDate)
        merged = self.merge_into_contract_json(contract_json, transformed)
        return merged