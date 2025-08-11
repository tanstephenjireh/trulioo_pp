import json
from openai import AsyncOpenAI
from simple_salesforce.api import Salesforce
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from config import get_ssm_param
import asyncio


WATCHLIST_INSTRUCTIONS = """
You are an intelligent field extractor. You are given a chunk of a document that may contain two relevant sections:
    1. Selected Services and Pricing: Watchlist
    2. Watchlist Tier Pricing
If a value is not present or no chunk was provided, return "NA". Do not leave any field blank.

## `subscription` (subscription level fields).
Do this for each Name. One Name is one Subscription. 
From Section: Selected Services and Pricing: Watchlist
    - `ProductName`: Extract the Name listed in this section. 
    - `CurrencyIsoCode`: get the ISO currency code from the "Price Per Query" column or From Section: Watchlist Tier Pricing whicever is the price of the Name. 
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
    - `CurrencyIsoCode`: get the ISO currency code from the "Price Per Query" column or From Section: Watchlist Tier Pricing whicever is the price of the Name. 
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


class WatchlistExtractor:
    def __init__(self):
        """Initialize the Watchlist extractor."""
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")
        
        # Google Sheet configuration
        self.gsheet_url = 'https://docs.google.com/spreadsheets/d/1uDA-59DhXE5rld3UBawrLb5iERNsLL8Nyd8kweSuqaw/edit?gid=309153661#gid=309153661'
        self.creds_path = 'trulioo-413bdb6f7cd9.json'
        
        # Salesforce connection
        self.sf = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            domain=self.domain
        )

    async def call_llm_for_watchlist_boundaries(self, full_text):
        """Find the exact line text that marks the START and END of the Watchlist section."""
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
        await asyncio.sleep(5)

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
        """Extract the full watchlist chunk based on boundaries."""
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
        """Extract watchlist consumption data from the chunk."""
        outputs = []
        if watchlist_chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(5)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": WATCHLIST_INSTRUCTIONS},
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

    def get_watchlist_productcodes_from_gsheet(self, product_name):
        """Look up ProductName in the 'WL and DocV' sheet and return ProductCodes."""
        try:
            scopes = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive',
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_path, scopes)  # type: ignore
            client = gspread.authorize(creds)  # type: ignore
            sheet = client.open_by_url(self.gsheet_url).worksheet('WL and DocV')
            data = sheet.get_all_values()
            codes = []
            for row in data[1:]:  # skip header
                if row and row[0].strip() == product_name.strip() and len(row) > 1:
                    codes.append(row[1].strip())
            return codes
        except Exception as e:
            print(f"[Watchlist GSheet] Error: {e}")
        return []

    def transform_to_flat_records(self, outputs, contract_external_id, contract_name, subscription_start_date):
        """Transform outputs to flat records with amd_ prefix for external IDs."""
        subscriptions = []
        sub_cs = []
        sub_cr = []
        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
                    sub_external_id = subscription.get("subExternalId") or f"amd_watchlist_sub_{idx}_{contract_external_id}"
                    sub_flat = {
                        "subExternalId": sub_external_id,
                        "ProductName": subscription.get("ProductName", ""),
                        "ContractExternalId": contract_external_id,
                        "ContractName": contract_name,
                        "CurrencyIsoCode": "USD",
                        "SBQQ__SubscriptionStartDate__c": subscription_start_date,
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
                        "subCsExternalId": f"amd_watchlist_subcs_{idx}_{contract_external_id}",
                        "subExternalId": sub_external_id,
                        "subscriptionName": subscription.get("ProductName", ""),
                        "subCsName": subscription.get("ProductName", "") + " Direct Consumption Schedule",
                        "CurrencyIsoCode": "USD",
                        "RatingMethod__c": "Tier",
                        "Type__c": "Range"
                    }
                    sub_cs.append(sub_cs_flat)
                    for i, scr in enumerate(subscription.get("scr", []), 1):
                        sub_cr_flat = {
                            "subCrExternalId": f"amd_watchlist_subcr_{idx}_{contract_external_id}",
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
        """Merge extracted output into contract JSON."""
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

    async def extract_watchlist_data(self, markdown_text, contract_json):
        """
        Main method to extract watchlist data from markdown and enrich contract JSON.
        
        Args:
            markdown_text (str): The markdown text containing watchlist information
            contract_json (dict): The contract JSON to enrich
            
        Returns:
            dict: Enriched contract JSON with watchlist data
        """
        if not isinstance(contract_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")
        
        # Extract contract data
        contract_data = contract_json["output_records"][0]["data"][0]
        contract_external_id = contract_data.get("ContractExternalId", "")
        contract_name = contract_data.get("AccountName", "")
        start_date = contract_data.get("StartDate", "")
        
        print("Starting Watchlist extraction...")
        
        # Step 1: Extract watchlist chunk
        watchlist_chunk = await self.extract_full_watchlist_chunk_by_llm(markdown_text)
        if not watchlist_chunk:
            print("No watchlist section found in document.")
            return contract_json
        
        # Step 2: Extract watchlist data
        outputs = await self.extract_watchlist_consumption_from_llm(watchlist_chunk)
        if not outputs:
            print("No watchlist data extracted.")
            return contract_json
        
        # Step 3: Collect all product names and get product codes
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
                        product_codes = self.get_watchlist_productcodes_from_gsheet(name)
                        if product_codes:
                            productcode_map[name] = product_codes[0]  # for backward compatibility
                            productcode_multi_map[name] = product_codes
                            all_product_codes.update(product_codes)
        
        all_names = list(set(all_names))
        all_product_codes = list(all_product_codes)
        
        # Step 4: Salesforce lookup
        product_id_map = {}
        note_map = {}
        
        if all_names:
            try:
                # Batch query by ProductCode
                if all_product_codes:
                    codes_str = ", ".join([f"'{code}'" for code in all_product_codes])
                    soql = f"SELECT Id, Name, ProductCode, SBQQ__BillingFrequency__c, SBQQ__PricingMethod__c, SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c, SBQQ__BillingType__c, CreatedDate FROM Product2 WHERE ProductCode IN ({codes_str}) AND IsActive = true AND Id IN (SELECT Product2Id FROM PricebookEntry WHERE IsActive = true AND Pricebook2.Name = 'CPQ Pricebook') ORDER BY ProductCode, CreatedDate DESC"
                    print(f"[DEBUG] BATCH SOQL QUERY (ProductCode): {soql}")
                    res = self.sf.query_all(soql)
                    print(f"[DEBUG] BATCH QUERY RESULT (ProductCode): {json.dumps(res, indent=2, ensure_ascii=False)}")
                    
                    # Map by ProductName||ProductCode
                    for r in res["records"]:
                        key = f"{r['Name']}||{r['ProductCode']}"
                        if key not in product_id_map:
                            product_id_map[key] = r["Id"]
                            note_map[key] = "Amendment: Successfully Matched"
                        else:
                            note_map[key] = "Amendment: Duplicate products found. Id obtained from latest CreatedDate"
                
                # Batch query by Name (for those without ProductCode)
                names_without_codes = [name for name in all_names if name not in productcode_multi_map]
                if names_without_codes:
                    names_escaped = [n.replace("'", "\\'") for n in names_without_codes]
                    names_str = ", ".join([f"'{n}'" for n in names_escaped])
                    soql = (f"SELECT Id, Name, ProductCode, SBQQ__BillingFrequency__c, SBQQ__PricingMethod__c, "
                            f"SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c, SBQQ__BillingType__c, CreatedDate "
                            f"FROM Product2 WHERE Name IN ({names_str}) AND IsActive = true ORDER BY Name, CreatedDate DESC")
                    print(f"[DEBUG] BATCH SOQL QUERY (Name): {soql}")
                    res = self.sf.query_all(soql)
                    print(f"[DEBUG] BATCH QUERY RESULT (Name): {json.dumps(res, indent=2, ensure_ascii=False)}")
                    
                    for r in res["records"]:
                        name = r["Name"]
                        if name not in product_id_map:
                            product_id_map[name] = r["Id"]
                            note_map[name] = "Amendment: Successfully Matched"
                        else:
                            note_map[name] = "Amendment: Duplicate products found. Id obtained from latest CreatedDate"
                            
            except Exception as e:
                print(f"Salesforce query error: {e}")
                for name in all_names:
                    product_id_map[name] = None
                    note_map[name] = "Amendment: Could not extract ProductId from Salesforce"
        
        # Step 5: Build product fields map
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
        
        # Step 6: Expand subscriptions for multiple product codes
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
        
        # Step 7: Enrich subscriptions with Salesforce data
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
                            subscription["Note"] = "Amendment: ProductCode not found"
                        fields = product_fields_map.get(key, {})
                    else:
                        subscription["ProductId"] = product_id_map.get(name)
                        if name in note_map:
                            subscription["Note"] = note_map.get(name)
                        else:
                            subscription["Note"] = "Amendment: ProductCode not found"
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
        
        # Step 8: Transform and merge
        transformed = self.transform_to_flat_records(outputs, contract_external_id, contract_name, start_date)
        merged = self.merge_into_contract_json(contract_json, transformed)
        
        print("Watchlist extraction completed successfully!")
        return merged


# if __name__ == "__main__":
#     # Test the class
#     input_md = "BACKEND/THIRDV/input_single/parsed_Complete_with_DocuSign_Crowdcube_Capital_Ltd.md"
#     input_contract_json = "BACKEND/THIRDV/output_single_json/json_parsed_Complete_with_DocuSign_Crowdcube_Capital_Ltd_final.json"
#     output_json = "json_parsed_Complete_with_DocuSign_Crowdcube_Capital_Ltd_watchlist.json"

#     with open(input_md, "r", encoding="utf-8") as f:
#         md_str = f.read()
#     with open(input_contract_json, "r", encoding="utf-8") as f:
#         contract_json = json.load(f)

#     # Create extractor instance and process
#     extractor = WatchlistExtractor()
#     merged = extractor.extract_watchlist_data(md_str, contract_json)

#     with open(output_json, "w", encoding="utf-8") as f:
#         json.dump(merged, f, indent=2, ensure_ascii=False)

#     print(f"Saved merged output to {output_json}") 