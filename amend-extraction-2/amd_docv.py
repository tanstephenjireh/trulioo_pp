import json
from openai import AsyncOpenAI
from simple_salesforce.api import Salesforce
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from config import get_ssm_param
import asyncio

## NO NEW LIBRARY WAS USED

class DocVExtractor:
    def __init__(self):
        """Initialize the DocV extractor with environment and Salesforce connection."""
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")
        
        # ========== OCR / PDF EXTRACT ==========
        self.DOCV_INSTRUCTION = """
You are an intelligent field extractor. You are given a chunk of a document that may contain two relevant sections:
    1. Selected Services and Pricing: Identity Document Verification
    2. Identity Document Verification - Tier Pricing
If a value is not present or no chunk was provided, return "NA". Do not leave any field blank.

## `subscription` (subscription level fields)
From Section: Selected Services and Pricing: Identity Document Verification
    - `ProductName`: Extract Item Name listed in this section. There is only one ItemName, under the column Item Name
From Section: Identity Document Verification - Tier Pricing
    - `CurrencyIsoCode`: get the ISO currency code from the "Price Per Query" table. 
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
        
        # Initialize Salesforce connection
        self.sf = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            domain=self.domain
        )
        
        # Google Sheets configuration
        self.GSHEET_URL = 'https://docs.google.com/spreadsheets/d/1uDA-59DhXE5rld3UBawrLb5iERNsLL8Nyd8kweSuqaw/edit?gid=309153661#gid=309153661'
        self.CREDS_PATH = 'trulioo-413bdb6f7cd9.json'

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
        await asyncio.sleep(5)

        response = await self.openai.chat.completions.create(
            model="gpt-4o-mini",
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
        return data

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

    async def extract_docv_consumption_from_llm(self, docv_chunk):
        """Extract DOCV consumption data from the chunk using LLM."""
        outputs = []
        if docv_chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(5)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.DOCV_INSTRUCTION},
                        {"role": "user", "content": docv_chunk}
                    ]
                )
                content = response.choices[0].message.content
                if content is not None:
                    outputs.append(json.loads(content.strip()))
            except Exception as e:
                print(f"Error processing DOCV chunk: {e}")
        else:
            print("No DOCV chunk found.")
        return outputs

    def transform_to_flat_records(self, outputs):
        """Transform LLM outputs to flat record structure."""
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
                        "CurrencyIsoCode": "USD",
                        "SBQQ__SubscriptionStartDate__c": subscription.get("SBQQ__SubscriptionStartDate__c", ""),
                        "ProductId": subscription.get("ProductId", ""),
                        "Note": subscription.get("Note", ""),
                        "SBQQ__BillingFrequency__c": subscription.get("SBQQ__BillingFrequency__c", ""),
                        "SBQQ__PricingMethod__c": subscription.get("SBQQ__PricingMethod__c", ""),
                        "SBQQ__SubscriptionPricing__c": subscription.get("SBQQ__SubscriptionPricing__c", ""),
                        "SBQQ__SubscriptionType__c": subscription.get("SBQQ__SubscriptionType__c", ""),
                        "SBQQ__ChargeType__c": subscription.get("SBQQ__ChargeType__c", ""),
                        "SBQQ__BillingType__c": subscription.get("SBQQ__BillingType__c", "")
                    }
                    subscriptions.append(sub_flat)

                    # SUB CONSUMPTION SCHEDULE (one per subscription)
                    sub_cs_flat = {
                        "subCsExternalId": subscription.get("subCsExternalId", ""),
                        "subExternalId": subscription.get("subExternalId", ""),
                        "subscriptionName": subscription.get("ProductName", ""),
                        "subCsName": subscription.get("ProductName", "") + " Direct Consumption Schedule",
                        "CurrencyIsoCode": "USD",
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
                            "CurrencyIsoCode": "USD",
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
        """Merge extracted DOCV data into the contract JSON."""
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

    def get_docv_productcodes_from_gsheet(self, product_name):
        """
        Looks up the ProductName in the 'WL and DocV' sheet (first column). Returns a list of all ProductCodes (second column) for all matching rows.
        Returns [] if not found or error.
        """
        try:
            scopes = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive',
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.CREDS_PATH, scopes)  # type: ignore
            client = gspread.authorize(creds)  # type: ignore
            sheet = client.open_by_url(self.GSHEET_URL).worksheet('WL and DocV')
            data = sheet.get_all_values()
            codes = []
            for row in data[1:]:  # skip header
                if row and row[0].strip() == product_name.strip() and len(row) > 1:
                    codes.append(row[1].strip())
            return codes
        except Exception as e:
            print(f"[DocV GSheet] Error: {e}")
        return []

    async def extract_docv_data(self, markdown_text, contract_json):
        """
        Main method to extract DOCV data from markdown and enrich contract JSON.
        """
        if not isinstance(contract_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")

        print("Starting DocV extraction process...")
        
        # Use markdown_text directly:
        full_text = markdown_text 

        contract_data = contract_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")

        # --- DocV ProductCode mapping from Google Sheet ---
        # Extract ProductName from the contract (if present)
        docv_product_name = None
        # Try to get ProductName from the markdown or contract_json (fallback to None)
        try:
            docv_chunk = await self.extract_full_docv_chunk_by_llm(full_text)
            outputs = await self.extract_docv_consumption_from_llm(docv_chunk)
            if outputs and outputs[0] and 'subscription' in outputs[0] and outputs[0]['subscription']:
                docv_product_name = outputs[0]['subscription'][0].get('ProductName')
        except Exception:
            docv_product_name = None

        # Look up ProductCodes from Google Sheet if ProductName matches
        docv_product_codes = []
        if docv_product_name:
            docv_product_codes = self.get_docv_productcodes_from_gsheet(docv_product_name)
        
        # --- Salesforce batch query for DocV ProductId(s) ---
        product_id_map = {}
        note_map = {}
        try:
            select_fields = (
                "Id, Name, ProductCode, CreatedDate, SBQQ__BillingFrequency__c, SBQQ__PricingMethod__c, "
                "SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c, SBQQ__BillingType__c"
            )
            if docv_product_codes:
                codes_str = ", ".join([f"'{code}'" for code in docv_product_codes])
                query = (
                    f"SELECT {select_fields} FROM Product2 WHERE Family = 'DocV' AND IsActive=true AND ProductCode IN ({codes_str}) "
                    "ORDER BY ProductCode, CreatedDate DESC"
                )
                print(f"[DEBUG] BATCH SOQL QUERY (ProductCode): {query}")
                res = self.sf.query_all(query)
                print(f"[DEBUG] BATCH QUERY RESULT (ProductCode): {json.dumps(res, indent=2, ensure_ascii=False)}")
                for r in res["records"]:
                    key = f"{r['Name']}||{r['ProductCode']}"
                    if key not in product_id_map:
                        product_id_map[key] = r["Id"]
                        note_map[key] = "Amendment: Successfully Matched"
                    else:
                        note_map[key] = "Amendment: Duplicate products found. Id obtained from latest CreatedDate"
            else:
                # fallback to default ProductCode
                query = (
                    f"SELECT {select_fields} FROM Product2 WHERE Family = 'DocV' AND IsActive=true AND ProductCode = 'DOCV2-LGC-DEFAULT' "
                    "ORDER BY CreatedDate DESC, ProductCode DESC"
                )
                print(f"[DEBUG] SOQL QUERY (Default): {query}")
                res = self.sf.query(query)
                print(f"[DEBUG] QUERY RESULT (Default): {json.dumps(res, indent=2, ensure_ascii=False)}")
                if res["totalSize"] > 0:
                    r = res["records"][0]
                    product_id_map[docv_product_name] = r["Id"]
                    note_map[docv_product_name] = "Amendment: Successfully Matched" if res["totalSize"] == 1 else "Amendment: Duplicate products found. Id obtained from latest CreatedDate"
                else:
                    product_id_map[docv_product_name] = None
                    note_map[docv_product_name] = "Amendment: DocV Product not found"
        except Exception as e:
            product_id_map[docv_product_name] = None
            note_map[docv_product_name] = "Amendment: Could not extract ProductId from Salesforce"
        
        # --- Expand subscriptions for multiple product codes ---
        expanded_outputs = []
        for doc in outputs:
            if "subscription" in doc:
                new_doc = {"subscription": []}
                for idx, subscription in enumerate(doc["subscription"], 1):
                    name = subscription.get("ProductName", "")
                    codes = docv_product_codes if docv_product_codes else []
                    if codes:
                        for code in codes:
                            sub_copy = subscription.copy()
                            sub_copy["_ProductCodeFromSheet"] = code  # for debug
                            new_doc["subscription"].append(sub_copy)
                    else:
                        new_doc["subscription"].append(subscription)
                expanded_outputs.append(new_doc)
            else:
                expanded_outputs.append(doc)
        outputs = expanded_outputs
        
        # Add custom fields (these will ALWAYS be present)
        extra_fields = [
            "SBQQ__BillingFrequency__c",
            "SBQQ__PricingMethod__c",
            "SBQQ__SubscriptionPricing__c",
            "SBQQ__SubscriptionType__c",
            "SBQQ__ChargeType__c",
            "SBQQ__BillingType__c"
        ]
        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
                    # Use amd_docv prefix for external IDs
                    subscription["subExternalId"] = f"amd_docv_sub_{idx}_{contractExternalId}"
                    subscription["ContractExternalId"] = contractExternalId
                    subscription["ContractName"] = ContractName
                    subscription["SBQQ__SubscriptionStartDate__c"] = StartDate
                    subscription["subCsExternalId"] = f"amd_docv_subcs_{idx}_{contractExternalId}"
                    code = subscription.get("_ProductCodeFromSheet")
                    if code:
                        key = f"{subscription.get('ProductName', '')}||{code}"
                        subscription["ProductId"] = product_id_map.get(key)
                        subscription["Note"] = note_map.get(key)
                    else:
                        subscription["ProductId"] = product_id_map.get(docv_product_name)
                        subscription["Note"] = note_map.get(docv_product_name)
                    for f in extra_fields:
                        subscription[f] = ""  # Optionally fill with actual values if needed
                    if "scr" in subscription:
                        for i, scr in enumerate(subscription["scr"], 1):
                            scr["subCrExternalId"] = f"amd_docv_subcr_{idx}_{contractExternalId}"

        # Transform to requested output structure
        transformed = self.transform_to_flat_records(outputs)

        # Merge extracted data into contract_json
        merged = self.merge_into_contract_json(contract_json, transformed)

        print("DocV extraction completed successfully!")
        return merged


# if __name__ == "__main__":
#     # Test the class
#     input_md = "BACKEND/THIRDV/input_single/parsed_Complete_with_DocuSign_Crowdcube_Capital_Ltd.md"
#     input_contract_json = "BACKEND/THIRDV/output_single_json/json_parsed_Complete_with_DocuSign_Crowdcube_Capital_Ltd_final.json"
    
#     # Load input data
#     with open(input_contract_json, "r", encoding="utf-8") as f:
#         contract_data = json.load(f)
    
#     with open(input_md, "r", encoding="utf-8") as f:
#         markdown_text = f.read()
    
#     # Create extractor instance and process data
#     extractor = DocVExtractor()
#     merged = extractor.extract_docv_data(markdown_text, contract_data)

#     # Save output
#     with open("amd_docv2.json", "w", encoding="utf-8") as f:
#         json.dump(merged, f, indent=2, ensure_ascii=False)
    
#     print("DocV extraction test completed!") 