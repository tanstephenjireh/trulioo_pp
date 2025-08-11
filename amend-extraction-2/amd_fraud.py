import json
import asyncio
from openai import AsyncOpenAI
from simple_salesforce.api import Salesforce
from config import get_ssm_param

class FraudExtractor:
    def __init__(self):
        """Initialize the Fraud Intelligence extractor with environment and Salesforce connection."""
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")
        
        # ========== PROMPT ==========
        self.FRAUD_INTELLIGENCE_INSTRUCTION = """
You are an intelligent field extractor. You are given a chunk of a document that may contain up to three relevant sections:
    1. Selected Services and Pricing: Fraud Intelligence
    2. Fraud Intelligence – Person Fraud Tier Pricing
    3. Fraud Intelligence – Person Fraud Surcharge

If a value is not present or no chunk was provided, return "NA". Do not leave any field blank.

## `subscription` (subscription-level fields).
Do this for each Name. One Name is one Subscription. 
From Section: Selected Services and Pricing: Fraud Intelligence
    - `ProductName`: Extract the Name listed in this section (e.g. "Fraud Intelligence – Person Fraud").
    - `CurrencyIsoCode`: Get the ISO currency code from the "Price Per Query" column, or from Tier Pricing/Surcharge. 
        - If "$", it is automatically USD.
        - Return "NA" if there is no price.

## `scr` (subscription consumption rate for this subscription)
For each ProductName above, find the corresponding pricing details from Fraud Intelligence – Person Fraud Tier Pricing.
Disregard ## Fraud Intelligence – Person Fraud Surcharge if present.
    - `subCrName`: The name or identifier of the price or tier. 
        - Use "<Name> Consumption Rate" if the price is not tiered.
        - Use "<Name Tier <n>> Consumption Rate" if the price is tiered.
        - Use "<Name Surcharge>" for surcharges if applicable.
    - `LowerBound__c`: The monthly volume lower bound for tiered pricing (Including 0). Use "1" if the price is not dependent on a range. 
    - `UpperBound__c`: The monthly volume upper bound for tiered pricing. Use "NA" if none.
    - `Price__c`: The value:
        - From the "Price per Query" column for tiered.
        - From the "Fee per Query" column for non-tiered.
        - Extract only the number, **without** the currency symbol.
    - `CurrencyIsoCode`: Get the ISO currency code from the relevant price. If "$", it is automatically USD. Return "NA" if not present.

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
    }
  ]
}
"""
        
        # Initialize Salesforce connection
        self.sf = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            domain=self.domain
        )

    async def call_llm_for_fraud_boundaries(self, full_text):
        """
        Uses LLM to find the start and end line (as exact line text) of the Fraud Intelligence section.
        Returns a dict: {"start_line": "...", "end_line": "..."} or None if not found.
        """
        system_prompt = (
            "You are an expert contract parser. "
            "Your job is to find the exact line text that marks the START and END of the Fraud Intelligence section in the contract below, if present"
        )
        user_prompt = f"""
# Instructions:
- Check if the document has a Fraud Intelligence section and get the start_line and end_line, only if the section exists.
### START OF FRAUD INTELLIGENCE ###
- Find the line that marks the START of the Fraud Intelligence block. This may be a line containing 'Selected Services and Pricing: Fraud Intelligence'.
    - A Fraud Intelligence block may also be associated with sections like '# Fraud Intelligence – Person Fraud Tier Pricing' or '# Fraud Intelligence – Person Fraud Surcharge'.
- The END of the Fraud Intelligence block is the first line AFTER the Fraud Intelligence section that is clearly a new section (such as "# General Terms and Conditions" or section header about a different product).
### Output
- Output a JSON object with two fields: "start_line" (the exact text of the start line), and "end_line" (the exact text of the end line; use "" if there is no subsequent section).
- If the Fraud Intelligence section does not exist, output {{"start_line": "", "end_line": ""}}.

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

    async def extract_full_fraud_chunk_by_llm(self, full_text):
        """
        Uses GPT-4o boundary finder to return the Fraud Intelligence chunk text (or "" if not found).
        """
        boundaries = await self.call_llm_for_fraud_boundaries(full_text)
        start_line = boundaries.get("start_line", "").strip()
        end_line = boundaries.get("end_line", "").strip()

        if not start_line:
            return ""  # No block found

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

    async def extract_fraud_consumption_from_llm(self, fraud_chunk):
        """Extract Fraud Intelligence consumption data from the chunk using LLM."""
        outputs = []
        if fraud_chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(5)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.FRAUD_INTELLIGENCE_INSTRUCTION},
                        {"role": "user", "content": fraud_chunk}
                    ]
                )
                content = response.choices[0].message.content
                if content is not None:
                    outputs.append(json.loads(content.strip()))
            except Exception as e:
                print(f"Error processing FRAUD chunk: {e}")
        else:
            print("No Fraud Intelligence chunk found.")
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
                        "Note": subscription.get("Note", "")
                    }
                    subscriptions.append(sub_flat)
                    # SUB CONSUMPTION SCHEDULE
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
                    # SUB CONSUMPTION RATE
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
        """Merge extracted Fraud Intelligence data into the contract JSON."""
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

    async def extract_fraud_data(self, markdown_text, contract_json):
        """
        Main method to extract Fraud Intelligence data from markdown and enrich contract JSON.
        """
        if not isinstance(contract_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")

        print("Starting Fraud Intelligence extraction process...")
        
        # Use markdown_text directly:
        full_text = markdown_text 

        # 1. Read contract info
        contract_data = contract_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")

        # 2. Extract the Fraud chunk
        fraud_chunk = await self.extract_full_fraud_chunk_by_llm(full_text)
        
        # 3. Get LLM extraction
        outputs = await self.extract_fraud_consumption_from_llm(fraud_chunk)

        # 4. Salesforce query for 'Fraud Intelligence - Person Fraud'
        fraud_field_map = {}
        fraud_product_id = None
        note = "Amendment: Product not found"
        try:
            query = (
                "SELECT Id, Name, CreatedDate, SBQQ__BillingFrequency__c, SBQQ__PricingMethod__c, "
                "SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c, SBQQ__BillingType__c "
                "FROM Product2 WHERE Name = 'Fraud Intelligence - Person Fraud' AND IsActive = true "
                "ORDER BY CreatedDate DESC"
            )
            print(f"[DEBUG] FRAUD SOQL QUERY: {query}")
            res = self.sf.query(query)
            if res["totalSize"] > 0:
                for r in res["records"]:
                    fraud_field_map[r["Name"]] = r
                fraud_product_id = res["records"][0]["Id"]
                note = "Amendment: Successfully Matched" if res["totalSize"] == 1 else "Amendment: Duplicate products found. Id obtained from latest CreatedDate"
            else:
                note = "Amendment: Product not found"
        except Exception as e:
            note = "Amendment: Could not extract ProductId from Salesforce"

        # 5. Add custom fields to ALL subscriptions (like workflow.py)
        extra_fields = [
            "SBQQ__BillingFrequency__c",
            "SBQQ__PricingMethod__c",
            "SBQQ__SubscriptionPricing__c",
            "SBQQ__SubscriptionType__c",
            "SBQQ__ChargeType__c",
            "SBQQ__BillingType__c"
        ]
        sub_count = 1
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    # Use amd_fraud prefix for external IDs
                    subscription["subExternalId"] = f"amd_fraud_sub_{sub_count}_{contractExternalId}"
                    subscription["ContractExternalId"] = contractExternalId
                    subscription["ContractName"] = ContractName
                    subscription["SBQQ__SubscriptionStartDate__c"] = StartDate
                    subscription["subCsExternalId"] = f"amd_fraud_subcs_{sub_count}_{contractExternalId}"
                    # Set ProductId and custom fields from fraud_field_map
                    fraud_record = fraud_field_map.get(subscription.get("ProductName", ""), {})
                    subscription["ProductId"] = fraud_record.get("Id", "")
                    for f in extra_fields:
                        subscription[f] = fraud_record.get(f, "")
                    subscription["Note"] = note if not fraud_record else ("Amendment: Successfully Matched" if res["totalSize"] == 1 else "Amendment: Duplicate products found. Id obtained from latest CreatedDate")
                    if "scr" in subscription:
                        for i, scr in enumerate(subscription["scr"], 1):
                            scr["subCrExternalId"] = f"amd_fraud_subcr{sub_count}_{i}_{contractExternalId}"
                    sub_count += 1

        # 6. Transform to requested output structure
        transformed = self.transform_to_flat_records(outputs)
        
        # 7. Merge extracted data into contract_json
        merged = self.merge_into_contract_json(contract_json, transformed)
        
        print("Fraud Intelligence extraction completed successfully!")
        return merged


# if __name__ == "__main__":
#     # Test the class
#     input_md = "BACKEND/THIRDV/spot_checks_output/parsed_Guesty Services Agreement with Guesty (67bd05eb22).md"
#     input_contract_json = "BACKEND/THIRDV/output_json/Guesty Services Agreement with Guesty (67bd05eb22).json"
    
#     # Load input data
#     with open(input_contract_json, "r", encoding="utf-8") as f:
#         contract_data = json.load(f)
    
#     with open(input_md, "r", encoding="utf-8") as f:
#         markdown_text = f.read()
    
#     # Create extractor instance and process data
#     extractor = FraudExtractor()
#     merged = extractor.extract_fraud_data(markdown_text, contract_data)

#     # Save output
#     with open("amd_fraud2.json", "w", encoding="utf-8") as f:
#         json.dump(merged, f, indent=2, ensure_ascii=False)
    
#     print("Fraud Intelligence extraction test completed!") 