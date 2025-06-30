import re
import json
import logging
from openai import OpenAI
from config import get_ssm_param


# Configure logging
logger = logging.getLogger(__name__)

class DocV:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.DOCV_INSTRUCTION = get_ssm_param("/myapp/docv_prompt")
        self.openai = OpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")


    # ========== DOCV EXTRACTION LOGIC ==========
    def extract_full_docv_chunk(self, full_text):
        """
        Extracts the entire DOCV block—starting from the first
        'Selected Services and Pricing: Identity Document Verification'
        header until the end of the input text.
        Returns a single string to pass to the LLM.
        """
        header = r"Selected Services and Pricing: Identity Document Verification"
        match = re.search(header, full_text, re.IGNORECASE)
        if not match:
            return ""  # No DOCV block found

        start = match.start()
        chunk = full_text[start:].strip()  # Everything from header to end of file
        return chunk
    
    def extract_docv_consumption_from_llm(self, docv_chunk):
        outputs = []
        if docv_chunk.strip():
            try:
                response = self.openai.chat.completions.create(
                    model="gpt-4.1",
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
                        "subCsName": subscription.get("ProductName", "") + " - Direct Consumption Schedule",
                        "subExternalId": subscription.get("subExternalId", ""),
                        "subscriptionName": subscription.get("ProductName", ""),
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
    
    def main(self, parsed_input, output_all_json):
        from simple_salesforce import Salesforce


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


        # 4. Extract the DOCV chunk
        docv_chunk = self.extract_full_docv_chunk(parsed_input)

        # 5. Get LLM extraction
        outputs = self.extract_docv_consumption_from_llm(docv_chunk)

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