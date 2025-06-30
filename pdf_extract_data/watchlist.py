import re
import json
import logging
from openai import OpenAI
from config import get_ssm_param
from simple_salesforce import Salesforce

# Configure logging
logger = logging.getLogger(__name__)

class Watchlist:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.WATCHLIST_INSTRUCTION = get_ssm_param("/myapp/watchlist_prompt")
        self.openai = OpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")

    def extract_full_watchlist_chunk(self, full_text):
        """
        Extracts the entire Watchlist block—starting from the first
        markdown header containing 'Watchlist' (e.g. '# ...Watchlist...')
        until the end of the input text.
        Returns a single string to pass to the LLM.
        """
        # Match any markdown header (e.g. '#', '##', '###', etc.) with 'Watchlist' in it
        header_regex = r"^#{1,6}\s.*watchlist.*$"
        match = re.search(header_regex, full_text, re.IGNORECASE | re.MULTILINE)
        if not match:
            return ""
        start = match.start()
        chunk = full_text[start:].strip()
        return chunk
    
    def extract_watchlist_consumption_from_llm(self, docv_chunk):
        outputs = []
        if docv_chunk.strip():
            try:
                response = self.openai.chat.completions.create(
                    model="gpt-4.1",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.WATCHLIST_INSTRUCTION},
                        {"role": "user", "content": docv_chunk}
                    ]
                )
                outputs.append(json.loads(response.choices[0].message.content.strip()))
            except Exception as e:
                print(f"Error processing DOCV chunk: {e}")
        else:
            print("No Watchlist chunk found.")
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
        section_map = {rec["name"]: rec for rec in contract_json.get("output_records", [])}

        for section in extracted_output:
            section_name = section["name"]
            section_data = section["data"]
            if section_name in section_map:
                section_map[section_name]["data"].extend(section_data)
            else:
                # Add new section if not present
                contract_json["output_records"].append({
                    "name": section_name,
                    "data": section_data
                })

        return contract_json
    
    def main(self, parsed_input, output_all_json):
        # 1. Read contract info

        contract_data = output_all_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")

        # 3. Extract the Watchlist chunk
        docv_chunk = self.extract_full_watchlist_chunk(parsed_input)

        # 4. Get LLM extraction
        outputs = self.extract_watchlist_consumption_from_llm(docv_chunk)

        # 5. Collect all unique ProductNames for batch lookup
        all_names = []
        for doc in outputs:
            if "subscription" in doc:
                for subscription in doc["subscription"]:
                    name = subscription.get("ProductName", "")
                    if name:
                        all_names.append(name)
        all_names = list(set(all_names))  # unique

        # 6. Batch query Salesforce for these names
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
        else:
            product_id_map = {}
            note_map = {}

        # 7. Add custom fields (these will ALWAYS be present)
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

        # 8. Transform to requested output structure
        transformed = self.transform_to_flat_records(outputs)

        # 9. Merge extracted data into contract_json
        merged = self.merge_into_contract_json(output_all_json, transformed)

        return merged 