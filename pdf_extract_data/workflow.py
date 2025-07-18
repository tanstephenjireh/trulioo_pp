import json
import logging
import asyncio
from openai import AsyncOpenAI
from config import get_ssm_param
from simple_salesforce import Salesforce

class Wflow:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.DOCV_INSTRUCTION = get_ssm_param("/myapp/docv_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")

        self.WORKFLOWSTUDIO_INSTRUCTION = """
        You are an intelligent field extractor. You are given a chunk of a document that may contain the relevant section:

            1. "# Selected Services and Pricing: Workflow Studio" (or similar such as "Selected Services & Pricing: Workflow Studio". There may be slight naming variations)

        If a value is not present or no chunk was provided, return "NA". Do not leave any field blank.

        ## `subscription` (subscription level fields)
        From Section: Selected Services and Pricing: Workflow Studio
            - `ProductName`: Extract the value under the column Item Name. (One ItemName is one ProductName under Subscription)
            - `CurrencyIsoCode`: get the ISO currency code from the “Fee per Query” column.
                - If "$", it is automatically USD. 

        ##  `scr` (subscription consumption rate for this subscription)
        For each ProductName above, find the corresponding pricing details from the section for the above Item Name, extract:
            - `subCrName`: Use "<ItemName> Consumption Rate"
            - `LowerBound__c`: The monthly volume lower bound for tiered pricing (Including 0). Always use "1" if the price is not dependent on a range or waived. 
            - `UpperBound__c`: The monthly volume upper bound for pricing. Use "NA" if none or not dependent on range or price is waived.
            - `Price__c`: The value from the "Fee per Query" column (without currency, 0 if Waived or no value)
            - `CurrencyIsoCode`: the currency under the "Fee per Query" column. If "$", it is automatically USD. 

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

    async def call_llm_for_workflowstudio_boundaries(self, full_text):
        """
        Uses LLM to find the start and end line (as exact line text) of the Workflow Studio section.
        Returns a dict: {"start_line": "...", "end_line": "..."} or {"start_line": "", "end_line": ""} if not found.
        """
        system_prompt = (
            "You are an expert contract parser. "
            "Your job is to find the exact line text that marks the START and END of the Workflow Studio section."
        )
        user_prompt = f"""
    # Instructions:
    ### START OF WORKFLOW STUDIO ###
    - Find the line that marks the START of the Workflow Studio block. This is a line containing "# Selected Services and Pricing: Workflow Studio" (or similar such as "Selected Services & Pricing: Workflow Studio". There may be slight naming variations).
    - While there may be naming variations, there may also be a lot of '# Selected Services and Pricing:'headers. make sure you are extracting for Workflow Studio
    - Do not extract from Person Match, Watchlist, Identity Document Verification or anything else. WORKFLOW STUDIO ONLY.
    ### END OF WORKFLOW STUDIO
    - Find the line that marks the END of the Workflow Studio block or when the section is no longer about Workflow Studio or its pricing.
        - This is the first line AFTER the Workflow Studio section that is clearly a new section (such as '# Selected Services and Pricing: Watchlist', '# Identity Document Verification', etc.).
    ### Output
    - Output a JSON object with two fields: "start_line" (the exact text of the start line), and "end_line" (the exact text of the end line; use "" if there is no subsequent section).
    - If the Workflow Studio section does not exist, output {{"start_line": "", "end_line": ""}}.

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

    async def extract_full_workflowstudio_chunk_by_llm(self, full_text):
        boundaries = await self.call_llm_for_workflowstudio_boundaries(full_text)
        print("\n==== WORKFLOW STUDIO BOUNDARIES LLM OUTPUT ====")
        print(json.dumps(boundaries, indent=2, ensure_ascii=False))

        start_line = boundaries.get("start_line", "").strip()
        end_line = boundaries.get("end_line", "").strip()

        if not start_line:
            return ""  # No section found

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
        print("\n==== WORKFLOW STUDIO CHUNK EXTRACTED ====")
        print(chunk if chunk else "No Workflow Studio chunk found.")
        return chunk


    async def extract_workflowstudio_consumption_from_llm(self, chunk):
        outputs = []
        if chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(1)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.WORKFLOWSTUDIO_INSTRUCTION},
                        {"role": "user", "content": chunk}
                    ]
                )
                outputs.append(json.loads(response.choices[0].message.content.strip()))
            except Exception as e:
                print(f"Error processing Workflow Studio chunk: {e}")
        else:
            print("No Workflow Studio chunk found.")
        print("\n==== WORKFLOW STUDIO EXTRACTION LLM OUTPUT ====")
        print(json.dumps(outputs, indent=2, ensure_ascii=False))
        return outputs

    def transform_workflowstudio_to_flat_records(self, outputs, contractExternalId, ContractName, StartDate, product_id=None, note=""):
        subscriptions = []
        sub_cs = []
        sub_cr = []

        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
                    subExternalId = f"wfstudio_sub_{contractExternalId}"
                    subCsExternalId = f"wfstudio_subcs_{contractExternalId}"
                    # Subscription-level flat
                    sub_flat = {
                        "subExternalId": subExternalId,
                        "ProductName": subscription.get("ProductName", ""),
                        "ContractExternalId": contractExternalId,
                        "ContractName": ContractName,
                        "CurrencyIsoCode": subscription.get("CurrencyIsoCode", ""),
                        "SBQQ__SubscriptionStartDate__c": StartDate,
                        "ProductId": product_id,
                        "Note": note
                    }
                    subscriptions.append(sub_flat)

                    sub_cs_flat = {
                        "subCsExternalId": subCsExternalId,
                        "subExternalId": subExternalId,
                        "subscriptionName": subscription.get("ProductName", ""),
                        "subCsName": subscription.get("ProductName", "") + " Direct Consumption Schedule",
                        "CurrencyIsoCode": subscription.get("CurrencyIsoCode", ""),
                        "RatingMethod__c": "Tier",
                        "Type__c": "Range"
                    }
                    sub_cs.append(sub_cs_flat)

                    for i, scr in enumerate(subscription.get("scr", []), 1):
                        subCrExternalId = f"wfstudio_subcr{i}_{contractExternalId}"
                        sub_cr_flat = {
                            "subCrExternalId": subCrExternalId,
                            "subCrName": scr.get("subCrName", ""),
                            "subExternalId": subExternalId,
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


    def fetch_product2_fields(self, sf, product_names, batch_size=100):
        if not product_names:
            return {}
        product_names = list(product_names)
        all_records = {}
        for i in range(0, len(product_names), batch_size):
            batch = product_names[i:i+batch_size]
            # Always strip names
            batch = [name.strip() for name in batch]
            names_str = "', '".join(name.replace("'", r"\'") for name in batch)
            soql = (
                "SELECT Id, Name, SBQQ__PricingMethod__c, SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, "
                "SBQQ__BillingFrequency__c, SBQQ__ChargeType__c, SBQQ__BillingType__c, CreatedDate "
                "FROM Product2 "
                f"WHERE IsActive = TRUE AND Name IN ('{names_str}') "
                "ORDER BY Name, CreatedDate DESC"
            )
            print("\n[DEBUG] SOQL QUERY:\n", soql)
            res = sf.query_all(soql)
            for r in res['records']:
                print("[DEBUG] Got record:", repr(r['Name']))
                all_records.setdefault(r['Name'], []).append(r)
        print("[DEBUG] Final product mapping:", all_records)
        return all_records

    def update_subscription_product_fields(self, subscriptions, product_field_map, note_map=None):
        """
        Update each subscription dict in subscriptions (a list of dicts)
        with Product2 Salesforce fields from product_field_map (name -> [record(s)]).

        Optionally update 'Note' from note_map if provided.
        """
        extra_fields = [
            "SBQQ__PricingMethod__c",
            "SBQQ__SubscriptionPricing__c",
            "SBQQ__SubscriptionType__c",
            "SBQQ__BillingFrequency__c",
            "SBQQ__ChargeType__c",
            "SBQQ__BillingType__c"
        ]
        for sub in subscriptions:
            name = sub.get("ProductName", "")
            records = product_field_map.get(name, [])
            if records:
                # If duplicates, records are sorted by CreatedDate DESC so take the first
                record = records[0]
                sub["ProductId"] = record.get("Id")
                for field in extra_fields:
                    sub[field] = record.get(field, "")
                sub["Note"] = "Successfully Matched"  
            else:
                sub["ProductId"] = ""
                for field in extra_fields:
                    sub[field] = ""
                if note_map and name in note_map:
                    sub["Note"] = note_map[name]
                else:
                    sub["Note"] = "Product not found"


    async def main(self, parsed_input, output_all_json):

        # 0. Sanity check
        if not isinstance(output_all_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")
        full_text = parsed_input

        contract_data = output_all_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")

        # 1. Extract Workflow Studio chunk and run LLM extraction
        chunk = await self.extract_full_workflowstudio_chunk_by_llm(full_text)
        outputs = await self.extract_workflowstudio_consumption_from_llm(chunk)

        # 2. Gather all ProductNames and handle special mapping
        all_names = set()
        name_remap = {}
        for doc in outputs:
            if "subscription" in doc:
                for sub in doc["subscription"]:
                    pname = sub.get("ProductName", "")
                    all_names.add(pname)
                    if pname == "Navigator & Training Materials":
                        all_names.add("Navigator & Training Material")
                        name_remap["Navigator & Training Materials"] = "Navigator & Training Material"

        # 3. Query Salesforce for all names (including mapped ones)
        try:
            sf = Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            product_field_map = self.fetch_product2_fields(sf, all_names)
        except Exception as e:
            print(f"Salesforce query failed: {e}")
            product_field_map = {}

        # 4. Transform LLM outputs to flat records
        transformed = self.transform_workflowstudio_to_flat_records(
            outputs, contractExternalId, ContractName, StartDate
        )

        # 5. Update the Subscription section with Product2 Salesforce fields
        for section in transformed:
            if section["name"] == "Subscription":
                # Apply name remap if needed
                for sub in section["data"]:
                    orig = sub.get("ProductName", "")
                    if orig in name_remap:
                        sub["ProductName"] = name_remap[orig]
                # Now update all fields from product_field_map
                self.update_subscription_product_fields(section["data"], product_field_map)

        # 6. Merge into contract_json as usual
        merged = self.merge_into_contract_json(output_all_json, transformed)
        return merged