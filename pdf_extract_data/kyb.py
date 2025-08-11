import json
import asyncio
from openai import AsyncOpenAI
from config import get_ssm_param

class KYB:
    def __init__(self):
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.DOCV_INSTRUCTION = get_ssm_param("/myapp/docv_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")

        self.KYB_BOUNDARIES_INSTRUCTION = """
        You are an expert contract parser. 
        Your job is to find the exact line text that marks the START and END of the Business Verification section.

        # Instructions:
        ### START OF BUSINESS VERIFICATION ###
        - Find the line that marks the START of the Business Verification block. This is a line containing "## Selected Services and Pricing: Business Verification" (or similar such as "Selected Services & Pricing: Business Verification". There may be slight naming variations).
        - While there may be naming variations, there may also be a lot of 'Selected Services and Pricing:' headers. make sure you are extracting for Business Verification.
        - Do not extract from Person Match, Watchlist, Identity Document Verification, Workflow Studio, General Terms and Conditions or anything else. BUSINESS VERIFICATION ONLY.
        ### END OF BUSINESS VERIFICATION
        - Find the line that marks the END of the Business Verification block or when the section is no longer about Business Verification.
            - This is the first line AFTER the Business Verification section that is clearly a new section (such as 'Selected Services and Pricing: Watchlist', 'Identity Document Verification', 'Workflow Studio', 'eneral Terms and Conditions', 'Appendix A' etc.).
        ### Output
        - Output a JSON object with two fields: "start_line" (the exact text of the start line), and "end_line" (the exact text of the end line; use "" if there is no subsequent section).
        - If the Business Verification section does not exist, output {"start_line": "", "end_line": ""}.
        """

        self.KYB_INSTRUCTION = """
        You are an intelligent field extractor. You are given a chunk of a document that contains the Business Verification pricing table.

        ## `subscription` (subscription level fields)
        For each row in the pricing table, create a subscription for each column that has a price value (such as but not limited to: Search, Essentials, Insights, Complete.)
        - `ProductName`: Combine the Query Type (column) with the Group Number (row). Format: "Business <Query Type> - GR<Group Number>"
        Examples: "Business Essentials - GR1", "Business Insights - GR1", "Business Complete - GR2", "Business Search - GR5"

        ## `scr` (subscription consumption rate for this subscription)
        For each subscription created above, create one scr record:
        - `Price__c`: Extract the numerical price value without currency symbol

        ### IMPORTANT RULES:
        - Only create subscriptions for columns that have actual price values.
        - Skip the "Additional Services" column as it has descriptive text, not simple prices
        - Each Group + Query Type combination becomes one subscription
        - Extract only the numerical value from prices (e.g., "0.50" from "$0.50")

        Return the extracted data as a structured JSON object:
        ```json
        {
        "subscription": [
            {
            "ProductName": ,
            "scr": [
                {
                "subCrName": ,
                "Price__c": ,
                }
            ]
            }
        ]
        }
        ```
        """

    async def call_llm_for_kyb_boundaries(self, full_text):
        """
        Uses LLM to find the start and end line (as exact line text) of the Business Verification section.
        Returns a dict: {"start_line": "...", "end_line": "..."} or {"start_line": "", "end_line": ""} if not found.
        """
        user_prompt = f"""
    DOCUMENT:
    ---
    {full_text}
        """
        # Add delay before API call
        await asyncio.sleep(1)

        response = await self.openai.chat.completions.create(
            model="gpt-4.1",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": self.KYB_BOUNDARIES_INSTRUCTION},
                {"role": "user", "content": user_prompt}
            ]
        )
        content = response.choices[0].message.content
        data = json.loads(content if content is not None else '{}')
        
        print("\n==== BUSINESS VERIFICATION BOUNDARIES LLM OUTPUT ====")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        return data        
    
    async def extract_full_kyb_chunk_by_llm(self, full_text):
        """
        Returns the chunk of text between the start and end lines for Business Verification, or "" if not found.
        """
        boundaries = await self.call_llm_for_kyb_boundaries(full_text)
        start_line = (boundaries.get("start_line") or "").strip()
        end_line = (boundaries.get("end_line") or "").strip()
        if not start_line:
            print("\n==== BUSINESS VERIFICATION CHUNK EXTRACTED ====")
            print("No Business Verification section found.")
            return ""
        lines = full_text.splitlines()
        try:
            start_idx = lines.index(start_line)
        except ValueError:
            print("\n==== BUSINESS VERIFICATION CHUNK EXTRACTED ====")
            print("Start line not found in document.")
            return ""
        end_idx = lines.index(end_line) if end_line and end_line in lines else len(lines)
        chunk = "\n".join(lines[start_idx:end_idx]).strip()
        
        print("\n==== BUSINESS VERIFICATION CHUNK EXTRACTED ====")
        print(chunk if chunk else "No Business Verification chunk found.")
        
        return chunk
    

    async def extract_kyb_from_llm(self, chunk):
        """
        Uses LLM to extract Business Verification pricing data from the chunk.
        Returns a list of extracted outputs.
        """
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
                        {"role": "system", "content": self.KYB_INSTRUCTION},
                        {"role": "user", "content": chunk}
                    ]
                )
                content = response.choices[0].message.content
                outputs.append(json.loads(content if content is not None else '{}'))
                
                print("\n==== BUSINESS VERIFICATION EXTRACTION LLM OUTPUT ====")
                print(json.dumps(outputs, indent=2, ensure_ascii=False))
                
            except Exception as e:
                print(f"Error processing Business Verification chunk: {e}")
        else:
            print("No Business Verification chunk found.")
        return outputs
    
    def transform_to_flat_records(self, outputs, contractExternalId, ContractName, StartDate):
        """
        Transforms LLM outputs to flat records format for Business Verification data.
        """
        subscriptions = []
        sub_cs = []
        sub_cr = []

        for doc in outputs:
            if "subscription" in doc:
                for idx, subscription in enumerate(doc["subscription"], 1):
                    subExternalId = f"kyb_sub_{idx}_{contractExternalId}"
                    subCsExternalId = f"kyb_subcs_{idx}_{contractExternalId}"
                    
                    # Subscription-level flat
                    sub_flat = {
                        "subExternalId": subExternalId,
                        "ProductName": subscription.get("ProductName", ""),
                        "ContractExternalId": contractExternalId,
                        "ContractName": ContractName,
                        "CurrencyIsoCode": "USD",
                        "SBQQ__SubscriptionStartDate__c": StartDate,
                        "ProductId": "",  # Placeholder for Salesforce
                        "Note": ""  # Placeholder for Salesforce
                    }
                    subscriptions.append(sub_flat)

                    # Sub Consumption Schedule
                    sub_cs_flat = {
                        "subCsExternalId": subCsExternalId,
                        "subExternalId": subExternalId,
                        "subscriptionName": subscription.get("ProductName", ""),
                        "subCsName": subscription.get("ProductName", "") + " Consumption Schedule",
                        "CurrencyIsoCode": "USD",
                        "RatingMethod__c": "Tier",
                        "Type__c": "Range"
                    }
                    sub_cs.append(sub_cs_flat)

                    # Sub Consumption Rate
                    for scr in subscription.get("scr", []):
                        subCrExternalId = f"kyb_subcr_{idx}_{contractExternalId}"
                        sub_cr_flat = {
                            "subCrExternalId": subCrExternalId,
                            "subCrName": subscription.get("ProductName", "") + " Consumption Rates",
                            "subExternalId": subExternalId,
                            "subscriptionName": subscription.get("ProductName", ""),
                            "CurrencyIsoCode": "USD",
                            "Price__c": scr.get("Price__c", ""),
                            "LowerBound__c": "1",
                            "UpperBound__c": "NA",
                        }
                        sub_cr.append(sub_cr_flat)

        return [
            {"name": "Subscription", "data": subscriptions},
            {"name": "subConsumptionSchedule", "data": sub_cs},
            {"name": "subConsumptionRate", "data": sub_cr}
        ]
    
    def merge_into_contract_json(self, contract_json, extracted_output):
        """
        Merges extracted Business Verification data into the contract JSON.
        """
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
    

    def fetch_product2_fields(self, product_names):
        """
        Fetches Product2 fields from Salesforce for given product names.
        """
        if not product_names:
            return {}
        
        try:
            from simple_salesforce.api import Salesforce
            sf = Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            
            # Create the SOQL query
            like_clauses = []
            for name in product_names:
                if name.startswith('Business Documents'):
                    like_clauses.append("Name LIKE 'Business Documents%'")
                else:
                    like_clauses.append("Name LIKE '" + name.replace("'", "\\'") + "%'")
            where_clause = " OR ".join(like_clauses)
            soql = f"""
            SELECT Id, Name, ProductCode, CreatedDate, SBQQ__PricingMethod__c, SBQQ__BillingFrequency__c, 
                SBQQ__BillingType__c, SBQQ__ChargeType__c, SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c
            FROM Product2 
            WHERE ({where_clause}) AND IsActive = true 
            ORDER BY Name, CreatedDate DESC
            """
            
            print(f"\n[DEBUG] SOQL QUERY:\n{soql}")
            result = sf.query(soql)
            
            # Group by Name and take the latest CreatedDate
            product_map = {}
            for record in result['records']:
                name = record['Name']
                if name not in product_map:
                    product_map[name] = record
                else:
                    # If multiple records, keep the one with latest CreatedDate
                    if record['CreatedDate'] > product_map[name]['CreatedDate']:
                        product_map[name] = record
            
            return product_map
            
        except Exception as e:
            print(f"Salesforce query failed: {e}")
            return {}
        
    def update_subscription_product_fields(self, subscriptions, product_map):
        """
        Update each subscription dict with Product2 Salesforce fields.
        """
        extra_fields = [
            "SBQQ__PricingMethod__c",
            "SBQQ__BillingFrequency__c", 
            "SBQQ__BillingType__c",
            "SBQQ__ChargeType__c",
            "SBQQ__SubscriptionPricing__c",
            "SBQQ__SubscriptionType__c"
        ]
        
        for sub in subscriptions:
            name = sub.get("ProductName", "")
            record = product_map.get(name)
            
            if record:
                sub["ProductId"] = record.get("Id", "")
                # Add the extra fields
                for field in extra_fields:
                    sub[field] = record.get(field, "")
                # Check if there were multiple records for this name
                if len([r for r in product_map.values() if r['Name'] == name]) > 1:
                    sub["Note"] = "Duplicates found, extracted latest CreatedDate"
                else:
                    sub["Note"] = "Successfully Matched"
            else:
                sub["ProductId"] = ""
                # Add empty values for extra fields
                for field in extra_fields:
                    sub[field] = ""
                sub["Note"] = "Product not found"


    async def main(self, markdown_text, contract_json):
        """
        Main function to extract Business Verification data and merge into contract JSON.
        Args:
            markdown_text (str): The markdown text containing the contract
            contract_json (dict): The contract JSON object to merge results into
        """
        # 0. Sanity check
        if not isinstance(contract_json, dict):
            raise TypeError("contract_json must be a Python dict (not a file path).")
        
        # Get contract info
        contract_data = contract_json["output_records"][0]["data"][0]
        contractExternalId = contract_data.get("ContractExternalId", "")
        ContractName = contract_data.get("AccountName", "")
        StartDate = contract_data.get("StartDate", "")

        # 1. Extract Business Verification chunk and run LLM extraction
        chunk = await self.extract_full_kyb_chunk_by_llm(markdown_text)
        outputs = await self.extract_kyb_from_llm(chunk)

        # 2. Gather all ProductNames for Salesforce query
        all_names = set()
        for doc in outputs:
            if "subscription" in doc:
                for sub in doc["subscription"]:
                    pname = sub.get("ProductName", "")
                    all_names.add(pname)

        # 3. Query Salesforce for all names
        product_map = self.fetch_product2_fields(all_names)

        # 4. Transform LLM outputs to flat records
        transformed = self.transform_to_flat_records(outputs, contractExternalId, ContractName, StartDate)

        # 4.5. Combine LLM and Salesforce subscriptions as described
        # Get LLM subscriptions from transformed
        llm_subscriptions = []
        for section in transformed:
            if section["name"] == "Subscription":
                llm_subscriptions = section["data"]

        # Build a dict of LLM subscriptions by ProductName
        llm_sub_dict = {sub["ProductName"]: sub for sub in llm_subscriptions}

        # Build a dict of Salesforce records by Name for duplicate detection
        salesforce_records_by_name = {}
        for record in product_map.values():
            pname = record["Name"]
            if pname not in salesforce_records_by_name:
                salesforce_records_by_name[pname] = []
            salesforce_records_by_name[pname].append(record)

        # Enrich LLM subscriptions with Salesforce data if available, and add Salesforce-only as new
        for pname, sub in llm_sub_dict.items():
            records = salesforce_records_by_name.get(pname, [])
            if not records:
                # No Salesforce match
                sub["ProductId"] = ""
                sub["ProductCode"] = ""
                for field in [
                    "SBQQ__PricingMethod__c",
                    "SBQQ__BillingFrequency__c",
                    "SBQQ__BillingType__c",
                    "SBQQ__ChargeType__c",
                    "SBQQ__SubscriptionPricing__c",
                    "SBQQ__SubscriptionType__c"
                ]:
                    sub[field] = ""
                sub["Note"] = f"Product not found based on ProductName: {pname}"
            else:
                # If multiple records, take the latest by CreatedDate
                record = max(records, key=lambda r: r.get("CreatedDate", ""))
                sub["ProductId"] = record.get("Id", "")
                sub["ProductCode"] = record.get("ProductCode", "")
                for field in [
                    "SBQQ__PricingMethod__c",
                    "SBQQ__BillingFrequency__c",
                    "SBQQ__BillingType__c",
                    "SBQQ__ChargeType__c",
                    "SBQQ__SubscriptionPricing__c",
                    "SBQQ__SubscriptionType__c"
                ]:
                    sub[field] = record.get(field, "")
                if len(records) > 1:
                    sub["Note"] = f"Duplicates found for ProductName: {pname}, Id taken from latest CreatedDate"
                else:
                    sub["Note"] = "Successfully Matched"

        # Add Salesforce-only ProductNames as new subscriptions
        # Get the next available index for subExternalId
        next_index = len(llm_sub_dict) + 1
        for pname, records in salesforce_records_by_name.items():
            if pname not in llm_sub_dict:
                # Take the latest by CreatedDate if duplicates
                record = max(records, key=lambda r: r.get("CreatedDate", ""))
                new_sub = {
                    "subExternalId": f"kyb_sub_{next_index}_{contractExternalId}",
                    "ProductName": pname,
                    "ProductCode": record.get("ProductCode", ""),
                    "ContractExternalId": contractExternalId,
                    "ContractName": ContractName,
                    "CurrencyIsoCode": "USD",
                    "SBQQ__SubscriptionStartDate__c": StartDate,
                    "ProductId": record.get("Id", ""),
                    "Note": "From Salesforce query"
                }
                for field in [
                    "SBQQ__PricingMethod__c",
                    "SBQQ__BillingFrequency__c",
                    "SBQQ__BillingType__c",
                    "SBQQ__ChargeType__c",
                    "SBQQ__SubscriptionPricing__c",
                    "SBQQ__SubscriptionType__c"
                ]:
                    new_sub[field] = record.get(field, "")
                llm_sub_dict[pname] = new_sub
                next_index += 1

        # Final combined list
        combined_subscriptions = list(llm_sub_dict.values())

        # Assign to output
        for section in transformed:
            if section["name"] == "Subscription":
                section["data"] = combined_subscriptions

        # Create subConsumptionSchedule and subConsumptionRate for all KYB subscriptions
        sub_consumption_schedules = []
        sub_consumption_rates = []
        
        for i, sub in enumerate(combined_subscriptions, 1):
            # Create subConsumptionSchedule record
            sub_cs = {
                "subCsExternalId": f"kyb_subcs_{i}_{contractExternalId}",
                "subExternalId": sub["subExternalId"],
                "subscriptionName": sub["ProductName"],
                "subCsName": sub["ProductName"] + " Consumption Schedule",
                "CurrencyIsoCode": "USD",
                "RatingMethod__c": "Tier",
                "Type__c": "Range"
            }
            sub_consumption_schedules.append(sub_cs)
            
            # Create subConsumptionRate record
            sub_cr = {
                "subCrExternalId": f"kyb_subcr_{i}_{contractExternalId}",
                "subCrName": sub["ProductName"] + " Consumption Rates",
                "subExternalId": sub["subExternalId"],
                "subscriptionName": sub["ProductName"],
                "CurrencyIsoCode": "USD",
                "Price__c": "0",  # Default price
                "LowerBound__c": "1",
                "UpperBound__c": "NA"
            }
            sub_consumption_rates.append(sub_cr)

        # Update or add subConsumptionSchedule and subConsumptionRate sections
        for section in transformed:
            if section["name"] == "subConsumptionSchedule":
                section["data"] = sub_consumption_schedules
                break
        else:
            transformed.append({"name": "subConsumptionSchedule", "data": sub_consumption_schedules})
            
        for section in transformed:
            if section["name"] == "subConsumptionRate":
                section["data"] = sub_consumption_rates
                break
        else:
            transformed.append({"name": "subConsumptionRate", "data": sub_consumption_rates})

        # 4.6. Build LineItemSource records for each Salesforce subscription
        # Build mapping from Product2.Id to subExternalId and subscriptionName
        id_to_subext = {}
        id_to_subname = {}
        for sub in combined_subscriptions:
            pid = sub.get("ProductId")
            id_to_subext[pid] = sub["subExternalId"]
            id_to_subname[pid] = sub["ProductName"]

        # Get contract info for StartDate and ContractTerm
        contract_section = next((rec for rec in contract_json.get("output_records", []) if rec.get("name") == "Contract"), None)
        contract_data = contract_section["data"][0] if contract_section and contract_section.get("data") else {}
        contract_start_date = contract_data.get("StartDate", "")
        contract_term = contract_data.get("ContractTerm", "")

        # Query SBQQ__ProductOption__c for each Product2.Id
        option_records = {}
        try:
            from simple_salesforce.api import Salesforce
            sf = Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            ids = list(id_to_subext.keys())
            if ids:
                ids_str = "', '".join(ids)
                soql = (
                    "SELECT Id, BaseAddOn__c, CurrencyIsoCode, OwnerId, Option_Product_Name__c, "
                    "SBQQ__ProductName__c, Component_Charge_Name__c, SBQQ__ConfiguredSKU__c, "
                    "SBQQ__OptionalSKU__c, SBQQ__ProductCode__c, CreatedDate "
                    f"FROM SBQQ__ProductOption__c WHERE SBQQ__ConfiguredSKU__c IN ('{ids_str}') "
                    "ORDER BY SBQQ__ConfiguredSKU__c, SBQQ__ProductName__c, Id, CreatedDate DESC"
                )
                print(f"[DEBUG] LIS SOQL QUERY: {soql}")
                res = sf.query_all(soql)
                for r in res['records']:
                    # Deduplicate by Id, keep latest by CreatedDate
                    rid = r['Id']
                    if rid not in option_records or r['CreatedDate'] > option_records[rid]['CreatedDate']:
                        option_records[rid] = r
        except Exception as e:
            print(f"[KYB] Error querying SBQQ__ProductOption__c: {e}")

        # Build LineItemSource records
        lineitem_sources = []
        for i, r in enumerate(option_records.values(), 1):
            # Skip records where SBQQ__ProductName__c (lisName) is null or empty
            if not r.get('SBQQ__ProductName__c'):
                continue
            pid = r.get('SBQQ__ConfiguredSKU__c')
            subext = id_to_subext.get(pid, "")
            subname = id_to_subname.get(pid, "")
            lis_id = r['SBQQ__OptionalSKU__c']
            base_addon = r.get('BaseAddOn__c', "")
            note = "Successfully Matched"
            lineitem = {
                "lisExternalId": f"kyb_lis{i}_{subext}",
                "lisName": r.get('SBQQ__ProductName__c', ""),
                "subExternalId": subext,
                "subscriptionName": subname,
                "BaseAddon__c": base_addon,
                "CurrencyIsoCode": r.get('CurrencyIsoCode', ""),
                "Description__c": "",
                "EndDate__c": "",
                "Included__c": True if base_addon == "Base" else False,
                "OwnerId": r.get('OwnerId', ""),
                "Product__c": "",
                "ProductCode__c": r.get('SBQQ__ProductCode__c', ""),
                "SortOrder__c": "",
                "StartDate__c": contract_start_date,
                "Subscription__c": subname,
                "SubscriptionTerm__c": contract_term,
                "ProductId": lis_id,
                "Option_Product_Name__c": r.get('Option_Product_Name__c', ""),
                "Component_Charge_Name__c": r.get('Component_Charge_Name__c', ""),
                "Note": note
            }
            lineitem_sources.append(lineitem)

        # Add LineItemSource to output_records
        found = False
        for section in transformed:
            if section["name"] == "LineItemSource":
                section["data"] = lineitem_sources
                found = True
        if not found:
            transformed.append({"name": "LineItemSource", "data": lineitem_sources})

        # Build lisConsumptionSchedule records for every lis
        lis_consumption_schedules = []
        for lis in lineitem_sources:
            lis_consumption_schedule = {
                "scsExternalId": f"scs_{lis['lisExternalId']}",
                "scsName": f"{lis['lisName']} Consumption Schedule",
                "subExternalId": lis['subExternalId'],
                "subscriptionName": lis['subscriptionName'],
                "lisExternalId": lis['lisExternalId'],
                "lisName": lis['lisName'],
                "CurrencyIsoCode": "USD",
                "LineItemSource__c": "",
                "RatingMethod__c": "Tier",
                "Type__c": "Range"
            }
            lis_consumption_schedules.append(lis_consumption_schedule)

        # Add lisConsumptionSchedule to output_records
        found = False
        for section in transformed:
            if section["name"] == "lisConsumptionSchedule":
                section["data"] = lis_consumption_schedules
                found = True
        if not found:
            transformed.append({"name": "lisConsumptionSchedule", "data": lis_consumption_schedules})

        # Build lisConsumptionRate records for every lis
        lis_consumption_rates = []
        for lis in lineitem_sources:
            lis_consumption_rate = {
                "scrExternalId": f"scr_{lis['lisExternalId']}",
                "scrName": f"{lis['lisName']} Consumption Rate",
                "subExternalId": lis['subExternalId'],
                "subscriptionName": lis['subscriptionName'],
                "lisExternalId": lis['lisExternalId'],
                "lisName": lis['lisName'],
                "CurrencyIsoCode": "USD",
                "OriginalPrice__c": lis.get("Price__c", ""),
                "Price__c": 0,
                "LowerBound__c": 1,
                "UpperBound__c": "NA"
            }
            lis_consumption_rates.append(lis_consumption_rate)

        # Add lisConsumptionRate to output_records
        found = False
        for section in transformed:
            if section["name"] == "lisConsumptionRate":
                section["data"] = lis_consumption_rates
                found = True
        if not found:
            transformed.append({"name": "lisConsumptionRate", "data": lis_consumption_rates})

        # 6. Merge into contract_json
        merged = self.merge_into_contract_json(contract_json, transformed)
        return merged