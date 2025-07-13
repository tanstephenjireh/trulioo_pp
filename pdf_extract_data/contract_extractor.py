import io
import re
import json
import uuid
import logging
from openai import AsyncOpenAI
from datetime import datetime
from config import get_ssm_param
import pandas as pd
import pdfplumber
import asyncio

# Configure logging
logger = logging.getLogger(__name__)

class ContractExtractor:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.SUBSCRIPTION_INSTRUCTIONS = get_ssm_param("/myapp/subscription_prompt")
        # self.CONTRACT_INSTRUCTIONS = get_ssm_param("/myapp/contract_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.CONTRACT_INSTRUCTIONS = """
        You are an intelligent field extractor. You are given a document that may contain one or more service subscriptions grouped under a single contract. Your task is to extract all relevant fields and return a **single structured JSON object** in a **nested format**, organized as follows:

        - The top level represents the **contract**.

        If a value is not present, return `"NA"`. Do not leave any field blank.

        ---

        ## CONTRACT-LEVEL FIELDS (top-level keys in the JSON)

        Extract the following fields based on their respective sections in the document:

        ### CUSTOMER INFORMATION
        Extract from the **Customer Information** section. If the field is not explicitly listed, return "NA":

        - `AccountName`: The name of the Customer, always before the ("Customer") string and the first line under Customer Information.
        - `BillingStreet`
        - `BillingCity`
        - `BillingState`
        - `BillingPostalCode`
        - `BillingCountry`

        ### GENERAL SERVICE FEES
        Extract from the **General Service Fees** section:

        - `ImplementationFee`: extract the numerical price under the item name "Implementation Fee". 0 if waived.
        - `LicenseFee`: extract the numerical price under the item name "License Fee". 0 if waived.


        ### FEES AND PAYMENT TERMS
        Extract from the **Fees and Payment Terms** section:

        - `ContractTerm`: extract the number of months from the line beginning with “Initial Term”
        - `CurrencyIsoCode`: get the ISO currency code from the “Prepaid Usage Credit”. 
            - If "$", it is automatically USD. Do not base currency on country.
        - `PaymentMethod__c`: extract from the field labeled “Payment Method”
        - `PrepaidCredits__c`: extract the numeric value of “Prepaid Usage Credit”; if none, return "NA"
        - `Minimum_Monthly__c`: extract the number from “Monthly Minimum Commitment”; if none, return "NA"

        ### FEES AND PAYMENT TERMS or ### GENERAL TERMS AND CONDITIONS OR ### TERMS AND CONDITIONS
        Extract from the **Fees and Payment Terms** section or signature date under **General Terms and Conditions** section
        - `StartDate`: (formatted as YYYY-MM-DD)
            - Also exact date under "Effective Date" under "# Fees and Payment Terms" if there is an exact date. If none,
            - Check "# General Terms and Conditions" or # Terms and Conditions section, and extract the **latest date** found in the signature section of either Trulioo or the Customer Authorized Representative.
            - Most likely, there is a present date, in either of the two.

        """

        self.SUBSCRIPTION_INSTRUCTIONS = """
        You are an intelligent field extractor. You are given a document containing a single **subscription block** for a specific country. Your task is to extract the relevant subscription and list item fields, and return a structured JSON object in the following format:

        ```json
        {
        "subscriptions": [
            {
            <Subscription-level fields>,
            "listitemsource": [ ...list items for this subscription... ]
            }
        ]
        }
        ```

        If a value is not present, return \"NA\". Do not leave any field blank.

        IMPORTANT: Always use the exact country name as it appears in the input document for the subscriptionName field. Do not abbreviate, translate, or normalize it.
        ---

        ## `subscriptions` (list of subscription blocks grouped by country)

        **For each subscription**, extract the following:

        - `subscriptionName`: the country where this subscription applies
        - `CurrencyIsoCode`: the currency used in the "Fee per Query" rows of this subscription.
            - If "$", it is automatically USD. Do not base currency on country.
        - `subCsName`: set as `"<subscriptionName> Transactions - Direct Consumption Schedules"`
        - `subCrName`: set as `"<subscriptionName> Transactions - Consumption Rate"`
        -  Price__c: the numeric value of the "Fee per Query" for the Base Configuration in the subscription block (usually the first row). NA if there is no Base Configuration.
        ---

        ### `listitemsource` (inside each subscription block)

        This is a list of rows representing the selected services under each subscription block.

        **Each row represents an item listed under the "Selected Services and Pricing: Person Match" section of the document.**

        For each row, extract:

        - `lisName`: from the "Name" field of the row
        - `BaseAddon__c`:  
        - "Base" if Type is "Base" or "In-Base"  
        - "Add-On" if Type is "In Additional" or "Additional"
        - `CurrencyIsoCode`: the currency under the "Fee per Query" column
            - If "$", it is automatically USD. Do not base currency on country.
        - `Description__c`: from the "Comments" column
        - `Included__c`:  
        - "TRUE" if the "Fee per Query" is marked "Included"  
        - "FALSE" otherwise
        - `scsName`: set as `"<lisName> Consumption Schedule"`
        - `scrName`: set as `"<lisName> Consumption Rate"`
        - `Price__c`: the numeric value from "Fee per Query", or 0 if "Included"
        """
    

    # ========== FIELD EXTRACTION LOGIC ==========

#############################################################################################

    async def extract_contract_fields(self, text):

        # Add delay before API call
        await asyncio.sleep(1)

        response = await self.openai.chat.completions.create(
            model="gpt-4.1",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": self.CONTRACT_INSTRUCTIONS},
                {"role": "user", "content": text}
            ]
        )
            # Print the raw JSON string returned by the model
        print("\n===== Raw LLM Extract Contract Output =====")
        print(response.choices[0].message.content.strip())
        return json.loads(response.choices[0].message.content.strip())


    # ===== SUBSCRIPTIONS
    async def call_llm_for_chunk_boundaries(self, full_doc_text):
        system_prompt = (
            "You are an expert document parsing assistant. "
            "Your job is to extract country boundaries in the 'Selected Services and Pricing: Person Match' section, "
            "even if there are unrelated headers inside that section."
        )

        user_prompt = f"""
    You are given a contract document in markdown or plain text.  
    Your task is to **find all country blocks** under the section titled 'Selected Services and Pricing: Person Match'.
    The section header may be written in different ways such as use of ampersand, different spacing but similar to Selected Services and Pricing: Person Match


    # INSTRUCTIONS

    ## 1. SCOPE
    - Only consider content after the line that marks the start of 'Selected Services and Pricing: Person Match' (any case variation is fine).
    - Ignore unrelated headers within this section (like '# Query Fees by Country and Type', '# General', '# Query Fee Information', etc).
    - Continue processing until you reach the start of a truly new section, such as:
        - 'General Terms and Conditions'
        - 'Selected Services and Pricing: Workflow Studio'
        - 'Selected Services and Pricing: Watchlist'
        - 'Identity Document Verification'
        - or other top-level 'Selected Services' sections.

    ## 2. IDENTIFY COUNTRY BLOCKS
    - A country block **always begins** with a line that is clearly a country header, such as:
        - A line with only a country name (e.g. "Norway")
        - Or the country name marked as a header ("# Norway", "## Norway", "**Norway**", etc.)
    - A valid country block **must** have a markdown table immediately after the country header. The table should contain at least the columns 'Name', 'Type', and 'Fee per Query'. The table may optionally include a 'Comments' column.
    - There may be other markdown headers or lines between country blocks—ignore them unless they mark the start of a new section as defined above.

    ## 3. FINDING END OF BLOCK
    - The end of a country block is marked by the **start line** of the next country header (per the above definition).
    - If there are no more country blocks, the end is the start line of the next true section (see SCOPE above), or the end of the document.

    ## 4. OUTPUT FORMAT
    - For each country block, output an object:
        - "country": the country name as it appears (do not normalize or translate)
        - "start_line_text": the exact line marking the start of this country block (the country header)
        - "end_line_text": the exact line marking the start of the next country block, or the start of the next section, or "END" if at the end of document
    - Return a single JSON object with key "boundaries" and a list of these objects, **in order of appearance**.

    ## 5. IMPORTANT RULES
    - **DO NOT** end a country block on an unrelated header (such as '# Query Fees by Country and Type')—these are not true section ends.
    - **DO NOT** stop processing until you reach a major new section or end of the document.
    - **DO NOT** output any explanation, commentary, or formatting—only the JSON object as specified.
    - **Never skip a country/table just because of intervening markdown headers.**

    ## 6. EXAMPLES OF TRUE SECTION ENDS
    - "# General Terms and Conditions"
    - "# Selected Services and Pricing: Workflow Studio"
    - "# Selected Services and Pricing: Watchlist"
    - "# Identity Document Verification"
    - "# Fees and Payment Terms"
    - or similar top-level headings.

    DOCUMENT:
    ---
    {full_doc_text}
    """
        # Add delay before API call
        await asyncio.sleep(1)

        response = await self.openai.chat.completions.create(
            model="gpt-4.1",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        data = json.loads(response.choices[0].message.content)
        return data["boundaries"]
    
    def chunk_doc_by_country_boundaries(self, md_text, boundaries):
        results = []
        remaining_text = md_text

        for i, block in enumerate(boundaries):
            country = block["country"]
            start_marker = block["start_line_text"].strip()
            end_marker = block["end_line_text"].strip()

            start_idx = remaining_text.find(start_marker)
            if start_idx == -1:
                print(f"Warning: Start marker '{start_marker}' not found for {country}")
                continue

            if end_marker == "END":
                chunk = remaining_text[start_idx:]
                remaining_text = ""
            else:
                end_idx = remaining_text.find(end_marker, start_idx + len(start_marker))
                if end_idx == -1:
                    print(f"Warning: End marker '{end_marker}' not found after '{country}', using end of document.")
                    chunk = remaining_text[start_idx:]
                    remaining_text = ""
                else:
                    chunk = remaining_text[start_idx:end_idx]
                    remaining_text = remaining_text[end_idx:]

            results.append({
                "country": country,
                "text": chunk.strip()
            })

        # Print all country chunks (optional, you can comment out if not needed)
        print(f"\n==== All Extracted Country Chunks ====")
        for chunk in results:
            print(f"\n[Country: {chunk['country']}]\n{'-'*40}\n{chunk['text']}\n{'='*40}")

        return results
    
    async def call_llm_for_extraction(self, batch_chunks):
        batch_text = "\n\n".join(chunk["text"] for chunk in batch_chunks)

        system_prompt = (
            "You are an intelligent contract field extractor. "
            "Your job is to extract structured data from contract subscription blocks."
        )
        user_prompt = f"""
    You are given chunks with up to 3 country subscription blocks below. One subscription block is one country.
    A subscription block can be from multiple CONSECUTIVE tables. Every table is part of a latest country before it.

    Return a structured JSON object in the following format:

    {{
    "subscriptions": [
        {{
        <Subscription-level fields>,
        "listitemsource": [ ...list items for this subscription... ]
        }}
    ]
    }}

    IMPORTANT: Always use the exact country name as it appears in the input document for the subscriptionName field. Do not abbreviate, translate, or normalize it.
    ---

    ## Edgecase: Multiple Tables belonging to one Subscription block ##
    - If there is a markdown table in the text that is NOT directly under a country header, it MUST be included as part of the most recent preceding country’s subscription block.
    - Even if a table has a header or text above it, and is not a country, treat it as a continuation of the previous table, and under the most recent country.
    - Never leave any table or list item unassigned. Every table belongs to the country that came before it.
    Example:
    ## Country

    | Name                  | Type    | Fee per Query                                |
    |-----------------------|---------|----------------------------------------------|
    | Line Item Source 1 | Base    | $<price>                              |
    ```

    ## General
    ```markdown
    <lines in between that are not country>

    | Name           | Type     | Fee per Query                                     |
    |----------------|----------|--------------------------------------------------|
    | Line Item Source 2  | In Base  | Included in Fee per query for Base Configuration |

    - From the example, the table with Line Item Source 2 is a continuation of previous table since it does not have a country above it,
    - Every table belongs to the country that came before it. Do not disregard any table.

    ---

    # Field extraction guidelines
    ## `subscriptions` (list of subscription blocks grouped by country)
    **For each subscription**, extract the following:

    - `subscriptionName`: the country where this subscription applies
    - `CurrencyIsoCode`: the currency ISO code used in the "Fee per Query" rows of this subscription.
        - If "$", it is automatically USD. Do not base currency on country.
    - `subCsName`: set as `"<subscriptionName> Transactions - Direct Consumption Schedules"`
    - `subCrName`: set as `"<subscriptionName> Transactions - Consumption Rate"`
    - `Price__c`: the numeric value of the "Fee per Query" for the Base Configuration in the subscription block (usually the first row). NA if there is no Base Configuration.

    ---

    ### `listitemsource` (inside each subscription block)

    This is a list of rows representing the selected services under each subscription block.

    For each row, extract:

    - `lisName`: from the "Name" field of the row
    - `BaseAddon__c`:  
    - "Base" if Type is "Base" or "In-Base"  
    - "Add-On" if Type is "In Additional" or "Additional"
    - `CurrencyIsoCode`: the currency under the "Fee per Query" column
        - If "$", it is automatically USD. Do not base currency on country.
    - `Description__c`: from the "Comments" column
    - `Included__c`:  
    - "TRUE" if the "Fee per Query" is marked "Included"  
    - "FALSE" otherwise
    - `scsName`: set as `"<lisName> Consumption Schedule"`
    - `scrName`: set as `"<lisName> Consumption Rate"`
    - `Price__c`: the numeric value from "Fee per Query", or 0 if "Included"

    - If a value is missing, use "NA". Do not leave any field blank.
    - If there are fewer than 3 countries, just return as many as you find.

    TEXT:
    ---
    {batch_text}
    """
        # Add delay before API call
        await asyncio.sleep(1)

        response = await self.openai.chat.completions.create(
            model="gpt-4.1",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        data = json.loads(response.choices[0].message.content)
        return data  # Will have "subscriptions" as key
    
    async def extract_all_subscriptions_from_chunks(self, country_chunks, batch_size=3):
        """
        Given all country_chunks [{country, text}], returns all extracted subscription blocks.
        Prints the country chunk contents and LLM response per batch.
        """
        all_subs = []
        for batch in self.batch_chunks(country_chunks, batch_size):
            countries = [c['country'] for c in batch]
            print(f"\n--- Extracting batch: {countries} ---")

            # Print the chunk texts
            for chunk in batch:
                print(f"\n[Chunk: {chunk['country']}]\n{'-'*40}\n{chunk['text']}\n{'='*40}")

            res = await self.call_llm_for_extraction(batch)

            # Print the LLM response for this batch (pretty-printed)
            print(f"\n[LLM Response for batch: {countries}]\n{'-'*50}\n{json.dumps(res, indent=2, ensure_ascii=False)}\n{'='*50}")

            all_subs.extend(res["subscriptions"])
        return all_subs

    def batch_chunks(self, chunks, batch_size=3):
        """Yield batches of up to batch_size."""
        for i in range(0, len(chunks), batch_size):
            yield chunks[i:i+batch_size]

    # ========== ENRICHMENT ==========
    def enrich_llm_response(self, llm_response):
        contractExternalId = f"{str(uuid.uuid4())}"
        llm_response["contractExternalId"] = contractExternalId

        subscriptions = llm_response.get("subscriptions", [])
        for i, subscription in enumerate(subscriptions, start=1):
            sub_external_id = f"sub{i}_{contractExternalId}"
            subcs_external_id = f"subcs{i}_{contractExternalId}"
            subcr_external_id = f"subcr{i}_{contractExternalId}"

            subscription["subExternalId"] = sub_external_id
            subscription["subCsExternalId"] = subcs_external_id
            subscription["subCrExternalId"] = subcr_external_id

            non_base_count = 1
            for item in subscription.get("listitemsource", []):
                if "base configuration" in item.get("lisName", "").strip().lower():
                    continue
                item["lisExternalId"] = f"lis{non_base_count}_{sub_external_id}"
                item["scsExternalId"] = f"scs{non_base_count}_{sub_external_id}"
                item["scrExternalId"] = f"scr{non_base_count}_{sub_external_id}"
                non_base_count += 1

        contractExternalId = llm_response.get("contractExternalId", "NA")

        return llm_response
    
    # === CREATION OF DATAFRAMES ===
    def create_contract_dataframe(self, llm_response):
        contract_fields = {
            "AccountId": "",
            "AccountName": llm_response.get("AccountName", "NA"),
            "ContractExternalId": llm_response.get("contractExternalId", "NA"),  
            "BillingCity": llm_response.get("BillingCity", "NA"),
            "BillingCountry": llm_response.get("BillingCountry", "NA"),
            "BillingPostalCode": llm_response.get("BillingPostalCode", "NA"),
            "BillingState": llm_response.get("BillingState", "NA"),
            "BillingStreet": llm_response.get("BillingStreet", "NA"),
            "ContractTerm": llm_response.get("ContractTerm", "NA"),
            "CurrencyIsoCode": llm_response.get("CurrencyIsoCode", "NA"),
            "Description": "Test Data Load",
            "Minimum_Monthly__c": llm_response.get("Minimum_Monthly__c", "NA"),
            "OwnerId": "",
            "PaymentMethod__c": llm_response.get("PaymentMethod__c", "NA"),
            "PrepaidCredits__c": llm_response.get("PrepaidCredits__c", "NA"),
            "ImplementationFee": llm_response.get("ImplementationFee", "NA"),
            "RecordTypeId": "",
            "SBQQ__RenewalOpportunity__c": "",
            "SBQQ__RenewalTerm__c": "",
            "ShippingCity": "",
            "ShippingCountry": "",
            "ShippingPostalCode": "",
            "ShippingState": "",
            "ShippingStreet": "",
            "StartDate": llm_response.get("StartDate", "NA"),
            "Status": "",
            "Pricebook2Id": "",
            "SBQQ__AmendmentPricebookId__c": "",
            "SBQQ__AmendmentRenewalBehavior__c": "",
            "SBQQ__DefaultRenewalContactRoles__c": "",
            "SBQQ__DefaultRenewalPartners__c": "",
            "SBQQ__DisableAmendmentCoTerm__c": "",
            "SBQQ__Evergreen__c": "",
            "SBQQ__MasterContract__c": "",
            "SBQQ__PreserveBundleStructureUponRenewals__c": "",
            "SBQQ__RenewalForecast__c": "",
            "SBQQ__RenewalPricebookId__c": "",
            "SBQQ__RenewalQuoted__c": ""
        }
        return pd.DataFrame([contract_fields])
    
    def create_subscription_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])
        

        records = []
        for entry in subscriptions:

            record = {
                "subExternalId": entry.get("subExternalId", "NA"),
                "ProductName": entry.get("subscriptionName", "NA"),
                "ContractExternalId": llm_response.get("contractExternalId", "NA"),
                "ContractName": llm_response.get("AccountName", "NA"),
                "CurrencyIsoCode": entry.get("CurrencyIsoCode", "NA"),
                "LicenseFee": llm_response.get("LicenseFee", "NA"), 
                "_InvoiceSchedule__c": "",
                "OwnerId": "",
                "SBQQ__Account__c": "",
                "SBQQ__Contract__c": "",
                "SBQQ__Discount__c": "",
                "SBQQ__ListPrice__c": "",
                "SBQQ__Number__c": "",
                "SBQQ__OrderProduct__c": "",
                "SBQQ__PricingMethod__c": "",
                "SBQQ__Product__c": "",
                "SBQQ__ProductSubscriptionType__c": "",
                "SBQQ__Quantity__c": "",
                "SBQQ__SubscriptionEndDate__c": "",
                "SBQQ__SubscriptionStartDate__c": llm_response.get("StartDate", "NA"),
                "SBQQ__BillingFrequency__c": "",
                "SBQQ__BillingType__c": "",
                "SBQQ__Bundle__c": "",
                "SBQQ__Bundled__c": "",
                "SBQQ__ChargeType__c": "",
                "SBQQ__NetPrice__c": "",
                "SBQQ__ProrateMultiplier__c": "",
                "SBQQ__RegularPrice__c": "",
                "SBQQ__RenewalQuantity__c": "",
                "SBQQ__RequiredById__c": "",
                "SBQQ__RequiredByProduct__c": "",
                "SBQQ__SubscriptionPricing__c": "",
                "SBQQ__SubscriptionType__c": ""
            }
            records.append(record)

        return pd.DataFrame(records)
    
    def create_line_item_source_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])

        records = []
        for sub in subscriptions:
            list_items = sub.get("listitemsource", [])
            for item in list_items:
                if "base configuration" in (item.get("lisName", "")).lower():
                    continue

                record = {
                    "lisExternalId": item.get("lisExternalId", "NA"),
                    "lisName": item.get("lisName", "NA"),
                    "subExternalId": sub.get("subExternalId", "NA"),
                    "subscriptionName": sub.get("subscriptionName", "NA"),
                    "BaseAddon__c": item.get("BaseAddon__c", "NA"),
                    "CurrencyIsoCode": item.get("CurrencyIsoCode", "NA"),
                    "Description__c": item.get("Description__c", "NA"),
                    "EndDate__c": "",
                    "Included__c": item.get("Included__c", "NA"),
                    "OwnerId": "",
                    "Product__c": "",
                    "ProductCode__c": "",
                    "SortOrder__c": "",
                    "StartDate__c": llm_response.get("StartDate", "NA"),
                    "Subscription__c": "",
                    "SubscriptionTerm__c": llm_response.get("ContractTerm", "NA")
                }
                records.append(record)

        return pd.DataFrame(records)
    
    def create_subscription_consumption_schedule_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])

        records = []
        for sub in subscriptions:
            record = {
                "subCsExternalId": sub.get("subCsExternalId", "NA"),
                "subCsName": sub.get("subCsName", "NA"),
                "subExternalId": sub.get("subExternalId", "NA"),
                "subscriptionName": sub.get("subscriptionName", "NA"),
                "CurrencyIsoCode": sub.get("CurrencyIsoCode", "NA"),
                "LineItemSource__c": "",
                "RatingMethod__c": "Tier",
                "Type__c": "Range"
            }
            records.append(record)

        return pd.DataFrame(records)

    def create_subscription_consumption_rate_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])

        records = []
        for sub in subscriptions:
            record = {
                "subCrExternalId": sub.get("subCrExternalId", "NA"),
                "subCrName": sub.get("subCrName", "NA"),
                "subExternalId": sub.get("subExternalId", "NA"),
                "subscriptionName": sub.get("subscriptionName", "NA"),
                "CurrencyIsoCode": sub.get("CurrencyIsoCode", "NA"),
                "OriginalPrice__c": sub.get("OriginalPrice__c", "NA"),
                "Price__c": sub.get("Price__c", "NA"),
                "PricingMethod__c": "PerUnit",
                "ProcessingOrder__c": "1",
                "QuoteLineConsumptionSchedule__c": "",
                "LowerBound__c": "1",
                "UpperBound__c": ""
            }
            records.append(record)

        return pd.DataFrame(records)

    def create_source_consumption_schedule_dataframe(self, enriched_llm_response):
        subscriptions = enriched_llm_response.get("subscriptions", [])
        records = []

        for sub in subscriptions:
            list_items = sub.get("listitemsource", [])

            for item in list_items:
                if "base configuration" in (item.get("lisName", "")).lower():
                    continue
                record = {
                    "scsExternalId": item.get("scsExternalId", "NA"),
                    "scsName": item.get("scsName", "NA"),  
                    "subExternalId": sub.get("subExternalId", "NA"),
                    "subscriptionName": sub.get("subscriptionName", "NA"),
                    "lisExternalId": item.get("lisExternalId", "NA"),
                    "lisName": item.get("lisName", "NA"),
                    "CurrencyIsoCode": item.get("CurrencyIsoCode", "NA"),
                    "LineItemSource__c": "",
                    "RatingMethod__c": "Tier",
                    "Type__c": "Range"
                }
                records.append(record)

        return pd.DataFrame(records)

    def create_source_consumption_rate_dataframe(self, llm_response):
        subscriptions = llm_response.get("subscriptions", [])
        records = []

        for sub in subscriptions:
            list_items = sub.get("listitemsource", [])

            for item in list_items:
                if "base configuration" in (item.get("lisName", "")).lower():
                    continue
                record = {
                    "scrExternalId": item.get("scrExternalId", "NA"),
                    "scrName": item.get("scrName", "NA"),
                    "lisExternalId": item.get("lisExternalId", "NA"),
                    "lisName": item.get("lisName", "NA"),
                    "subExternalId": sub.get("subExternalId", "NA"),
                    "subscriptionName": sub.get("subscriptionName", "NA"),
                    "CurrencyIsoCode": item.get("CurrencyIsoCode", "NA"),
                    "OriginalPrice__c": "",
                    "Price__c": item.get("Price__c", "NA"),
                    "PricingMethod__c": "PerUnit",
                    "ProcessingOrder__c": "1",
                    "QuoteLineConsumptionSchedule__c": "",
                    "LowerBound__c": "1",
                    "UpperBound__c": ""
                }
                records.append(record)

        return pd.DataFrame(records)
    
    # === TOP-LEVEL EXTRACTION ===
    async def extract_fields_from_text(self, full_text):
        contract_fields = await self.extract_contract_fields(full_text)
        try:
            boundaries = await self.call_llm_for_chunk_boundaries(full_text)
            print("\n==== LLM Country Boundaries ====")
            print(json.dumps(boundaries, indent=2, ensure_ascii=False))  # <-- ADD THIS
        except Exception as e:
            print(f"Error in chunk boundary extraction: {e}")
            boundaries = []

        if not boundaries:
            print("No country boundaries found. No subscriptions will be extracted.")
            contract_fields["subscriptions"] = []
            return contract_fields

        country_chunks = self.chunk_doc_by_country_boundaries(full_text, boundaries)
        batch_extracted_subs = await self.extract_all_subscriptions_from_chunks(country_chunks, batch_size=3)
        contract_fields["subscriptions"] = batch_extracted_subs

        return contract_fields
    
    #==============VALIDATION=======================
    def get_lineitemsource_count(self, pdf):
        pm_list = []

        with pdfplumber.open(io.BytesIO(pdf)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                pm_list.append(tables)

        # Lowercase all keywords for comparison
        keywords = {'base', 'in base', 'in additional', 'additional'}
        filtered_rows = []

        for plist in pm_list:
            if plist:
                for pst in plist:
                    for pt in pst:
                        # Compose a lowercased version of the row for all checks
                        pt_lower = [item.lower() if item else '' for item in pt]
                        # Any item matches keywords
                        if any(any(kw in itm for kw in keywords) for itm in pt_lower):
                            if len(pt) >= 3:
                                # Exclude if first cell contains "base configuration" or the other special phrases
                                exclusion_list = [
                                    "base configuration",
                                    "identity document verification - verification with face biometrics"
                                ]
                                first_cell = pt[0].lower() if pt and pt[0] else ""
                                if not any(ex in first_cell for ex in exclusion_list):
                                    filtered_rows.append(pt)

        return filtered_rows


    def extract_subscription_rows(self, pdf):
        pm_list = []
        with pdfplumber.open(io.BytesIO(pdf)) as pdf_obj:
            for page in pdf_obj.pages:
                tables = page.extract_tables()
                pm_list.append(tables)
        subscription_rows = []
        for plist in pm_list:  # List of tables per page
            if plist:
                for table in plist:  # Each table on this page
                    for pt in table:  # Each row in this table
                        print("Row being checked:", pt)
                        pt_cleaned = [item.replace('\n', ' ') if item else item for item in pt]
                        if len(pt_cleaned) >= 3:
                            if pt_cleaned[0]:
                                first_col_normalized = pt_cleaned[0].replace(' ', '').lower()
                                if "baseconfiguration" in first_col_normalized:
                                    print("   ---> Added to subscription_rows!")
                                    subscription_rows.append(pt_cleaned)
                            else:
                                print("Skipped row due to empty first column:", pt_cleaned)


        return subscription_rows
    
    async def extract_contract_pipeline_from_md(self, input_pdf, extracted_text, file_name):
        """
        extracted_text: string containing the contract markdown.
        file_path: optional, the filename or path for logging/trace (default empty).
        """
        result = await self.extract_fields_from_text(extracted_text)

        # if input_pdf and input_pdf.lower().endswith(".pdf"):
        #     try:
        lis_rows = self.get_lineitemsource_count(input_pdf)
        sub_rows = self.extract_subscription_rows(input_pdf)
        actual_lis_cnt = len(lis_rows)
        actual_sub_cnt = len(sub_rows)
            # except Exception as e:
            #     print(f"Warning: Could not count LIS/SUB rows for {input_pdf}: {e}")
            #     actual_lis_cnt = ""
            #     actual_sub_cnt = ""
        
        if not result:
            return {
                "TimeStamp": datetime.now().isoformat(),
                "FileName": file_name,
                "Contractid": "",
                "AccountName": "",
                "ActualSubCnt": "",
                "ActualLisCnt": "",
                "ExtractedSubCnt": "",
                "ExtractedLisCnt":"",
                "MatchedSubCnt": "",
                "MatchedLisCnt": "",
                "%Accuracy": "",
                "output_records": []
            }

        enriched_result = self.enrich_llm_response(result)

        Contract = self.create_contract_dataframe(enriched_result)
        Subscription = self.create_subscription_dataframe(enriched_result)
        LineItemSource = self.create_line_item_source_dataframe(enriched_result)
        subConsumptionSchedule = self.create_subscription_consumption_schedule_dataframe(enriched_result)
        subConsumptionRate = self.create_subscription_consumption_rate_dataframe(enriched_result)
        lisConsmptionSchedule = self.create_source_consumption_schedule_dataframe(enriched_result)
        lisConsumptionRate = self.create_source_consumption_rate_dataframe(enriched_result)

        output_json = {
            "TimeStamp": datetime.now().isoformat(),
            "FileName": file_name,
            "Contractid": enriched_result.get("contractExternalId", ""),
            "AccountName": enriched_result.get("AccountName", ""),
            "ActualSubCnt": actual_sub_cnt,
            "ExtractedSubCnt": "",    
            "MatchedSubCnt": "",       
            "% Sub Extraction Confidence Score": "",            
            "% Sub Matching Rate": "",              
            "ActualLisCnt": actual_lis_cnt,
            "ExtractedLisCnt":"",
            "MatchedLisCnt": "",
            "% LIS Extraction Confidence Score": "",        
            "% LIS Matching Rate": "",
            "output_records": [
                {"name": "Contract", "data": Contract.to_dict(orient="records")},
                {"name": "Subscription", "data": Subscription.to_dict(orient="records")},
                {"name": "LineItemSource", "data": LineItemSource.to_dict(orient="records")},
                {"name": "subConsumptionSchedule", "data": subConsumptionSchedule.to_dict(orient="records")},
                {"name": "subConsumptionRate", "data": subConsumptionRate.to_dict(orient="records")},
                {"name": "lisConsmptionSchedule", "data": lisConsmptionSchedule.to_dict(orient="records")},
                {"name": "lisConsumptionRate", "data": lisConsumptionRate.to_dict(orient="records")}
            ]
        }

        return output_json
