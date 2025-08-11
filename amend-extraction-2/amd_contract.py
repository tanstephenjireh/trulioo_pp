import io
import json
import pandas as pd
from openai import AsyncOpenAI
import time
from datetime import datetime
import pdfplumber
import asyncio
from config import get_ssm_param

# ========== ENV SETUP ==========

class ContractExtractor:
    def __init__(self):
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)
        
        # Contract extraction instructions
        self.CONTRACT_INSTRUCTIONS = """
You are an intelligent field extractor. You are given a document that may contain one or more service subscriptions grouped under a single contract. Your task is to extract all relevant fields and return a **single structured JSON object** in a **nested format**, organized as follows:

- The top level represents the **contract**.

If a value is not present, return `"NA"`. Do not leave any field blank.

---

## CONTRACT-LEVEL FIELDS (top-level keys in the JSON)

Extract the following fields based on their respective sections in the document:

### CUSTOMER INFORMATION
Extract from the **Customer Information** section. If the field is not explicitly listed, return "NA":

- `AccountName`: The name of the Customer, always before the (\"Customer\") string and the first line under Customer Information.\n

### GENERAL SERVICE FEES
Extract from the **General Service Fees** section:

- `ImplementationFee`: extract the numerical price under the item name "Implementation Fee". 0 if waived. NA if not found.
- `LicenseFee`: extract the numerical price under the item name "License Fee". 0 if waived. NA if not found.    

### FEES AND PAYMENT TERMS or ## Payment Terms
Extract from the **Fees and Payment Terms** or **Payment Terms** section if available:

- `ContractTerm`: extract the number of months from the line beginning with "Initial Term". NA if none.
- `PaymentMethod__c`: extract from the field labeled "Payment Method". NA if not found.
- `PrepaidCredits__c`: extract the numeric value of "Prepaid Usage Credit"; if none, return "NA"
- `Minimum_Monthly__c`: 
    - Under **Payment Terms** section, if available, understand the context of what is the updated minimum monthly commitment.
    - Example: If it is indicated "The Parties hereby agree to amend the Monthly Minimum Commitment amount under the Original Order
Form to $1,000" then return "1000" without any currency.
    - If no amount change is indicated, return "NA".


### FEES AND PAYMENT TERMS or ### GENERAL TERMS AND CONDITIONS OR ### TERMS AND CONDITIONS
Extract from the **Fees and Payment Terms** section or signature date under **General Terms and Conditions** section
- `StartDate`: (formatted as YYYY-MM-DD)
- Also exact date under "Effective Date" under "# Fees and Payment Terms" or "# Payment Terms" if there is an exact date. If none,
    - Check "# General Terms and Conditions" or # Terms and Conditions section, and extract the **latest date** found in the signature section of either Trulioo or the Customer Authorized Representative.
    - Most likely, there is a present date, in either of the two.

**NOTE** Ensure that you have read the contract and was able to apply whether there are amendments on any of the fields we are extracting. Always extract the new amended values.

"""
        
        # Subscription extraction instructions
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

- `subscriptionName`: the country where this subscription applies. 
    - This should be a valid country name.
    - If there are extra words or phrases after the country name, remove, and only return the country name. (e.g. "Japan" NOT "Japan Configuration")
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
  - "Add-On" if Type is "Additional"
  - "In Additional" if Type is "In Additional"
- `Description__c`: from the "Comments" column
- `Included__c`:  
  - "TRUE" if the "Fee per Query" is marked "Included"  
  - "FALSE" otherwise
- `scsName`: set as `"<lisName> Consumption Schedule"`
- `scrName`: set as `"<lisName> Consumption Rate"`
- `Price__c`: the numeric value from "Fee per Query", or 0 if "Included"
"""

    async def extract_contract_fields(self, text):
        """Extract contract-level fields using LLM."""
        # Add delay before API call
        await asyncio.sleep(5)

        response = await self.openai.chat.completions.create(
            model="gpt-4.1",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": self.CONTRACT_INSTRUCTIONS},
                {"role": "user", "content": text}
            ]
        )
        print("\n===== Raw LLM Extract Contract Output =====")
        print(response.choices[0].message.content.strip())
        return json.loads(response.choices[0].message.content.strip())

    async def call_llm_for_chunk_boundaries(self, full_doc_text):
        """Find country boundaries in the document."""
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
        await asyncio.sleep(5)

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
        """Split document into country-specific chunks."""
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

        print(f"\n==== All Extracted Country Chunks ====")
        for chunk in results:
            print(f"\n[Country: {chunk['country']}]\n{'-'*40}\n{chunk['text']}\n{'='*40}")

        return results

    async def call_llm_for_extraction(self, batch_chunks):
        """Extract subscription data from country chunks."""
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
- If there is a markdown table in the text that is NOT directly under a country header, it MUST be included as part of the most recent preceding country's subscription block.
- Even if a table has a header or text above it, and is not a country, treat it as a continuation of the previous table, and under the most recent country.
- Never leave any table or list item unassigned. Every table belongs to the country that came before it.

---

# Field extraction guidelines
## `subscriptions` (list of subscription blocks grouped by country)
**For each subscription**, extract the following:

- `subscriptionName`: the country where this subscription applies
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
        await asyncio.sleep(5)

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
        return data

    async def extract_all_subscriptions_from_chunks(self, country_chunks, batch_size=3):
        """Extract all subscriptions from country chunks in batches."""
        all_subs = []
        for batch in self.batch_chunks(country_chunks, batch_size):
            countries = [c['country'] for c in batch]
            print(f"\n--- Extracting batch: {countries} ---")

            for chunk in batch:
                print(f"\n[Chunk: {chunk['country']}]\n{'-'*40}\n{chunk['text']}\n{'='*40}")

            res = await self.call_llm_for_extraction(batch)

            print(f"\n[LLM Response for batch: {countries}]\n{'-'*50}\n{json.dumps(res, indent=2, ensure_ascii=False)}\n{'='*50}")

            all_subs.extend(res["subscriptions"])
        return all_subs

    def batch_chunks(self, chunks, batch_size=3):
        """Yield batches of up to batch_size."""
        for i in range(0, len(chunks), batch_size):
            yield chunks[i:i+batch_size]

    def enrich_llm_response(self, llm_response, contract_external_id):
        """Add unique IDs to the extracted data."""
        llm_response["contractExternalId"] = contract_external_id

        subscriptions = llm_response.get("subscriptions", [])
        valid_subscriptions = []  # New list to store only valid subscriptions
        
        for i, subscription in enumerate(subscriptions, start=1):
            # Filter subscriptionName once here
            subscription_name = subscription.get("subscriptionName", "NA")
            if subscription_name != "NA":
                import re
                unwanted_words = r'\b(?:General|Country:|Configuration|Fee|Pricing|Information|country|base configuration|table|details|services|structure)\b'
                subscription_name = re.sub(unwanted_words, '', subscription_name, flags=re.IGNORECASE).strip()
                # Clean up extra spaces
                subscription_name = re.sub(r'\s+', ' ', subscription_name).strip()
                if not subscription_name:
                    subscription_name = "NA"
            
            # Update the subscription with filtered name
            subscription["subscriptionName"] = subscription_name
            
            # Skip subscriptions with blank/NA subscriptionName
            if subscription_name == "NA" or not subscription_name.strip():
                print(f"Skipping subscription {i}: blank subscriptionName")
                continue
            
            # Only assign external IDs to valid subscriptions
            sub_external_id = f"amd_sub{i}_{contract_external_id}"
            subcs_external_id = f"amd_subcs{i}_{contract_external_id}"
            subcr_external_id = f"amd_subcr{i}_{contract_external_id}"

            subscription["subExternalId"] = sub_external_id
            subscription["subCsExternalId"] = subcs_external_id
            subscription["subCrExternalId"] = subcr_external_id

            # Filter out invalid list items
            valid_list_items = []
            non_base_count = 1
            for item in subscription.get("listitemsource", []):
                lis_name = item.get("lisName", "NA")
                
                # Skip base configuration items
                if "base configuration" in lis_name.strip().lower():
                    continue
                    
                # Skip items with blank/NA lisName
                if lis_name == "NA" or not lis_name.strip():
                    print(f"Skipping list item in subscription {i}: blank lisName")
                    continue
                
                # Only assign external IDs to valid list items
                item["lisExternalId"] = f"amd_lis{non_base_count}_{sub_external_id}"
                item["scsExternalId"] = f"amd_scs{non_base_count}_{sub_external_id}"
                item["scrExternalId"] = f"amd_scr{non_base_count}_{sub_external_id}"
                non_base_count += 1
                valid_list_items.append(item)
            
            # Update subscription with only valid list items
            subscription["listitemsource"] = valid_list_items
            valid_subscriptions.append(subscription)

        # Update llm_response with only valid subscriptions
        llm_response["subscriptions"] = valid_subscriptions

        return llm_response

    def create_contract_dataframe(self, llm_response):
        """Create contract dataframe."""
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
            "CurrencyIsoCode": "",
            "Description": "",
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
        """Create subscription dataframe."""
        subscriptions = llm_response.get("subscriptions", [])
        records = []
        for entry in subscriptions:
            record = {
                "subExternalId": entry.get("subExternalId", "NA"),
                "ProductName": entry.get("subscriptionName", "NA"),
                "ContractExternalId": llm_response.get("contractExternalId", "NA"),
                "ContractName": llm_response.get("AccountName", "NA"),
                "CurrencyIsoCode": "USD",
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
        """Create line item source dataframe."""
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
                    "CurrencyIsoCode": "USD",
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
        """Create subscription consumption schedule dataframe."""
        subscriptions = llm_response.get("subscriptions", [])
        records = []
        for sub in subscriptions:
            record = {
                "subCsExternalId": sub.get("subCsExternalId", "NA"),
                "subCsName": sub.get("subCsName", "NA"),
                "subExternalId": sub.get("subExternalId", "NA"),
                "subscriptionName": sub.get("subscriptionName", "NA"),
                "CurrencyIsoCode": "USD",
                "LineItemSource__c": "",
                "RatingMethod__c": "Tier",
                "Type__c": "Range"
            }
            records.append(record)
        return pd.DataFrame(records)

    def create_subscription_consumption_rate_dataframe(self, llm_response):
        """Create subscription consumption rate dataframe."""
        subscriptions = llm_response.get("subscriptions", [])
        records = []
        for sub in subscriptions:
            record = {
                "subCrExternalId": sub.get("subCrExternalId", "NA"),
                "subCrName": sub.get("subCrName", "NA"),
                "subExternalId": sub.get("subExternalId", "NA"),
                "subscriptionName": sub.get("subscriptionName", "NA"),
                "CurrencyIsoCode": "USD",
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
        """Create source consumption schedule dataframe."""
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
                    "CurrencyIsoCode": "USD",
                    "LineItemSource__c": "",
                    "RatingMethod__c": "Tier",
                    "Type__c": "Range"
                }
                records.append(record)
        return pd.DataFrame(records)

    def create_source_consumption_rate_dataframe(self, llm_response):
        """Create source consumption rate dataframe."""
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
                    "CurrencyIsoCode": "USD",
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

    def get_lineitemsource_count(self, pdf):
        """Get line item source count from PDF."""
        pm_list = []
        with pdfplumber.open(io.BytesIO(pdf)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                pm_list.append(tables)

        keywords = {'base', 'in base', 'additional'}
        filtered_rows = []

        for plist in pm_list:
            if plist:
                for pst in plist:
                    for pt in pst:
                        pt_lower = [item.lower() if item else '' for item in pt]
                        if any(any(kw in itm for kw in keywords) for itm in pt_lower):
                            if len(pt) >= 3:
                                exclusion_list = [
                                    "base configuration",
                                    "identity document verification - verification with face biometrics"
                                ]
                                first_cell = pt[0].lower() if pt and pt[0] else ""
                                if not any(ex in first_cell for ex in exclusion_list):
                                    filtered_rows.append(pt)
        return filtered_rows

    def extract_subscription_rows(self, pdf):
        """Extract subscription rows from PDF."""
        pm_list = []
        with pdfplumber.open(io.BytesIO(pdf)) as pdf_obj:
            for page in pdf_obj.pages:
                tables = page.extract_tables()
                pm_list.append(tables)
        subscription_rows = []
        for plist in pm_list:
            if plist:
                for table in plist:
                    for pt in table:
                        print("Row being checked:", pt)
                        pt_cleaned = [item.replace('\n', ' ') if item else item for item in pt]
                        if len(pt_cleaned) >= 3:
                            # Check if this row contains all three required columns anywhere in the row
                            has_name = any('name' in str(cell).lower() for cell in pt_cleaned if cell)
                            has_type = any('type' in str(cell).lower() for cell in pt_cleaned if cell)
                            has_fee_per_query = any('fee per' in str(cell).lower() for cell in pt_cleaned if cell)
                            
                            if has_name and has_type and has_fee_per_query:
                                print("   ---> Found header row with ['Name', 'Type', 'Fee per Query'] columns")
                                print(f"   ---> Row content: {pt_cleaned}")
                                # Count this as a subscription section
                                subscription_rows.append(pt_cleaned)
                            else:
                                print(f"   ---> Skipped row - missing required columns:")
                                print(f"      has_name: {has_name}, has_type: {has_type}, has_fee_per_query: {has_fee_per_query}")
                        else:
                            print("Skipped row due to insufficient columns:", pt_cleaned)
        return subscription_rows

    async def extract_fields_from_text(self, full_text):
        """Extract all fields from text."""
        contract_fields = await self.extract_contract_fields(full_text)
        try:
            boundaries = await self.call_llm_for_chunk_boundaries(full_text)
            print("\n==== LLM Country Boundaries ====")
            print(json.dumps(boundaries, indent=2, ensure_ascii=False))
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

    async def extract_contract_pipeline_from_md(self, extracted_text, file_path, fileName, contract_external_id):
        """Main pipeline to extract contract data from markdown."""
        start_time = time.time()
        result = await self.extract_fields_from_text(extracted_text)
        extracting_end = time.time()

        lis_rows = self.get_lineitemsource_count(file_path)
        sub_rows = self.extract_subscription_rows(file_path)
        actual_lis_cnt = len(lis_rows)
        actual_sub_cnt = len(sub_rows)

        if not result:
            return {
                "TimeStamp": datetime.now().isoformat(),
                "FileName": fileName,
                "Contractid": contract_external_id,
                "AccountName": "",
                "TotalRunTime": round(extracting_end - start_time, 2),
                "ActualSubCnt": "",
                "ActualLisCnt": "",
                "ExtractedSubCnt": "",
                "ExtractedLisCnt":"",
                "MatchedSubCnt": "",
                "MatchedLisCnt": "",
                "%Accuracy": "",
                "output_records": []
            }

        enriched_result = self.enrich_llm_response(result, contract_external_id)

        Contract = self.create_contract_dataframe(enriched_result)
        Subscription = self.create_subscription_dataframe(enriched_result)
        LineItemSource = self.create_line_item_source_dataframe(enriched_result)
        subConsumptionSchedule = self.create_subscription_consumption_schedule_dataframe(enriched_result)
        subConsumptionRate = self.create_subscription_consumption_rate_dataframe(enriched_result)
        lisConsumptionSchedule = self.create_source_consumption_schedule_dataframe(enriched_result)
        lisConsumptionRate = self.create_source_consumption_rate_dataframe(enriched_result)

        output_json = {
            "TimeStamp": datetime.now().isoformat(),
             "FileName": fileName,
            "Contractid": enriched_result.get("contractExternalId", ""),
            "AccountName": enriched_result.get("AccountName", ""),
            "TotalRunTime": round(extracting_end - start_time, 2),
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
                {"name": "lisConsumptionSchedule", "data": lisConsumptionSchedule.to_dict(orient="records")},
                {"name": "lisConsumptionRate", "data": lisConsumptionRate.to_dict(orient="records")}
            ]
        }

        return output_json


# if __name__ == "__main__":
#     # Hardcoded inputs
#     contract_external_id = "dummy_ext_id"
#     input_pdf = "BACKEND/THIRDV/amendments/Plum_New_Markets_add_ons.docx.pdf"
#     input_md = "BACKEND/THIRDV/output_json/parsed_Plum_New_Markets_add_ons.docx.md"
#     output_folder = "BACKEND/THIRDV/output_single_json"

#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)

#     # Create extractor instance
#     extractor = ContractExtractor()
    
#     # Process single file
#     filename = os.path.basename(input_md)
#     print(f"Processing: {filename}")
#     print(f"Using ContractExternalId: {contract_external_id}")

#     with open(input_md, "r", encoding="utf-8") as f:
#         md_str = f.read()

#     all_json = extractor.extract_contract_pipeline_from_md(md_str, file_path=input_pdf, contract_external_id=contract_external_id)

#     json_out = os.path.join(
#         output_folder,
#         f"json_{os.path.splitext(filename)[0]}.json"
#     )
#     with open(json_out, "w") as f:
#         json.dump(all_json, f, indent=2)
#     print(f"    Saved JSON to {json_out}")

#     for table in all_json["output_records"]:
#         name = table["name"]
#         data = table["data"]
#         if data:
#             df = pd.DataFrame(data)
#             csv_out = os.path.join(
#                 output_folder,
#                 f"{os.path.splitext(filename)[0]}_{name}.csv"
#             )
#             df.to_csv(csv_out, index=False)
#     print(f"    All CSVs for {filename} saved.")
#     print("All processing done.")
