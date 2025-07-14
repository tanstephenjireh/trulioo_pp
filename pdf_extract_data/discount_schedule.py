import json
import asyncio
from config import get_ssm_param
from openai import AsyncOpenAI

class DiscountSchedule:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.DOCV_INSTRUCTION = get_ssm_param("/myapp/docv_prompt")
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

        self.DISCOUNT_SECTIONS_INSTRUCTION = """
        You are given a chunk of a document that may contain Person Match Tier Pricing and/or Business Verification Discount sections.
        Each section contains a table with slightly different column structures:

        ### PERSON MATCH TIER PRICING SECTION:
        - Columns: Monthly Spend Range: bottom, Monthly Spend Range: upper, Tier 1, Tier 2, ... (the first column after the Monthly Spend Range columns is Tier 1, the next is Tier 2, and so on)
        - Each row represents a spend range and the corresponding tiered discount for each tier.

        ### BUSINESS VERIFICATION DISCOUNT SECTION:
        - Columns: Range: bottom, Range: upper, Tier 1, Tier 2, ... (the first column after the Range columns is Tier 1, the next is Tier 2, and so on)
        - Each row represents a range and the corresponding tiered discount for each tier.

        ### EXTRACTION INSTRUCTIONS:

        For each tier in each row of BOTH sections, extract the following fields:
        - "InvoiceScheduleTierName": The tier name (e.g., "Tier 1", "Tier 2", etc.)
        - "LowerBound": Value for the bottom range (Monthly Spend Range: bottom for Person Match, Range: bottom for Business Verification) - Only get the numerical value without the currency.
        - "UpperBound": Value for the upper range (Monthly Spend Range: upper for Person Match, Range: upper for Business Verification) - Only get the numerical value without the currency. Unlimited if no upper limit.
        - "Discount": Value for the tiered discount in that column - Only get the numerical value without the "%".

        ### OUTPUT FORMAT:
        Return the extracted data as a structured JSON object, formatted as follows:
        ```json
        {
        "PersonMatchDiscountSchedule": [
            { "InvoiceScheduleTierName": "Tier 1", "LowerBound": "...", "UpperBound": "...", "Discount": "..." },
            { "InvoiceScheduleTierName": "Tier 2", "LowerBound": "...", "UpperBound": "...", "Discount": "..." }
            // ... more tiers and ranges as found for Person Match ...
        ],
        "BusinessVerificationDiscountSchedule": [
            { "InvoiceScheduleTierName": "Tier 1", "LowerBound": "...", "UpperBound": "...", "Discount": "..." },
            { "InvoiceScheduleTierName": "Tier 2", "LowerBound": "...", "UpperBound": "...", "Discount": "..." }
            // ... more tiers and ranges as found for Business Verification ...
        ]
        }
        ```

        ### IMPORTANT NOTES:
        - If a section is not present in the chunk, return an empty array for that section.
        - If a value is not present or no chunk was provided, return "NA" for that field. Do not leave any field blank.
        - Process both sections independently if both are present.
        - The tier logic (Tier 1, Tier 2, etc.) is the same for both sections.
        """

    async def call_llm_for_discount_sections_boundaries(self, full_text):
        """
        Uses GPT-4o to find the start and end lines (as exact line text) of both ## Person Match Tier Pricing and ## Business Verification Discount sections.
        Returns a dict with boundaries  {"start_line": "...", "end_line": "..."} for both sections if found or None if not found.
        """
        system_prompt = (
            "You are an expert contract parser. "
            "Your job is to find the exact line text that marks the START and END of discount sections in the contract below."
        )
        user_prompt = f"""
    # Instructions:
    ### START OF PERSON MATCH TIER PRICING ###
    - Find the line that marks the START of the Person Match Tier Pricing block. This is the line containing a header for Person Match tier pricing (e.g., '## Person Match Tier Pricing', 'Personmatch Tier Pricing', etc.).
        - Only match sections that are for Person Match Tier Pricing (NOT FOR Business Verification Discount or other products).
        - The section should contain a table with monthly Spend Range and tiered discount for Person Match.
    ### END OF PERSON MATCH TIER PRICING ###
    - Find the line that marks the END of the Person Match Tier Pricing block or when the section is no longer about Person Match tiered pricing/discounts.
        - This is the first line AFTER the Person Match Tier Pricing section that is clearly a new section (such as another product, e.g., '## Selected Services and Pricing: Identity Document Verification', 'Selected Services and Pricing: Workflow Studio', Selected Services and Pricing: Business Verification etc.).
    - If this section is NOT found in the document, use "" for both start and end lines.

    ### 2. BUSINESS VERIFICATION DISCOUNT ###
    - Find the line that marks the START of the Business Verification Discount block. This is the line containing a header for Business Verification discount (e.g., '## Business Verification Discount', 'Business Verification Discount', etc.).
        - The section should contain a table with Range: bottom, Range: upper, and Tiered Discount.
    - Find the line that marks the END of the Business Verification Discount block or when the section is no longer about Business Verification discounts.
        - This is the first line AFTER the last line of the Business Verification Discount sectio (such as ## General Terms and Condtions etc.)
    - If this section is NOT found in the document, use "" for both start and end lines.

    ### Output
    - Output a JSON object with four fields:
    - "person_match_start_line": The exact text of the Person Match Tier Pricing start line (or "" if not found)
    - "person_match_end_line": The exact text of the Person Match Tier Pricing end line (or "" if not found)
    - "business_verification_start_line": The exact text of the Business Verification Discount start line (or "" if not found)
    - "business_verification_end_line": The exact text of the Business Verification Discount end line (or "" if not found)

    ### Note
    - It is perfectly normal for a document to have 0, 1, or 2 of these sections.
    - If a section is not present, return "" for both its start and end lines.
    - Do not try to force-find sections that don't exist.

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
        data = json.loads(content) if content is not None else {
            "person_match_start_line": "", "person_match_end_line": "",
            "business_verification_start_line": "", "business_verification_end_line": ""
        }
        return data

    async def extract_full_disc_chunk_by_llm(self, full_text):
        """
        Uses GPT-4o-mini boundary finder to return both Person Match Tier Pricing and Business Verification Discount chunk texts.
        Returns a dict with both chunks, or empty strings if not found.
        """
        boundaries = await self.call_llm_for_discount_sections_boundaries(full_text)
        
        # Get Person Match boundaries
        pm_start_line = boundaries.get("person_match_start_line", "").strip()
        pm_end_line = boundaries.get("person_match_end_line", "").strip()
        
        # Get Business Verification boundaries
        bv_start_line = boundaries.get("business_verification_start_line", "").strip()
        bv_end_line = boundaries.get("business_verification_end_line", "").strip()

        # Split document into lines for precise search
        lines = full_text.splitlines()
        
        # Extract Person Match chunk
        pm_chunk = ""
        if pm_start_line:
            try:
                pm_start_idx = next(i for i, line in enumerate(lines) if line.strip() == pm_start_line)
                if pm_end_line:
                    try:
                        pm_end_idx = next(i for i, line in enumerate(lines) if line.strip() == pm_end_line)
                    except StopIteration:
                        print(f"Person Match end line not found: {pm_end_line}. Using end of document.")
                        pm_end_idx = len(lines)
                else:
                    pm_end_idx = len(lines)
                pm_chunk = "\n".join(lines[pm_start_idx:pm_end_idx]).strip()
            except StopIteration:
                print(f"Person Match start line not found: {pm_start_line}")
        
        # Extract Business Verification chunk
        bv_chunk = ""
        if bv_start_line:
            try:
                bv_start_idx = next(i for i, line in enumerate(lines) if line.strip() == bv_start_line)
                if bv_end_line:
                    try:
                        bv_end_idx = next(i for i, line in enumerate(lines) if line.strip() == bv_end_line)
                    except StopIteration:
                        print(f"Business Verification end line not found: {bv_end_line}. Using end of document.")
                        bv_end_idx = len(lines)
                else:
                    bv_end_idx = len(lines)
                bv_chunk = "\n".join(lines[bv_start_idx:bv_end_idx]).strip()
            except StopIteration:
                print(f"Business Verification start line not found: {bv_start_line}")

        return {
            "person_match_chunk": pm_chunk,
            "business_verification_chunk": bv_chunk
        }
    
    async def extract_discount_sections_from_llm(self, docv_chunk):
        outputs = []
        if docv_chunk.strip():
            try:
                # Add delay before API call
                await asyncio.sleep(1)

                response = await self.openai.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.DISCOUNT_SECTIONS_INSTRUCTION},
                        {"role": "user", "content": docv_chunk}
                    ]
                )
                content = response.choices[0].message.content
                outputs.append(json.loads(content.strip()) if content is not None else {
                    "PersonMatchDiscountSchedule": [],
                    "BusinessVerificationDiscountSchedule": []
                })
            except Exception as e:
                print(f"Error processing discount sections chunk: {e}")
        else:
            print("No discount sections chunk found.")
        return outputs
    

    def merge_discount_schedule_into_json(self, input_json, discountSchedule):
        """
        Adds the discount_schedule list as two new records inside the 'output_records' list in input_json:
        1. discountSchedule - schedule-level information
        2. discountRate - individual tier records
        """
        contract_id = input_json.get('Contractid', 'NA')
        
        # Process Person Match Discount Schedule
        person_match_discounts = discountSchedule.get('PersonMatchDiscountSchedule', [])
        business_verification_discounts = discountSchedule.get('BusinessVerificationDiscountSchedule', [])
        
        # Create discountSchedule records
        discount_schedule_records = []
        discount_rate_records = []
        
        # Process Person Match
        if person_match_discounts:
            pm_disc_ext_id = f"pm_disc_{contract_id}"
            discount_schedule_records.append({
                "DiscExtId": pm_disc_ext_id,
                "ContractId": contract_id,
                "DiscountUnit": "Percent",
                "Type": "Step",
                "Description": "Default person match discount schedule"
            })
            
            for i, tier in enumerate(person_match_discounts, 1):
                discount_rate_records.append({
                    "DiscExtId": pm_disc_ext_id,
                    "DiscRateExtId": f"pm_disc_{i}_{contract_id}",
                    "InvoiceScheduleTierName": tier.get("InvoiceScheduleTierName", "NA"),
                    "LowerBound": tier.get("LowerBound", "NA"),
                    "UpperBound": tier.get("UpperBound", "NA"),
                    "Discount (%)": tier.get("Discount", "NA")
                })
        
        # Process Business Verification
        if business_verification_discounts:
            bv_disc_ext_id = f"bv_disc_{contract_id}"
            discount_schedule_records.append({
                "DiscExtId": bv_disc_ext_id,
                "ContractId": contract_id,
                "DiscountUnit": "Percent",
                "Type": "Step",
                "Description": "Default business verification discount schedule"
            })
            
            # Add individual tier records for Business Verification
            for i, tier in enumerate(business_verification_discounts, 1):
                discount_rate_records.append({
                    "DiscExtId": bv_disc_ext_id,
                    "DiscRateExtId": f"bv_disc_{i}_{contract_id}",
                    "InvoiceScheduleTierName": tier.get("InvoiceScheduleTierName", "NA"),
                    "LowerBound": tier.get("LowerBound", "NA"),
                    "UpperBound": tier.get("UpperBound", "NA"),
                    "Discount (%)": tier.get("Discount", "NA")
                })

        # Add DiscExtId to all Subscription records based on their subExternalId prefix
        # Only add if there are actual discount schedule records
        if discount_schedule_records:
            pm_disc_ext_id = None
            bv_disc_ext_id = None
            
            # Find the actual DiscExtId values from the discount schedule records
            for record in discount_schedule_records:
                if record.get("Description", "").startswith("Default person match"):
                    pm_disc_ext_id = record.get("DiscExtId")
                elif record.get("Description", "").startswith("Default business verification"):
                    bv_disc_ext_id = record.get("DiscExtId")
            
            # Only add DiscExtId to subscriptions if we have the corresponding discount schedules
            for record in input_json['output_records']:
                if record.get('name') == 'Subscription' and isinstance(record.get('data'), list):
                    for item in record['data']:
                        if isinstance(item, dict):
                            sub_external_id = str(item.get('subExternalId', ''))
                            if sub_external_id.startswith('sub') and pm_disc_ext_id:
                                item['DiscExtId'] = pm_disc_ext_id
                            elif sub_external_id.startswith('bv') and bv_disc_ext_id:
                                item['DiscExtId'] = bv_disc_ext_id

        # Remove any existing discountSchedule and discountRate records to avoid duplicates
        input_json['output_records'] = [rec for rec in input_json['output_records'] 
                                    if rec.get('name') not in ['discountSchedule', 'discountRate']]
        
        # Add the new records
        if discount_schedule_records:
            input_json['output_records'].append({
                'name': 'discountSchedule',
                'data': discount_schedule_records
            })
        
        if discount_rate_records:
            input_json['output_records'].append({
                'name': 'discountRate',
                'data': discount_rate_records
            })
        
        return input_json
    

    async def main(self, full_text, contract_json):
        """
        Main function to extract Person Match Discount Schedule chunk and boundaries, and merge the result into the contract JSON.
        Args:
            input_md (str): Path to the markdown file containing the contract text.
            input_contract_json (str): Path to the contract JSON file to merge results into.
        """
        pmdiscount_boundaries = await self.call_llm_for_discount_sections_boundaries(full_text)
        print("\n==== PERSON MATCH DISCOUNT SCHEDULE BOUNDARIES LLM OUTPUT ====")
        print(json.dumps(pmdiscount_boundaries, indent=2, ensure_ascii=False))

        pmdiscount_chunks = await self.extract_full_disc_chunk_by_llm(full_text)
        print("\n==== PERSON MATCH DISCOUNT SCHEDULE CHUNKS EXTRACTED ====")
        print(pmdiscount_chunks)

        # Only proceed if a chunk was found
        if pmdiscount_chunks:
            # Process Person Match chunk
            person_match_chunk = pmdiscount_chunks.get("person_match_chunk", "")
            business_verification_chunk = pmdiscount_chunks.get("business_verification_chunk", "")
            
            # Initialize combined discount schedule
            combined_discount_schedule = {
                "PersonMatchDiscountSchedule": [],
                "BusinessVerificationDiscountSchedule": []
            }
            
            # Extract Person Match discount schedule
            if person_match_chunk.strip():
                extracted_pm = await self.extract_discount_sections_from_llm(person_match_chunk)
                if extracted_pm:
                    combined_discount_schedule["PersonMatchDiscountSchedule"] = extracted_pm[0].get('PersonMatchDiscountSchedule', [])
                    combined_discount_schedule["BusinessVerificationDiscountSchedule"] = extracted_pm[0].get('BusinessVerificationDiscountSchedule', [])
            
            # Extract Business Verification discount schedule
            if business_verification_chunk.strip():
                extracted_bv = await self.extract_discount_sections_from_llm(business_verification_chunk)
                if extracted_bv:
                    # Add to existing Business Verification schedule if any
                    existing_bv = combined_discount_schedule["BusinessVerificationDiscountSchedule"]
                    new_bv = extracted_bv[0].get('BusinessVerificationDiscountSchedule', [])
                    combined_discount_schedule["BusinessVerificationDiscountSchedule"] = existing_bv + new_bv
            
            # Merge discount schedule into contract JSON
            merged_json = self.merge_discount_schedule_into_json(contract_json, combined_discount_schedule)
            print("\n==== MERGED CONTRACT JSON WITH DISCOUNT SCHEDULE ====")
            print(json.dumps(merged_json, indent=2, ensure_ascii=False))
            return merged_json
        else:
            print('No Person Match Tier Pricing or Business Verification section found.')
            return contract_json