"""
cmpr_sub_add.py

Purpose:
- This script allows you to add a new subscription (and all related records) to your extraction JSON, using live Salesforce data and your mapping files, in the exact format your pipeline expects.
- It also updates a comparison JSON file to add a new subscription block for the added subscription, so both AI-Extracted and Salesforce data are shown for SubscriptionDetails, LineItemSources, DiscountSchedule, and DiscountRate.

Overall Inputs:
- extraction_json: Path to the original extraction JSON 
- subscriptionName: The subscription (product) name to add (e.g., 'United States Transactions')
- FileName: The contract PDF filename to match (e.g., 'AmWager Trulioo_DealRoom-Trulioo-17128-26-Feb-2025-19-52-26-signed.pdf')
- output_path: Path to save the merged extraction JSON (e.g., 'new_subscription_output.json')
- comparison_json: Path to the comparison JSON to update (e.g., 'compr_main_output.json')
- Salesforce credentials via environment variables

Intermediate outputs 
- account_files.json: Maps Account IDs to account names and a dictionary of filenames to contract start dates
- filename_to_account.json: Maps normalized filenames to Account ID, Account Name, Start Date, Contractid (UUID), and SalesforceContractId

Output:
- new_subscription_output.json: The merged extraction JSON, including all original records plus the new subscription and related records (Subscription, LineItemSource, discountSchedule, discountRate).
- Updated comparison JSON: The comparison JSON is updated to include a new subscription block for the added subscription, with both AI-Extracted and Salesforce data set to the Salesforce value for all relevant fields.

Process:
1. Generates account_files.json and filename_to_account.json by querying Salesforce and matching extraction logs (using logic from compile_records.py).
2. Looks up the correct AccountId, Contractid (UUID), and SalesforceContractId for the filename.
3. Builds custom IDs for the new subscription, line item sources, and discount schedule/rate (e.g., cmpr_<ProductName>_<Contractid>).
4. Connects to Salesforce and queries for the subscription record (and its Salesforce ID, discount schedule, and key SBQQ fields), all line item sources, and the discount schedule/tiers.
5. Assembles a dictionary in the exact format of your extraction JSON, with all required fields and defaults.
6. Loads the original extraction JSON, appends the new records to the appropriate lists, and saves the merged result to new_subscription_output.json.
7. Updates the comparison JSON by appending a new subscription block for the added subscription, setting both AI-Extracted and Salesforce data to the Salesforce value for all relevant fields.

Note:
- The main() function now takes a loaded JSON object (not a file path) and returns the merged JSON object (not a file).
- All file reading and writing (input and output) is handled in the if __name__ == '__main__' block.
- The output file will contain all original records plus the new "cmpr" records for the added subscription, if you choose to save it.
- The comparison JSON will be updated in-place to include the new subscription block for the added subscription, if you choose to save it.
"""

import json
import re
import os
from simple_salesforce import Salesforce
from config import get_ssm_param



username = get_ssm_param("/myapp/sf_username")
password = get_ssm_param("/myapp/sf_password")
security_token = get_ssm_param("/myapp/sf_security_token")
domain = get_ssm_param("/myapp/sf_domain")

# --- Helper Functions ---
def normalize_filename(name):
    """Normalize filename by removing extensions and lowercasing."""
    return re.sub(r'(\.[a-zA-Z0-9]+)+$', '', name).strip().lower()

def safe_val(val):
    """Return value or empty string if None."""
    return val if val is not None else ""

def strip_pdf(filename):
    """Remove .pdf extension if present."""
    return filename[:-4] if filename.lower().endswith('.pdf') else filename

# --- Main Classes and Pipeline ---
class RecordCompiler:
    def __init__(self, extraction_json_obj):
        self.extraction_json_obj = extraction_json_obj
        self.acc_dict = {}
        self.filename_to_account = {}
        self._sf_contractid_map = {}  # norm filename -> Salesforce ContractId

    def query_salesforce(self):
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        query = '''SELECT 
          ContentDocument.Title,
          LinkedEntityId,
          TYPEOF LinkedEntity
            WHEN Contract THEN StartDate, AccountId, Account.Name
          END
        FROM ContentDocumentLink
        WHERE LinkedEntityId IN (SELECT Id FROM Contract)'''
        res = sf.query_all(query)
        records = res['records']
        for rec in records:
            linked_entity = rec.get('LinkedEntity')
            if not linked_entity:
                continue
            acc_id = linked_entity.get('AccountId')
            acc_name = linked_entity.get('Account', {}).get('Name') if linked_entity.get('Account') else None
            fname = rec.get('ContentDocument', {}).get('Title')
            date = linked_entity.get('StartDate')
            salesforce_contract_id = rec.get('LinkedEntityId')
            if not acc_id or not fname:
                continue
            if acc_id not in self.acc_dict:
                self.acc_dict[acc_id] = {'AccountName': acc_name, 'Files': {}}
            self.acc_dict[acc_id]['Files'][fname] = date
            norm = normalize_filename(fname)
            self._sf_contractid_map[norm] = salesforce_contract_id

    def compile(self):
        self.query_salesforce()
        data = self.extraction_json_obj
        logs = data.get('Logs', [])
        file_lookup = {}
        for acc_id, acc_info in self.acc_dict.items():
            acc_name = acc_info.get('AccountName')
            for fname, date in acc_info.get('Files', {}).items():
                norm = normalize_filename(fname)
                file_lookup[norm] = {'AccountId': acc_id, 'AccountName': acc_name, 'StartDate': date}
        for log in logs:
            log_fname = log.get('FileName', '')
            norm_log_fname = normalize_filename(log_fname)
            match = file_lookup.get(norm_log_fname)
            if match:
                self.filename_to_account[norm_log_fname] = {
                    'AccountId': match['AccountId'],
                    'AccountName': match['AccountName'],
                    'StartDate': match['StartDate'],
                    'Contractid': log.get('Contractid'),  # UUID
                    'SalesforceContractId': self._sf_contractid_map.get(norm_log_fname)  # 15/18 char SF ID
                }
        return self.acc_dict, self.filename_to_account

def add_subscription(account_files, filename_to_account, subscription_name, filename):
    """
    Add a subscription by querying Salesforce for the contract, subscription, line item sources, and discount info.
    Returns a dictionary matching the extraction_json structure, with custom ID formats.
    """
    print(f"DEBUG add_subscription: Starting with filename='{filename}', subscription_name='{subscription_name}'")
    print(f"DEBUG add_subscription: account_files type={type(account_files)}")
    print(f"DEBUG add_subscription: filename_to_account type={type(filename_to_account)}")
    
    filename = strip_pdf(filename)
    norm_filename = normalize_filename(filename)
    print(f"DEBUG add_subscription: After processing - filename='{filename}', norm_filename='{norm_filename}'")
    
    # Find AccountId for the given filename
    account_id = None
    print(f"DEBUG add_subscription: Searching for account_id...")
    for acc_id, acc_info in account_files.items():
        print(f"DEBUG add_subscription: Checking acc_id='{acc_id}', acc_info type={type(acc_info)}")
        if acc_info is None:
            print(f"DEBUG add_subscription: acc_info is None for acc_id='{acc_id}'")
            continue
        if not isinstance(acc_info, dict):
            print(f"DEBUG add_subscription: acc_info is not dict for acc_id='{acc_id}', type={type(acc_info)}")
            continue
        
        files_dict = acc_info.get('Files', {})
        print(f"DEBUG add_subscription: files_dict type={type(files_dict)} for acc_id='{acc_id}'")
        
        for fname in files_dict:
            if normalize_filename(fname) == norm_filename:
                account_id = acc_id
                print(f"DEBUG add_subscription: Found matching account_id='{account_id}'")
                break
        if account_id:
            break
    
    if not account_id:
        raise ValueError(f"Filename '{filename}' not found in account_files.")


    print(f"DEBUG add_subscription: About to access filename_to_account with key='{norm_filename}'")

    # Add this debugging block:
    print(f"DEBUG: filename_to_account contents:")
    for k, v in filename_to_account.items():
        print(f"  Key: '{k}', Value: {v}, Type: {type(v)}")

    print(f"DEBUG: Looking for exact key: '{norm_filename}'")
    if norm_filename in filename_to_account:
        print(f"DEBUG: Key found in filename_to_account")
        print(f"DEBUG: Value at key: {filename_to_account[norm_filename]}, Type: {type(filename_to_account[norm_filename])}")
    else:
        print(f"DEBUG: Key NOT found in filename_to_account")
        print(f"DEBUG: Available keys: {list(filename_to_account.keys())}")
    

    # To get Contractid (UUID) and SalesforceContractId, use filename_to_account
    filename_account_data = filename_to_account.get(norm_filename)
    if filename_account_data is None:
        print(f"DEBUG: filename_account_data is None for key '{norm_filename}'")
        print(f"DEBUG: Available keys in filename_to_account: {list(filename_to_account.keys())}")
        raise ValueError(f"No account data found for filename '{norm_filename}' in filename_to_account.")

    contractid_uuid = filename_account_data.get('Contractid')  # UUID
    salesforce_contract_id = filename_account_data.get('SalesforceContractId')  # 15/18 char SF ID
    account_name = filename_account_data.get('AccountName')
    start_date = filename_account_data.get('StartDate')
    if not contractid_uuid or not salesforce_contract_id:
        raise ValueError(f"Contractid or SalesforceContractId for filename '{filename}' not found in filename_to_account.")
    # Build custom IDs
    def safe_id(s):
        return s.replace(' ', '_') if s else ''
    sub_external_id = f"cmpr_{safe_id(subscription_name)}_{contractid_uuid}"
    disc_ext_id = f"cmpr_disc_{safe_id(subscription_name)}_{contractid_uuid}"
    # Connect to Salesforce
    sf = Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        domain=domain
    )
    # Query for subscription id and additional fields
    sub_query = f"""
    SELECT Id, DiscountSched__c, SBQQ__SubscriptionStartDate__c,
           SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, SBQQ__ChargeType__c,
           SBQQ__BillingFrequency__c, SBQQ__BillingType__c
    FROM SBQQ__Subscription__c
    WHERE SBQQ__Contract__c = '{salesforce_contract_id}'
    AND SBQQ__ProductName__c = '{subscription_name}'
    """
    sub_res = sf.query_all(sub_query)
    print(f"DEBUG: sub_res type: {type(sub_res)}, value: {sub_res}")
    sub_records = sub_res.get('records', [])
    if not sub_records:
        raise ValueError(f"No subscription found for contract {salesforce_contract_id} and product {subscription_name}.")
    sub_rec = sub_records[0]
    actual_subscription_id = sub_rec['Id']
    discount_sched_id = sub_rec.get('DiscountSched__c')
    subscription_start_date = sub_rec.get('SBQQ__SubscriptionStartDate__c', start_date)
    # Query for line item sources
    lis_query = f"""
    SELECT Id, Name, SBQQ__ProductName__c, SBQQ__Contract__c,
           (SELECT Id, Name, BaseAddon__c, OwnerId, StartDate__c, Product__r.Name FROM LineItemSources__r)
    FROM SBQQ__Subscription__c
    WHERE SBQQ__Contract__c = '{salesforce_contract_id}'
    AND SBQQ__ProductName__c = '{subscription_name}'
    """
    lis_res = sf.query_all(lis_query)
    print(f"DEBUG: lis_res type: {type(lis_res)}, value: {lis_res}")
    lis_records = lis_res.get('records', [])
    line_item_sources = []
    if lis_records:
        line_item_sources_r = lis_records[0].get('LineItemSources__r')
        if line_item_sources_r is not None:
            lis_nested = line_item_sources_r.get('records', [])
        else:
            lis_nested = []
            print(f"DEBUG: LineItemSources__r is None for subscription")
        for lis in lis_nested:
            product_r = lis.get('Product__r')
            lis_name = product_r.get('Name') if isinstance(product_r, dict) else None
            if lis_name is not None:
                lis_external_id = f"cmpr_{safe_id(lis_name)}_{contractid_uuid}"
            else:
                lis_external_id = None
            line_item_sources.append({
                'subExternalId': sub_external_id,
                'lisName': lis_name,
                'lisExternalId': lis_external_id,
                'BaseAddon__c': safe_val(lis.get('BaseAddon__c')),
                'OwnerId': safe_val(lis.get('OwnerId')),
                'StartDate__c': safe_val(lis.get('StartDate__c')),
                'Note': 'Added from Comparison Hub',
                'CurrencyIsoCode': 'USD'
            })
    # Query for discount schedule and tiers
    discount_sched_query = f"""
    SELECT Id, Name, DiscountUnit__c, Type__c,
           (SELECT Id, Name, LowerBound__c, UpperBound__c, DiscountAmount__c, Discount__c FROM InvoiceScheduleTiers__r)
    FROM InvoiceSchedule__c
    WHERE Id IN (
        SELECT DiscountSched__c
        FROM SBQQ__Subscription__c
        WHERE SBQQ__Contract__c = '{salesforce_contract_id}'
        AND SBQQ__ProductName__c = '{subscription_name}'
    )
    """
    discount_res = sf.query_all(discount_sched_query)
    print(f"DEBUG: discount_res type: {type(discount_res)}, value: {discount_res}")
    discount_records = discount_res.get('records', [])
    discount_schedule = []
    discount_rate = []
    if discount_records:
        sched = discount_records[0]
        discount_schedule.append({
            'DiscExtId': disc_ext_id,
            'DiscountUnit': sched.get('DiscountUnit__c'),
            'Type': sched.get('Type__c')
        })
        for tier in sched.get('InvoiceScheduleTiers__r', {}).get('records', []):
            discount_rate.append({
                'DiscExtId': disc_ext_id,
                'InvoiceScheduleTierName': tier.get('Name'),
                'LowerBound': tier.get('LowerBound__c'),
                'UpperBound': tier.get('UpperBound__c'),
                'Discount': tier.get('Discount__c') or tier.get('DiscountAmount__c')
            })
    # Build output dictionary matching extraction_json
    result = {
        'Subscription': [
            {
                'ContractExternalId': contractid_uuid,
                'ProductName': subscription_name,
                'subExternalId': sub_external_id,
                'DiscExtId': disc_ext_id,
                'ContractName': account_name,
                'ProductId': actual_subscription_id,
                'SBQQ__SubscriptionStartDate__c': safe_val(subscription_start_date),
                'SBQQ__SubscriptionPricing__c': safe_val(sub_rec.get('SBQQ__SubscriptionPricing__c')),
                'SBQQ__SubscriptionType__c': safe_val(sub_rec.get('SBQQ__SubscriptionType__c')),
                'SBQQ__ChargeType__c': safe_val(sub_rec.get('SBQQ__ChargeType__c')),
                'SBQQ__BillingFrequency__c': safe_val(sub_rec.get('SBQQ__BillingFrequency__c')),
                'SBQQ__BillingType__c': safe_val(sub_rec.get('SBQQ__BillingType__c')),
                'CurrencyIsoCode': 'USD',
                'Note': 'Added from Comparison Hub',
            }
        ],
        'LineItemSource': line_item_sources,
        'discountSchedule': discount_schedule,
        'discountRate': discount_rate
    }
    print(json.dumps(result, indent=2))
    return result

def build_comparison_subscription_block(subscription_name, line_item_sources, discount_schedule, discount_rate):
    # SubscriptionDetails
    subscription_details = [
        {
            "Field": "SubscriptionName",
            "AI-Extracted": subscription_name,
            "Salesforce data": subscription_name
        }
    ]
    # LineItemSources
    line_item_sources_block = []
    for lis in line_item_sources:
        # Add safety check for None values and key existence
        if lis and isinstance(lis, dict) and lis.get("lisName"):
            line_item_sources_block.append({
                "Field": "LineItemSourceName",
                "AI-Extracted": lis["lisName"],
                "Salesforce data": lis["lisName"]
            })
    # DiscountSchedule
    discount_schedule_block = []
    for sched in discount_schedule:
        # Add safety check for None values and key existence
        if sched and isinstance(sched, dict):
            if "DiscountUnit" in sched and sched["DiscountUnit"] is not None:
                discount_schedule_block.append({
                    "Field": "DiscountUnit",
                    "AI-Extracted": sched["DiscountUnit"],
                    "Salesforce data": sched["DiscountUnit"]
                })
            if "Type" in sched and sched["Type"] is not None:
                discount_schedule_block.append({
                    "Field": "Type",
                    "AI-Extracted": sched["Type"],
                    "Salesforce data": sched["Type"]
                })
    # DiscountRate (tiers)
    discount_rate_block = {}
    for idx, tier in enumerate(discount_rate):
        # Add safety check for None values
        if tier and isinstance(tier, dict):
            tier_name = tier.get("InvoiceScheduleTierName") or f"Tier {idx+1}"
            discount_rate_block[tier_name] = [
                {
                    "Field": "Lower Bound",
                    "AI-Extracted": tier.get("LowerBound"),
                    "Salesforce data": tier.get("LowerBound")
                },
                {
                    "Field": "Upper Bound",
                    "AI-Extracted": tier.get("UpperBound"),
                    "Salesforce data": tier.get("UpperBound")
                },
                {
                    "Field": "Discount",
                    "AI-Extracted": tier.get("Discount"),
                    "Salesforce data": tier.get("Discount")
                }
            ]
    return {
        "SubscriptionDetails": subscription_details,
        "SubscriptionConsumptionSchedule": [],
        "SubscriptionRates": [],
        "LineItemSources": line_item_sources_block,
        "discounts": [
            {
                "DiscountSchedule": discount_schedule_block,
                "DiscountRate": discount_rate_block
            }
        ]
    }


def update_comparison_json(comparison_json_path, filename, account_name, subscription_name, line_item_sources, discount_schedule, discount_rate):
    with open(comparison_json_path, 'r', encoding='utf-8') as f:
        comparison_data = json.load(f)
    # Find the correct contract/account key by matching filename or account_name
    target_key = None
    for k, v in comparison_data.items():
        if v is not None and (v.get('filename', '').lower() == filename.lower() or k == account_name):
            target_key = k
            break
    if not target_key:
        raise ValueError(f"Could not find contract/account for filename '{filename}' or account '{account_name}' in comparison JSON.")
    # Build the new subscription block
    new_sub_block = build_comparison_subscription_block(subscription_name, line_item_sources, discount_schedule, discount_rate)
    # Append to the subscriptions list
    if 'subscriptions' not in comparison_data[target_key]:
        comparison_data[target_key]['subscriptions'] = []
    comparison_data[target_key]['subscriptions'].append(new_sub_block)
    # Save back
    with open(comparison_json_path, 'w', encoding='utf-8') as f:
        json.dump(comparison_data, f, indent=2)

def merge_and_save_subscription(account_files, filename_to_account, subscription_name, filename, extraction_json_obj, comparison_json_obj=None):
    # Keep the original filename for comparison JSON
    original_filename = filename
    filename = strip_pdf(filename)  # Only for extraction JSON and Salesforce logic
    result = add_subscription(account_files, filename_to_account, subscription_name, filename)
    # Work with the in-memory extraction_json_obj
    original_data = extraction_json_obj.copy()
    for key in ['Subscription', 'LineItemSource', 'discountSchedule', 'discountRate']:
        if key in result and key in original_data:
            original_data[key].extend(result[key])
        elif key in result:
            original_data[key] = result[key]
    # Update comparison JSON if provided
    updated_comparison_json = None
    if comparison_json_obj is not None:
        norm_filename = normalize_filename(filename)
        account_name = filename_to_account.get(norm_filename, {}).get('AccountName')
        updated_comparison_json = update_comparison_json_in_memory(
            comparison_json_obj,
            original_filename,  # Use the original filename with .pdf
            account_name,
            subscription_name,
            result['LineItemSource'],
            result['discountSchedule'],
            result['discountRate']
        )
    return original_data, updated_comparison_json

# New: in-memory version of update_comparison_json
def update_comparison_json_in_memory(comparison_data, filename, account_name, subscription_name, line_item_sources, discount_schedule, discount_rate):
    # DEBUG: Add logging to see what's causing the issue
    print(f"DEBUG: filename='{filename}', account_name='{account_name}'")
    print(f"DEBUG: comparison_data keys: {list(comparison_data.keys())}")
    
    # Find the correct contract/account key by matching filename or account_name
    target_key = None
    for k, v in comparison_data.items():
        print(f"DEBUG: Processing key='{k}', v type={type(v)}, v={v}")
        if v is not None and isinstance(v, dict) and (v.get('filename', '').lower() == filename.lower() or k == account_name):
            target_key = k
            break
    
    if not target_key:
        raise ValueError(f"Could not find contract/account for filename '{filename}' or account '{account_name}' in comparison JSON.")
    
    # Check if subscription already exists
    existing_sub_index = None
    if 'subscriptions' in comparison_data[target_key]:
        for i, sub in enumerate(comparison_data[target_key]['subscriptions']):
            # Check if subscription name matches in SubscriptionDetails
            for detail in sub.get('SubscriptionDetails', []):
                if detail.get('Field') == 'SubscriptionName' and detail.get('Salesforce data') == subscription_name:
                    existing_sub_index = i
                    break
            if existing_sub_index is not None:
                break
    
    # Build the new subscription block
    new_sub_block = build_comparison_subscription_block(subscription_name, line_item_sources, discount_schedule, discount_rate)
    
    if 'subscriptions' not in comparison_data[target_key]:
        comparison_data[target_key]['subscriptions'] = []
    
    if existing_sub_index is not None:
        # Update existing subscription
        comparison_data[target_key]['subscriptions'][existing_sub_index] = new_sub_block
    else:
        # Add new subscription
        comparison_data[target_key]['subscriptions'].append(new_sub_block)
    
    return comparison_data

# Refactored main
def main(extraction_json_obj, subscription_name, filename, comparison_json_obj=None):
    compiler = RecordCompiler(extraction_json_obj)
    account_files, filename_to_account = compiler.compile()
    print('account_files and filename_to_account generated in memory.')
    merged_json, updated_comparison_json = merge_and_save_subscription(
        account_files,
        filename_to_account,
        subscription_name,
        filename,
        extraction_json_obj,
        comparison_json_obj
    )
    return merged_json, updated_comparison_json

# if __name__ == '__main__':
#     extraction_json_path = 'BACKEND/THIRDV/amendments/extracted_data_updated.json'
#     subscription_name = 'United States Transactions'
#     filename = 'AmWager Trulioo_DealRoom-Trulioo-17128-26-Feb-2025-19-52-26-signed.pdf'
#     output_path = 'new_subscription_output.json' # Define output path 
#     comparison_json_path = 'BACKEND/THIRDV/COMPARISON/compr_main_output.json' # Example path

#     # File I/O only here
#     with open(extraction_json_path, 'r', encoding='utf-8') as f:
#         extraction_json_obj = json.load(f)
#     comparison_json_obj = None
#     if comparison_json_path:
#         with open(comparison_json_path, 'r', encoding='utf-8') as f:
#             comparison_json_obj = json.load(f)
#     merged_json, updated_comparison_json = main(
#         extraction_json_obj,
#         subscription_name,
#         filename,
#         comparison_json_obj
#     )
#     with open(output_path, 'w', encoding='utf-8') as f:
#         json.dump(merged_json, f, indent=2)
#     if comparison_json_path and updated_comparison_json is not None:
#         with open(comparison_json_path, 'w', encoding='utf-8') as f:
#             json.dump(updated_comparison_json, f, indent=2)
#     print(f'Merged extraction JSON with new subscription saved to {output_path}')
#     if comparison_json_path:
#         print(f'Updated comparison JSON at {comparison_json_path}')