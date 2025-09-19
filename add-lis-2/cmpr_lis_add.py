"""
cmpr_lis_add.py

Purpose:
- This script adds a new LineItemSource record under an existing subscription in your extraction JSON, using Salesforce data and your mapping files, in the exact format your pipeline expects.
- It also updates a comparison JSON file to add the new LineItemSource under the correct subscription, with both AI-Extracted and Salesforce data set to the new lisName.

Inputs:
- extraction_json: The loaded extraction JSON object (not a file path)
- subscriptionName: The subscription (product) name under which to add the LineItemSource (e.g., 'United States Transactions')
- filename: The contract PDF filename to match (e.g., 'AmWager Trulioo_DealRoom-Trulioo-17128-26-Feb-2025-19-52-26-signed.pdf')
- lisName: The name of the LineItemSource to add (e.g., 'Credit Agency')
- comparison_json: (Optional) The loaded comparison JSON object (not a file path)
- Salesforce credentials via environment variables

Output:
- The merged extraction JSON object, including all original records plus the new LineItemSource record.
- The updated comparison JSON object, with the new LineItemSource added under the correct subscription for both AI-Extracted and Salesforce data (if provided).

Note:
- The main() function now takes loaded JSON objects (not file paths) and returns the merged JSON object (not a file).
- All file reading and writing (input and output) is handled in the if __name__ == '__main__' block.
- Output files are only written if you choose to save them in the main block.
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
    return re.sub(r'(\.[a-zA-Z0-9]+)+$', '', name).strip().lower()

def strip_pdf(filename):
    return filename[:-4] if filename.lower().endswith('.pdf') else filename

def safe_id(s):
    return s.replace(' ', '_') if s else ''

class RecordCompiler:
    def __init__(self, extraction_json_obj):
        self.extraction_json_obj = extraction_json_obj
        self.acc_dict = {}
        self.filename_to_account = {}
        self._sf_contractid_map = {}

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
            # New fields
            base_addon = linked_entity.get('BaseAddon__c')
            owner_id = linked_entity.get('OwnerId')
            start_date_c = linked_entity.get('StartDate__c')
            if not acc_id or not fname:
                continue
            if acc_id not in self.acc_dict:
                self.acc_dict[acc_id] = {'AccountName': acc_name, 'Files': {}}
            self.acc_dict[acc_id]['Files'][fname] = date
            norm = normalize_filename(fname)
            self._sf_contractid_map[norm] = salesforce_contract_id
            # Optionally, store these new fields in the mapping for future use
            self.acc_dict[acc_id]['Files'][fname + '_BaseAddon__c'] = base_addon
            self.acc_dict[acc_id]['Files'][fname + '_OwnerId'] = owner_id
            self.acc_dict[acc_id]['Files'][fname + '_StartDate__c'] = start_date_c

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
                    'Contractid': log.get('Contractid'),
                    'SalesforceContractId': self._sf_contractid_map.get(norm_log_fname)
                }
        return self.acc_dict, self.filename_to_account

def add_lis(account_files, filename_to_account, extraction_json_obj, subscription_name, filename, lis_name):
    filename = strip_pdf(filename)
    norm_filename = normalize_filename(filename)
    # Get Contractid (UUID) and SalesforceContractId for the filename
    contractid_uuid = filename_to_account.get(norm_filename, {}).get('Contractid')
    salesforce_contract_id = filename_to_account.get(norm_filename, {}).get('SalesforceContractId')
    if not contractid_uuid or not salesforce_contract_id:
        raise ValueError(f"Contractid or SalesforceContractId for filename '{filename}' not found in filename_to_account.")
    # Work with the in-memory extraction_json_obj
    original_data = extraction_json_obj.copy()
    # Find the existing subscription (with fallback)
    subExternalId = None
    sub_name_to_try = [subscription_name]
    # Fallbacks for 'Transactions'
    if subscription_name.lower().endswith(' transactions'):
        sub_name_to_try.append(subscription_name[:-len(' transactions')].rstrip())
    if subscription_name.lower().endswith('transactions'):
        sub_name_to_try.append(subscription_name[:-len('transactions')].rstrip())
    found_sub = None
    for try_name in sub_name_to_try:
        for sub in original_data.get('Subscription', []):
            if sub.get('ProductName') == try_name and sub.get('ContractExternalId') == contractid_uuid:
                subExternalId = sub.get('subExternalId')
                found_sub = sub
                break
        if subExternalId:
            break
    if not subExternalId:
        raise ValueError(f"No subscription found for ProductName '{subscription_name}' (or fallback) and ContractExternalId '{contractid_uuid}'.")
    # Query Salesforce for the Subscription Id
    sf = Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        domain=domain
    )
    # Try all fallback names for Salesforce Subscription
    subscription_sf_id = None
    for try_name in sub_name_to_try:
        sub_query = f"""
        SELECT Id
        FROM SBQQ__Subscription__c
        WHERE SBQQ__Contract__c = '{salesforce_contract_id}'
        AND SBQQ__ProductName__c = '{try_name}'
        """
        sub_res = sf.query_all(sub_query)
        sub_records = sub_res.get('records', [])
        if sub_records:
            subscription_sf_id = sub_records[0]['Id']
            break
    if not subscription_sf_id:
        raise ValueError(f"No Salesforce subscription found for contract {salesforce_contract_id} and product {subscription_name} (or fallback).")
    # Query Salesforce for the LineItemSource__c record(s) for that Subscription Id and lisName
    lis_query = f"""
    SELECT Id, Name, BaseAddon__c, OwnerId, StartDate__c, Product__r.Name
    FROM LineItemSource__c
    WHERE Subscription__c = '{subscription_sf_id}'
    AND (Name = '{lis_name}' OR Product__r.Name = '{lis_name}')
    """
    lis_res = sf.query_all(lis_query)
    lis_records = lis_res.get('records', [])
    if not lis_records:
        raise ValueError(f"No LineItemSource found in Salesforce for Subscription Id '{subscription_sf_id}' and lisName '{lis_name}'.")
    # Use the first matching LIS
    lis = lis_records[0]
    product_r = lis.get('Product__r')
    lis_name_actual = product_r.get('Name') if isinstance(product_r, dict) else lis.get('Name')
    lis_external_id = f"cmpr_{safe_id(lis_name_actual)}_{subExternalId}"
    # Build new LineItemSource record
    new_lis = {
        'subExternalId': subExternalId,
        'lisName': lis_name_actual,
        'lisExternalId': lis_external_id,
        'BaseAddon__c': lis.get('BaseAddon__c', ""),
        'OwnerId': lis.get('OwnerId', ""),
        'StartDate__c': lis.get('StartDate__c', ""),
        'Note': 'Added from Comparison Hub',
        'CurrencyIsoCode': 'USD'
    }
    # Append to LineItemSource
    if 'LineItemSource' not in original_data:
        original_data['LineItemSource'] = []
    original_data['LineItemSource'].append(new_lis)
    return original_data

# In-memory version of update_comparison_json_lis
def update_comparison_json_lis_in_memory(comparison_data, filename, subscription_name, lis_name):
    # Find the correct contract/account key by matching filename (with .pdf)
    target_key = None
    for k, v in comparison_data.items():
        if v.get('filename', '').lower() == filename.lower():
            target_key = k
            break
    if not target_key:
        raise ValueError(f"Could not find contract/account for filename '{filename}' in comparison JSON.")
    
    # Find the correct subscription block by subscription name, with fallback
    subs = comparison_data[target_key].get('subscriptions', [])
    found_sub = None
    sub_name_to_try = [subscription_name]
    # Fallbacks for 'Transactions'
    if subscription_name.lower().endswith(' transactions'):
        sub_name_to_try.append(subscription_name[:-len(' transactions')].rstrip())
    if subscription_name.lower().endswith('transactions'):
        sub_name_to_try.append(subscription_name[:-len('transactions')].rstrip())
    
    for try_name in sub_name_to_try:
        for sub in subs:
            details = sub.get('SubscriptionDetails', [])
            for d in details:
                if d.get('Field') == 'SubscriptionName' and d.get('AI-Extracted', '').lower() == try_name.lower():
                    found_sub = sub
                    break
            if found_sub:
                break
        if found_sub:
            break
    
    if not found_sub:
        raise ValueError(f"Could not find subscription block for subscription name '{subscription_name}' (or fallback) in comparison JSON.")
    
    # Check if LineItemSource already exists in the list
    if 'LineItemSources' not in found_sub:
        found_sub['LineItemSources'] = []
    
    # Look for existing entry that matches the lis_name in Salesforce data
    existing_entry_index = None
    for i, lis_entry in enumerate(found_sub['LineItemSources']):
        if (lis_entry.get('Field') == 'LineItemSourceName' and 
            lis_entry.get('Salesforce data') == lis_name and
            (lis_entry.get('AI-Extracted') == 'Not found' or 
             lis_entry.get('AI-Extracted', '').strip() == '')):
            existing_entry_index = i
            break
    
    if existing_entry_index is not None:
        # Update existing entry
        found_sub['LineItemSources'][existing_entry_index]['AI-Extracted'] = lis_name
    else:
        # Add new entry only if no existing entry was found
        found_sub['LineItemSources'].append({
            "Field": "LineItemSourceName",
            "AI-Extracted": lis_name,
            "Salesforce data": lis_name
        })
    
    return comparison_data

# Refactored main
def main(extraction_json_obj, subscription_name, filename, lis_name, comparison_json_obj=None):
    compiler = RecordCompiler(extraction_json_obj)
    account_files, filename_to_account = compiler.compile()
    print('account_files and filename_to_account generated in memory.')
    merged_data = add_lis(
        account_files,
        filename_to_account,
        extraction_json_obj,
        subscription_name,
        filename,
        lis_name
    )
    updated_comparison_json = None
    if comparison_json_obj is not None:
        updated_comparison_json = update_comparison_json_lis_in_memory(
            comparison_json_obj,
            filename,  # Use the full filename with .pdf
            subscription_name,
            lis_name
        )
    return merged_data, updated_comparison_json

# if __name__ == '__main__':
#     extraction_json_path = 'BACKEND/THIRDV/amendments/extracted_data_updated.json'
#     subscription_name = 'United States Transactions'
#     filename = 'AmWager Trulioo_DealRoom-Trulioo-17128-26-Feb-2025-19-52-26-signed.pdf'
#     lis_name = 'Enhanced Credit 1 SuperS'
#     output_path = 'new_lis_output.json'
#     comparison_json_path = 'BACKEND/THIRDV/COMPARISON/compr_main_output_deleted_lis.json' # Example path

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
#         lis_name,
#         comparison_json_obj
#     )
#     with open(output_path, 'w', encoding='utf-8') as f:
#         json.dump(merged_json, f, indent=2)
#     if comparison_json_path and updated_comparison_json is not None:
#         with open(comparison_json_path, 'w', encoding='utf-8') as f:
#             json.dump(updated_comparison_json, f, indent=2)
#     print(f'Merged extraction JSON with new LineItemSource saved to {output_path}')
#     if comparison_json_path:
#         print(f'Updated comparison JSON at {comparison_json_path}')