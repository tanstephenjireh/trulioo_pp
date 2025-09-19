"""
This script connects to Salesforce, fetches contract file and subscription data, and joins it with input account/filename mappings.

Input:
- JSON string or file mapping account names to contract PDF filenames.
  Example:
  {
      "AccountName1": "ContractFile1.pdf",
      "AccountName2": "ContractFile2.pdf"
  }

Output:
- JSON object mapping each account to its contract filename and a list of subscriptions from Salesforce.
- Each subscription includes:
    - SubscriptionName
    - LineItemSources: list of dicts {"LisName": <Product__r.Name>}
    - Discount: {
        "DiscountSchedule": {"DiscountUnit": ..., "Type": ...},
        "DiscountRate": [
            {"Name": ..., "Lower Bound": ..., "Upper Bound": ..., "Discount": ...},
            ...
        ]
      }

Example output:
{
    "AccountName": {
        "filename": <FileName>,
        "subscriptions": [
            {
                "SubscriptionName": <ProductName>,
                "LineItemSources": [
                    {"LisName": <Product__r.Name>}, ...
                ],
                "Discount": {
                    "DiscountSchedule": {
                        "DiscountUnit": <unit>,
                        "Type": <type>
                    },
                    "DiscountRate": [
                        {
                            "Name": <tier name>,
                            "Lower Bound": <lower>,
                            "Upper Bound": <upper>,
                            "Discount": <discount>
                        }, ...
                    ]
                }
            }, ...
        ]
    }, ...
}

How it works (API call logic):
- The script makes only 4 Salesforce API calls, regardless of data size:
    1. Fetch all contract files.
    2. Fetch all subscriptions for all contracts in one call.
    3. Fetch all line item sources for all contracts in one call.
    4. Fetch all discount tiers for all discount schedules in one call.
- All data is joined in memory to produce the output.
"""
import json
import os
from simple_salesforce import Salesforce
from config import get_ssm_param

class SalesforceFileLookup:
    """
    Connects to Salesforce and fetches contract file and subscription data for accounts.
    """
    def __init__(self, username, password, security_token, domain='test'):
        """
        Initialize Salesforce connection with credentials.
        """
        try:
            self.sf = Salesforce(
                username=username,
                password=password,
                security_token=security_token,
                domain=domain
            )
            print("Successfully connected to Salesforce.")
        except Exception as e:
            print(f"Failed to connect to Salesforce: {e}")
            self.sf = None

    def _fetch_all_contract_files(self):
        """
        Fetch all files linked to contracts from Salesforce.
        Returns a mapping of file title to account and contract info.
        """
        if not self.sf:
            return {}

        print("\nFetching all files linked to contracts...")
        query = """
        SELECT 
          ContentDocument.Title,
          LinkedEntityId,
          TYPEOF LinkedEntity
            WHEN Contract THEN AccountId, Account.Name
          END
        FROM ContentDocumentLink
        WHERE LinkedEntityId IN (SELECT Id FROM Contract)
        """
        try:
            print("\n[QUERY]", query)
            res = self.sf.query_all(query)
            print("[DEBUG] _fetch_all_contract_files raw result:", json.dumps(res, indent=2))
            records = res.get('records', [])
            print(f"Fetched {len(records)} file links from Salesforce.")
        except Exception as e:
            print(f"Error during Salesforce query: {e}")
            return {}

        file_map = {}
        for rec in records:
            linked_entity = rec.get('LinkedEntity')
            if not linked_entity:
                continue
            account = linked_entity.get('Account')
            acc_id = linked_entity.get('AccountId')
            file_title = rec.get('ContentDocument', {}).get('Title')
            if account and acc_id and file_title:
                acc_name = account.get('Name')
                file_map[file_title] = {
                    'AccountId': acc_id,
                    'AccountName': acc_name,
                    'ContractId': rec.get('LinkedEntityId')
                }
        return file_map

    def _fetch_contract_files_by_filenames(self, filenames):
        """
        Fetch only files from Salesforce whose titles match the given filenames (no extension).
        """
        if not self.sf or not filenames:
            return {}
        # Remove extensions
        titles = [os.path.splitext(f)[0] for f in filenames]
        titles_str = ",".join([f"'{t}'" for t in titles])
        print(f"[DEBUG] Querying for file titles: {titles}")
        query = f"""
        SELECT 
          ContentDocument.Title,
          LinkedEntityId,
          TYPEOF LinkedEntity
            WHEN Contract THEN AccountId, Account.Name
          END
        FROM ContentDocumentLink
        WHERE LinkedEntityId IN (SELECT Id FROM Contract)
          AND ContentDocument.Title IN ({titles_str})
        """
        try:
            print("\n[QUERY]", query)
            res = self.sf.query_all(query)
            print("[DEBUG] _fetch_contract_files_by_filenames raw result:", json.dumps(res, indent=2))
            records = res.get('records', [])
            print(f"Fetched {len(records)} file links from Salesforce.")
        except Exception as e:
            print(f"Error during Salesforce query: {e}")
            return {}
        file_map = {}
        for rec in records:
            linked_entity = rec.get('LinkedEntity')
            if not linked_entity:
                continue
            account = linked_entity.get('Account')
            acc_id = linked_entity.get('AccountId')
            file_title = rec.get('ContentDocument', {}).get('Title')
            if account and acc_id and file_title:
                acc_name = account.get('Name')
                file_map[file_title] = {
                    'AccountId': acc_id,
                    'AccountName': acc_name,
                    'ContractId': rec.get('LinkedEntityId')
                }
        return file_map

    def _fetch_all_subscriptions(self, contract_ids):
        """
        Fetch all subscriptions for a list of contract IDs in one query.
        Returns a list of subscription records.
        """
        if not self.sf or not contract_ids:
            return []
        ids_str = ",".join([f"'{cid}'" for cid in contract_ids])
        query = f"""
        SELECT Id, Name, SBQQ__Product__r.Name, DiscountSched__c, SBQQ__Contract__c
        FROM SBQQ__Subscription__c
        WHERE SBQQ__Contract__c IN ({ids_str})
        """
        try:
            print("\n[QUERY]", query)
            res = self.sf.query_all(query)
            print("[DEBUG] _fetch_all_subscriptions raw result:", json.dumps(res, indent=2))
            return res.get('records', [])
        except Exception as e:
            print(f"Failed to fetch subscriptions for contracts: {e}")
            return []

    def _fetch_all_lineitemsources(self, contract_ids):
        """
        Fetch all LineItemSource__c records for a list of contract IDs in one query.
        Returns a list of line item source records.
        """
        if not self.sf or not contract_ids:
            return []
        ids_str = ",".join([f"'{cid}'" for cid in contract_ids])
        query = f"""
        SELECT Id, Name, Product__r.Name, Subscription__r.Name, Subscription__r.SBQQ__Contract__c
        FROM LineItemSource__c
        WHERE Subscription__r.SBQQ__Contract__c IN ({ids_str})
        """
        try:
            print("\n[QUERY]", query)
            res = self.sf.query_all(query)
            print("[DEBUG] _fetch_all_lineitemsources raw result:", json.dumps(res, indent=2))
            return res.get('records', [])
        except Exception as e:
            print(f"Failed to fetch line item sources for contracts: {e}")
            return []

    def _fetch_all_discount_tiers(self, discount_sched_ids):
        """
        Fetch all discount tiers for a list of DiscountSched__c IDs in one query.
        Returns a list of discount tier records.
        """
        if not self.sf or not discount_sched_ids:
            return []
        ids_str = ",".join([f"'{dsid}'" for dsid in discount_sched_ids])
        query = f"""
        SELECT InvoiceSchedule__r.Id, Id, Name, InvoiceSchedule__r.DiscountUnit__c, InvoiceSchedule__r.Type__c, InvoiceSchedule__r.Name, LowerBound__c, UpperBound__c, DiscountAmount__c, Discount__c
        FROM InvoiceScheduleTier__c
        WHERE InvoiceSchedule__r.Id IN ({ids_str})
        """
        try:
            print("\n[QUERY]", query)
            res = self.sf.query_all(query)
            print("[DEBUG] _fetch_all_discount_tiers raw result:", json.dumps(res, indent=2))
            return res.get('records', [])
        except Exception as e:
            print(f"Failed to fetch discount tiers for DiscountSched__c: {e}")
            return []

    def _fetch_activated_contract_ids(self, account_ids):
        """
        Given a list of AccountIds, fetch all Contract Ids where Status='Activated'.
        Returns a dict: {account_id: [contract_id, ...], ...}
        """
        if not self.sf or not account_ids:
            return {}
        ids_str = ",".join([f"'{aid}'" for aid in account_ids])
        query = f"""
        SELECT Id, AccountId FROM Contract WHERE AccountId IN ({ids_str}) AND Status = 'Activated'
        """
        try:
            print("\n[QUERY]", query)
            res = self.sf.query_all(query)
            records = res.get('records', [])
            print(f"Fetched {len(records)} activated contracts from Salesforce.")
        except Exception as e:
            print(f"Error during Salesforce contract query: {e}")
            return {}
        acc_to_contracts = {}
        for rec in records:
            acc_id = rec.get('AccountId')
            contract_id = rec.get('Id')
            if acc_id and contract_id:
                acc_to_contracts.setdefault(acc_id, []).append(contract_id)
        return acc_to_contracts

    def _join_with_input_data(self, input_data, sf_file_map):
        print("\nJoining Salesforce data with input JSON (optimized, by AccountId -> Activated Contract)...")
        final_output = {}
        # 1. Collect all AccountIds from sf_file_map
        account_ids = [v['AccountId'] for v in sf_file_map.values() if v.get('AccountId')]
        # 2. Fetch all activated contract IDs for these accounts
        acc_to_contracts = self._fetch_activated_contract_ids(account_ids)
        # 3. Flatten all contract IDs
        contract_ids = [cid for cids in acc_to_contracts.values() for cid in cids]
        # 4. Fetch all subscriptions for all contracts
        all_subs = self._fetch_all_subscriptions(contract_ids)
        # 5. Fetch all line item sources for all contracts
        all_lis = self._fetch_all_lineitemsources(contract_ids)
        # 6. Collect all unique discount schedule IDs
        discount_sched_ids = list({rec.get('DiscountSched__c') for rec in all_subs if rec.get('DiscountSched__c')})
        # 7. Fetch all discount tiers for all discount schedule IDs
        all_tiers = self._fetch_all_discount_tiers(discount_sched_ids)
        # 8. Build maps for fast lookup
        contract_to_subs = {}
        for rec in all_subs:
            contract_to_subs.setdefault(rec.get('SBQQ__Contract__c'), []).append(rec)
        contract_to_lis = {}
        for rec in all_lis:
            contract_id = rec.get('Subscription__r', {}).get('SBQQ__Contract__c')
            sub_name = rec.get('Subscription__r', {}).get('Name')
            lis_name = (rec.get('Product__r') or {}).get('Name')
            if contract_id and sub_name and lis_name:
                contract_to_lis.setdefault(contract_id, {}).setdefault(sub_name, []).append({'LisName': lis_name})
        dsid_to_discount = {}
        for rec in all_tiers:
            dsid = rec.get('InvoiceSchedule__r', {}).get('Id')
            if not dsid:
                continue
            if dsid not in dsid_to_discount:
                dsid_to_discount[dsid] = {
                    'DiscountSchedule': {
                        'DiscountUnit': rec.get('InvoiceSchedule__r', {}).get('DiscountUnit__c'),
                        'Type': rec.get('InvoiceSchedule__r', {}).get('Type__c')
                    },
                    'DiscountRate': []
                }
            dsid_to_discount[dsid]['DiscountRate'].append({
                'Name': rec.get('Name'),
                'Lower Bound': rec.get('LowerBound__c'),
                'Upper Bound': rec.get('UpperBound__c'),
                'Discount': rec.get('Discount__c') or rec.get('DiscountAmount__c')
            })
        # 9. Join everything by input account name
        for input_acc_name, pdf_filename in input_data.items():
            file_title_no_ext = os.path.splitext(pdf_filename)[0]
            sf_data = sf_file_map.get(file_title_no_ext)
            if sf_data:
                acc_id = sf_data.get('AccountId')
                contract_ids = acc_to_contracts.get(acc_id, [])
                out_subs = []
                for contract_id in contract_ids:
                    subs = contract_to_subs.get(contract_id, [])
                    for sub in subs:
                        sub_name = sub.get('Name')
                        prod_name = sub.get('SBQQ__Product__r', {}).get('Name')
                        discount_sched_id = sub.get('DiscountSched__c')
                        discount_info = dsid_to_discount.get(discount_sched_id, {'DiscountSchedule': {}, 'DiscountRate': []})
                        if prod_name:
                            out_subs.append({
                                'SubscriptionName': prod_name,
                                'LineItemSources': contract_to_lis.get(contract_id, {}).get(sub_name, []),
                                'Discount': {
                                    'DiscountSchedule': discount_info['DiscountSchedule'],
                                    'DiscountRate': discount_info['DiscountRate']
                                }
                            })
                final_output[input_acc_name] = {
                    'filename': pdf_filename,
                    'subscriptions': out_subs if out_subs else []
                }
            else:
                final_output[input_acc_name] = {
                    'filename': pdf_filename,
                    'subscriptions': []
                }
        return final_output

    def process_data(self, input_data):
        """
        Main entry point: fetch Salesforce data and join with input mapping.
        Returns the joined output dict.
        """
        filenames = list(input_data.values())
        sf_file_map = self._fetch_contract_files_by_filenames(filenames)
        if not sf_file_map:
            print("Could not fetch Salesforce data or no files linked to contracts.")
            return {}
        return self._join_with_input_data(input_data, sf_file_map)

def main(json_text):
    """
    Entrypoint for running as a module: loads input JSON, processes, and prints output.
    """
    SF_USERNAME = get_ssm_param("/myapp/sf_username")
    SF_PASSWORD = get_ssm_param("/myapp/sf_password")
    SF_SECURITY_TOKEN = get_ssm_param("/myapp/sf_security_token")
    SF_DOMAIN = get_ssm_param("/myapp/sf_domain")
    input_json_data = json.loads(json_text)
    lookup = SalesforceFileLookup(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN,
        domain=SF_DOMAIN
    )
    final_data = lookup.process_data(input_json_data)
    print("\n--- Final Processed Data ---")
    if final_data:
        print(json.dumps(final_data, indent=2))
    else:
        print("No data was processed.")
    return final_data

# if __name__ == '__main__':
#     # Reads input JSON from file and runs the main pipeline.
#     input_json_path = 'BACKEND/THIRDV/COMPARISON/dummy_input.json'  # You can change this to your input file
#     with open(input_json_path, 'r') as f:
#         json_text = f.read()
#     main(json_text)