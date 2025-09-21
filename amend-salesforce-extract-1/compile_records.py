"""
RecordCompiler class: Compiles account_files.json and filename_to_account.json from Salesforce and extraction logs.

Inputs:
- extraction_json: JSON file containing extraction logs (should have a 'Logs' key with file info and contract IDs).
- Salesforce connection: Used to query contract, account, and file information.

Outputs:
- account_files.json: Maps Account IDs to account names and a dictionary of filenames to contract start dates.
- filename_to_account.json: Maps normalized filenames to Account ID, Account Name, Start Date, and Contract ID.

Purpose:
- Queries Salesforce for contract and file info.
- Matches log files to accounts and contracts.
- Outputs account_files.json and filename_to_account.json for downstream processing.
"""
import json
import re
from simple_salesforce import Salesforce
from config import get_ssm_param

class RecordCompiler:
    def __init__(self, extraction_json):
        self.extraction_json = extraction_json
        self.acc_dict = {}
        self.filename_to_account = {}

    def normalize_filename(self, name):
        # Remove all extensions (e.g., .pdf, .docx, .xlsx, etc.)
        return re.sub(r'(\.[a-zA-Z0-9]+)+$', '', name).strip().lower()

    def query_salesforce(self, return_records=False):

        sf = Salesforce(
            username = get_ssm_param("/myapp/sf_username"),
            password = get_ssm_param("/myapp/sf_password"),
            security_token = get_ssm_param("/myapp/sf_security_token"),
            domain = get_ssm_param("/myapp/sf_domain")
        )
        # Query ContentDocumentLink/ContentDocument
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
            acc_name = None
            account = linked_entity.get('Account')
            if account:
                acc_name = account.get('Name')
            fname = rec.get('ContentDocument', {}).get('Title')
            date = linked_entity.get('StartDate')
            if not acc_id or not fname:
                continue
            if acc_id not in self.acc_dict:
                self.acc_dict[acc_id] = {'AccountName': acc_name, 'Files': {}}
            self.acc_dict[acc_id]['Files'][fname] = date
        # Query Attachment (get ParentId = Contract Id)
        attachment_query = '''SELECT Id, Name, ContentType, BodyLength, ParentId
        FROM Attachment
        WHERE ParentId IN (SELECT Id FROM Contract)'''
        att_res = sf.query_all(attachment_query)
        att_records = att_res['records']
        # Collect all unique ParentIds (Contract Ids)
        contract_ids = list({att['ParentId'] for att in att_records if 'ParentId' in att})
        # Query Contracts for Account info
        contract_info = {}
        if contract_ids:
            # Salesforce query limit: break into chunks of 2000
            chunk_size = 2000
            for i in range(0, len(contract_ids), chunk_size):
                chunk = contract_ids[i:i+chunk_size]
                ids_str = ",".join([f"'{cid}'" for cid in chunk])
                contract_query = f"""SELECT Id, AccountId, Account.Name, StartDate FROM Contract WHERE Id IN ({ids_str})"""
                contract_res = sf.query_all(contract_query)
                for c in contract_res['records']:
                    contract_info[c['Id']] = {
                        'AccountId': c.get('AccountId'),
                        'AccountName': c.get('Account', {}).get('Name') if c.get('Account') else None,
                        'StartDate': c.get('StartDate')
                    }
        # Add Attachments to acc_dict
        for att in att_records:
            contract_id = att.get('ParentId')
            fname = att.get('Name')
            if not contract_id or not fname:
                continue
            cinfo = contract_info.get(contract_id)
            if not cinfo:
                continue
            acc_id = cinfo['AccountId']
            acc_name = cinfo['AccountName']
            date = cinfo['StartDate']
            if not acc_id:
                continue
            if acc_id not in self.acc_dict:
                self.acc_dict[acc_id] = {'AccountName': acc_name, 'Files': {}}
            self.acc_dict[acc_id]['Files'][fname] = date
        if return_records:
            return {
                'contentdocument_records': records,
                'attachment_records': att_records,
                'contract_records': list(contract_info.values())
            }

    def compile(self):
        # Step 1: Query Salesforce and build account_files.json
        self.query_salesforce()
        # Step 2: For each FileName in Logs, match to Files in acc_dict
        logs = self.extraction_json.get('Logs', [])
        # Build a lookup: normalized filename -> (AccountId, AccountName, StartDate)
        file_lookup = {}
        for acc_id, acc_info in self.acc_dict.items():
            acc_name = acc_info.get('AccountName')
            for fname, date in acc_info.get('Files', {}).items():
                norm = self.normalize_filename(fname)
                file_lookup[norm] = {'AccountId': acc_id, 'AccountName': acc_name, 'StartDate': date}
        for log in logs:
            log_fname = log.get('FileName', '')
            norm_log_fname = self.normalize_filename(log_fname)
            match = file_lookup.get(norm_log_fname)
            if match:
                self.filename_to_account[norm_log_fname] = {
                    'AccountId': match['AccountId'],
                    'AccountName': match['AccountName'],
                    'StartDate': match['StartDate'],
                    'Contractid': log.get('Contractid')
                }
        return self.acc_dict, self.filename_to_account

# if __name__ == '__main__':
#     compiler = RecordCompiler('BACKEND/THIRDV/amendments/extracted_data_updated.json')
#     # Optionally save raw Salesforce query output for debugging
#     records = compiler.query_salesforce(return_records=True)
#     account_files, filename_to_account = compiler.compile()
#     print(json.dumps(account_files, indent=2))
#     print(json.dumps(filename_to_account, indent=2))
#     with open('account_files3.json', 'w', encoding='utf-8') as f:
#         json.dump(account_files, f, indent=2)
#     with open('filename_to_account.json', 'w', encoding='utf-8') as f:
#         json.dump(filename_to_account, f, indent=2)