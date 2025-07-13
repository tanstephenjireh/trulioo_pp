from simple_salesforce import Salesforce
from config import get_ssm_param
import logging

import gspread  # pip install gspread oauth2client
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

class SalesForce:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")
        self.gs_url = 'https://docs.google.com/spreadsheets/d/1uDA-59DhXE5rld3UBawrLb5iERNsLL8Nyd8kweSuqaw/edit?gid=309153661#gid=309153661'
        self.creds_path = 'trulioo-af66af3a3788.json'

    def update_subscription_names_from_country(self, subscriptions, country_map):
        """
        For each item in subscriptions, if ProductName matches a key in country_map, replace it with the mapped value.
        Prints a message to the terminal for each replacement.
        """

        for item in subscriptions:
            pname = item.get('ProductName')
            if pname in country_map:
                print(f"[Country Mapping] ProductName replaced: '{pname}' -> '{country_map[pname]}'")
                item['ProductName'] = country_map[pname]
                item['Note_country_mapping'] = f"ProductName replaced from '{pname}' to '{country_map[pname]}' via country sheet."

    def get_gsheet_dataframes(self, spreadsheet_url, creds_json_path):
        """
        Connects to a Google Sheet and returns a dict of {sheet_name: DataFrame}.
        Args:
            spreadsheet_url (str): The URL of the Google Sheet.
            creds_json_path (str): Path to your Google Service Account credentials JSON file.
        Returns:
            dict: {sheet_name: pd.DataFrame}
        """
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scope)  # type: ignore
        client = gspread.authorize(creds)  
        
        spreadsheet = client.open_by_url(spreadsheet_url)
        
        # Fetch all worksheets
        dataframes = {}
        for worksheet in spreadsheet.worksheets():
            # Get all values as a list of lists
            data = worksheet.get_all_values()
            if data:
                # First row is header, ensure columns are str and use pd.Index for type safety
                columns = pd.Index([str(col) for col in data[0]])
                df = pd.DataFrame(data[1:], columns=columns)  # type: ignore
                dataframes[worksheet.title] = df
        return dataframes

    def get_all_records(self, json_data, name):
        for group in json_data.get("output_records", []):
            if group.get("name") == name:
                return group.get("data", [])
        return []

    #======= Functions for subscription========
    def get_unique_product_names(self, subscriptions):
        # Gather all unique "ProductName Transactions" for querying Product2
        return {f"{item['ProductName']} Transactions" for item in subscriptions if item.get("ProductName")}

    def fetch_product2_fields(self, sf, product_names, batch_size=100):
        if not product_names:
            return {}
        product_names = list(product_names)
        all_records = {}
        for i in range(0, len(product_names), batch_size):
            batch = product_names[i:i+batch_size]
            names_str = "', '".join(batch)
            q = (
                "SELECT Id, Name, SBQQ__PricingMethod__c, SBQQ__SubscriptionPricing__c, SBQQ__SubscriptionType__c, "
                "SBQQ__BillingFrequency__c, SBQQ__ChargeType__c, SBQQ__BillingType__c, CreatedDate "
                "FROM Product2 "
                f"WHERE Family = 'Person Match' AND IsActive = TRUE AND Name IN ('{names_str}') "
                "ORDER BY Name, CreatedDate DESC"
            )
            res = sf.query_all(q)
            for r in res['records']:
                all_records.setdefault(r['Name'], []).append(r)
        return all_records
    
    def update_subscription_product_fields(self, subscriptions, product_field_map):
        subext_to_pid = {}
        extra_fields = [
            "SBQQ__PricingMethod__c",
            "SBQQ__SubscriptionPricing__c",
            "SBQQ__SubscriptionType__c",
            "SBQQ__BillingFrequency__c",
            "SBQQ__ChargeType__c",
            "SBQQ__BillingType__c"
        ]
        for item in subscriptions:
            pname = item.get("ProductName")
            key = f"{pname} Transactions"
            recs = product_field_map.get(key, [])
            if not recs:
                item["ProductId"] = None
                for f in extra_fields: item[f] = None
                item["Note"] = "No exact match found based on Subscription Name"
            else:
                item["ProductId"] = recs[0].get("Id")
                for f in extra_fields:
                    item[f] = recs[0].get(f)
                subext_to_pid[item["subExternalId"]] = recs[0].get("Id")
                if len(recs) > 1:
                    item["Note"] = "Duplicate Subscription Names found, Id was taken from latest CreatedDate"
                else:
                    item["Note"] = "Successfully Matched"
        return subext_to_pid
    
    #======= Functions for lis========
    def get_lineitem_keys(self, lineitems, subext_to_pid):
        # (Parent ProductId, lisName) pairs needed for querying options
        keymap = {}
        for item in lineitems:
            parent_pid = subext_to_pid.get(item.get("subExternalId"))
            lis_name = item.get("lisName")
            if parent_pid and lis_name:
                keymap[(parent_pid, lis_name)] = item
        return keymap
    
    def fetch_option_product_ids(self, sf, keymap, batch_size=50):
        if not keymap: return {}
        keylist = list(keymap.keys())
        all_records = {}
        for i in range(0, len(keylist), batch_size):
            batch = keylist[i:i+batch_size]
            ors = []
            for (pid, lis_name) in batch:
                escaped_lis_name = lis_name.replace("'", "\\'")
                ors.append(
                    f"(SBQQ__ConfiguredSKU__c = '{pid}' AND Option_Product_Name__c = '{escaped_lis_name}')"
                )
            q = (
                "SELECT Id, SBQQ__ConfiguredSKU__c, Option_Product_Name__c, "
                "SBQQ__OptionalSKU__c, CreatedDate, "
                "Component_Charge_Name__c, OwnerId "
                f"FROM SBQQ__ProductOption__c WHERE {' OR '.join(ors)} "
                "ORDER BY SBQQ__ConfiguredSKU__c, Option_Product_Name__c, CreatedDate DESC"
            )
            res = sf.query_all(q)
            for r in res['records']:
                key = (r['SBQQ__ConfiguredSKU__c'], r['Option_Product_Name__c'])
                all_records.setdefault(key, []).append(r)
        return all_records
    
    def update_lineitem_product_ids(self, lineitems, keymap, option_pid_map):
        for (parent_pid, lis_name), item in keymap.items():
            records = option_pid_map.get((parent_pid, lis_name), [])
            if not records:
                item['ProductId'] = None
                item['Option_Product_Name__c'] = None
                item['Component_Charge_Name__c'] = None
                item['OwnerId'] = None
                item['Note'] = "No exact match found based on Line Item Source Name"
            else:
                record = records[0]  # Take the latest by CreatedDate
                item['ProductId'] = record.get('SBQQ__OptionalSKU__c')
                item['Option_Product_Name__c'] = record.get('Option_Product_Name__c')
                item['Component_Charge_Name__c'] = record.get('Component_Charge_Name__c')
                item['OwnerId'] = record.get('OwnerId')
                item['Note'] = "Duplicate Line Item Source Names found, Id was taken from latest CreatedDate" if len(records) > 1 else "Successfully Matched"

    #======Functions for Sub Conscumption Schedule========
    def fetch_product_consumption_schedules(self, sf, product_ids, batch_size=100):
        if not product_ids:
            return {}
        product_ids = list(product_ids)
        pcs_map = {}
        for i in range(0, len(product_ids), batch_size):
            batch = product_ids[i:i+batch_size]
            ids_str = "', '".join(batch)
            q = (
                f"SELECT Id, Name, Product__c, CreatedDate FROM ProductConsumptionSchedule__c "
                f"WHERE Product__c IN ('{ids_str}') "
                f"ORDER BY Product__c, CreatedDate DESC"
            )
            res = sf.query_all(q)
            for rec in res['records']:
                prod_id = rec['Product__c']
                if prod_id not in pcs_map:
                    pcs_map[prod_id] = rec
        return pcs_map
    
    def fetch_consumption_schedules(self, sf, subscription_names, batch_size=100):
        if not subscription_names:
            return {}
        subscription_names = list(subscription_names)
        latest = {}
        for i in range(0, len(subscription_names), batch_size):
            batch = [f"{n} Transactions" for n in subscription_names[i:i+batch_size]]
            names_str = "', '".join(batch)
            q = (
                f"SELECT Id, Name, CreatedDate FROM ConsumptionSchedule__c "
                f"WHERE Name IN ('{names_str}') "
                f"ORDER BY Name, CreatedDate DESC"
            )
            res = sf.query_all(q)
            for r in res['records']:
                name = r['Name']
                if name not in latest:
                    latest[name] = r['Id']
        return latest
    
    def update_sub_consumption_schedules(self, schedules, subext_to_pid, pcs_map, consumption_schedule_map):
        for item in schedules:
            sub_ext_id = item.get("subExternalId")
            product_id = subext_to_pid.get(sub_ext_id)
            pcs_record = pcs_map.get(product_id)
            if pcs_record:
                item['pcsId'] = pcs_record['Id']
                item['pcsName'] = pcs_record['Name']
            else:
                item.pop('pcsId', None)
                item.pop('pcsName', None)
            # --- Add subCsId from ConsumptionSchedule__c ---
            subscr_name = item.get("subscriptionName")
            cs_name = f"{subscr_name} Transactions"
            consumption_id = consumption_schedule_map.get(cs_name)
            if consumption_id:
                item['subCsId'] = consumption_id
            else:
                item.pop('subCsId', None)

    #======Functions for Sub Conscumption Rate========
    def fetch_consumption_rates(self, sf, subcs_ids, batch_size=100):
        if not subcs_ids:
            return {}
        subcs_ids = list(subcs_ids)
        latest = {}
        for i in range(0, len(subcs_ids), batch_size):
            batch = subcs_ids[i:i+batch_size]
            ids_str = "', '".join(batch)
            q = (
                f"SELECT Id, ConsumptionSchedule__c, CreatedDate "
                f"FROM ConsumptionRate__c "
                f"WHERE ConsumptionSchedule__c IN ('{ids_str}') "
                f"ORDER BY ConsumptionSchedule__c, CreatedDate DESC"
            )
            res = sf.query_all(q)
            for r in res['records']:
                cs_id = r['ConsumptionSchedule__c']
                if cs_id not in latest:
                    latest[cs_id] = r['Id']
        return latest
    
    def update_sub_consumption_rates(self, rates, subext_to_subcsid, consumption_rate_map):
        for item in rates:
            sub_ext_id = item.get("subExternalId")
            subcs_id = subext_to_subcsid.get(sub_ext_id)
            subcr_id = consumption_rate_map.get(subcs_id)
            if subcr_id:
                item['subCrId'] = subcr_id
            else:
                item.pop('subCrId', None)

    #========MAIN PIPE==========
    def main(self, data):
        """
        Accepts the contract_subscription output as a dict, mutates it with Salesforce enrichment, and returns the result as a dict.
        """
        sf = Salesforce(
            username='consulting+trulioo@jumpr.io.trulioopsb',
            password='HPW@zfm0afv1hup5amh',
            security_token='8M87y8MwxarZ7xt1SPHsLhsG',
            domain='test'
        )

        dfs = self.get_gsheet_dataframes(self.gs_url, self.creds_path)

        country_df = dfs.get('country')
        country_map = {}
        if country_df is not None and country_df.shape[1] >= 2:
            # Build mapping from column A to column B
            country_map = dict(zip(country_df.iloc[:, 0], country_df.iloc[:, 1]))

        # --- SUBSCRIPTIONS ---
        subscriptions = self.get_all_records(data, "Subscription")
        self.update_subscription_names_from_country(subscriptions, country_map)
        product_names = self.get_unique_product_names(subscriptions)
        product_field_map = self.fetch_product2_fields(sf, product_names)
        subext_to_pid = self.update_subscription_product_fields(subscriptions, product_field_map)

        # --- LINE ITEM SOURCES ---
        lineitems = self.get_all_records(data, "LineItemSource")
        # --- SKU sheet mapping logic for lisName update ---
        sku_df = dfs.get('SKU')
        if sku_df is not None and sku_df.shape[1] >= 4:
            # Build a list of tuples for fast lookup: (ProductName, lisName) -> new_lisName
            sku_map = {}
            for idx, row in sku_df.iterrows():
                pname = row.iloc[0]
                old_lis = row.iloc[1]
                new_lis = row.iloc[3]
                sku_map[(pname, old_lis)] = new_lis
            # Update lisName in lineitems if both ProductName and lisName match
            for item in lineitems:
                parent_pid = subext_to_pid.get(item.get("subExternalId"))
                # Find the parent ProductName from subscriptions
                parent_sub = next((s for s in subscriptions if s.get("subExternalId") == item.get("subExternalId")), None)
                parent_pname = parent_sub.get("ProductName") if parent_sub else None
                lis_name = item.get("lisName")
                if parent_pname and lis_name and (parent_pname, lis_name) in sku_map:
                    old_lis = item["lisName"]
                    item["lisName"] = sku_map[(parent_pname, lis_name)]
                    item["Note_SKU_mapping"] = f"lisName replaced from '{old_lis}' to '{item['lisName']}' via SKU sheet."
                    print(f"[SKU Mapping] lisName replaced for ProductName '{parent_pname}': '{old_lis}' -> '{item['lisName']}'")
        keymap = self.get_lineitem_keys(lineitems, subext_to_pid)
        option_pid_map = self.fetch_option_product_ids(sf, keymap)
        self.update_lineitem_product_ids(lineitems, keymap, option_pid_map)
        
        # --- SUB CONSUMPTION SCHEDULES ---
        sub_consumption_schedules = self.get_all_records(data, "subConsumptionSchedule")
        product_ids = list(set(subext_to_pid.values()))
        pcs_map = self.fetch_product_consumption_schedules(sf, product_ids)

        subscription_names = [item.get("subscriptionName") for item in sub_consumption_schedules if item.get("subscriptionName")]
        consumption_schedule_map = self.fetch_consumption_schedules(sf, subscription_names)
        self.update_sub_consumption_schedules(sub_consumption_schedules, subext_to_pid, pcs_map, consumption_schedule_map)

        # --- SUB CONSUMPTION RATES ---
        sub_consumption_rates = self.get_all_records(data, "subConsumptionRate")
        subcs_ids = [item.get('subCsId') for item in sub_consumption_schedules if item.get('subCsId')]
        consumption_rate_map = self.fetch_consumption_rates(sf, subcs_ids)

        subext_to_subcsid = {item.get("subExternalId"): item.get("subCsId") for item in sub_consumption_schedules if item.get("subExternalId") and item.get("subCsId")}
        self.update_sub_consumption_rates(sub_consumption_rates, subext_to_subcsid, consumption_rate_map)

        # --- SUMMARY COUNTS ---
        subscriptions = self.get_all_records(data, "Subscription")
        lineitems = self.get_all_records(data, "LineItemSource")

        data["ExtractedSubCnt"] = len(subscriptions)
        data["ExtractedLisCnt"] = len(lineitems)
        data["MatchedSubCnt"] = sum(
            1 for item in subscriptions if (
                item.get("Note") == "Successfully Matched" or
                item.get("Note") == "Duplicate Subscription Names found, Id was taken from latest CreatedDate"
            )
        )
        data["MatchedLisCnt"] = sum(
            1 for item in lineitems if (
                item.get("Note") == "Successfully Matched" or
                item.get("Note") == "Duplicate Line Item Source Names found, Id was taken from latest CreatedDate"
            )
        )
        # --- ACCURACY RATE -----
        data["% Sub Matching Rate"] = f"{(data['MatchedSubCnt'] / data['ExtractedSubCnt'] * 100):.2f}%" if data['ExtractedSubCnt'] else ""
        data["% LIS Matching Rate"] = f"{(data['MatchedLisCnt'] / data['ExtractedLisCnt'] * 100):.2f}%" if data['ExtractedLisCnt'] else ""

        # --- 07/09: NA logic for confidence scores ---
        if data.get('ActualSubCnt', 0) == 0 and data.get('ExtractedSubCnt', 0) == 0:
            data["% Sub Extraction Confidence Score"] = "NA"
        elif data.get('ActualSubCnt') or data.get('ExtractedSubCnt'):
            data["% Sub Extraction Confidence Score"] = f"{(1 - abs(data['ExtractedSubCnt'] - data['ActualSubCnt']) / max(data['ExtractedSubCnt'], data['ActualSubCnt'], 1)) * 100:.2f}%"
        else:
            data["% Sub Extraction Confidence Score"] = ""

        if data.get('ActualLisCnt', 0) == 0 and data.get('ExtractedLisCnt', 0) == 0:
            data["% LIS Extraction Confidence Score"] = "NA"
        elif data.get('ActualLisCnt'):
            data["% LIS Extraction Confidence Score"] = f"{(1 - abs(data['ExtractedLisCnt'] - data['ActualLisCnt']) / max(data['ActualLisCnt'], 1)) * 100:.2f}%"
        else:
            data["% LIS Extraction Confidence Score"] = "NA"
        print(f"conf score: {data['ActualSubCnt']}")
        return data
