from simple_salesforce import Salesforce
from config import get_ssm_param
import logging

# Configure logging
logger = logging.getLogger(__name__)

class SalesForce:

    def __init__(self):
        # ========== ENV SETUP ==========
        self.username = get_ssm_param("/myapp/sf_username")
        self.password = get_ssm_param("/myapp/sf_password")
        self.security_token = get_ssm_param("/myapp/sf_security_token")
        self.domain = get_ssm_param("/myapp/sf_domain")

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
        # data = load_json(input_json_path)
        sf = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            domain=self.domain
        )

        # --- SUBSCRIPTIONS ---
        subscriptions = self.get_all_records(data, "Subscription")
        product_names = self.get_unique_product_names(subscriptions)
        product_field_map = self.fetch_product2_fields(sf, product_names)
        subext_to_pid = self.update_subscription_product_fields(subscriptions, product_field_map)

        # --- LINE ITEM SOURCES ---
        lineitems = self.get_all_records(data, "LineItemSource")
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
        data["% Sub Extraction Rate"] = f"{(data['ExtractedSubCnt'] / data['ActualSubCnt'] * 100):.2f}%" if data.get('ActualSubCnt') else ""
        data["% LIS Extraction Rate"] = f"{(data['ExtractedLisCnt'] / data['ActualLisCnt'] * 100):.2f}%" if data.get('ActualLisCnt') else ""

        return data