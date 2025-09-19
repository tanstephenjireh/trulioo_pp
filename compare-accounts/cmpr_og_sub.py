"""
This script reads a JSON file with contract, subscription, and log data, and outputs a summary mapping each AccountName to its contract file and subscriptions.

Input:
- JSON with sections:
  - "Contract": list of dicts with "ContractExternalId" and "AccountName"
  - "Subscription": list of dicts with "ContractExternalId", "ProductName", "subExternalId", and "DiscExtId"
  - "Logs": list of dicts with "Contractid" and "FileName"
  - "LineItemSource": list of dicts with "subExternalId" and "lisName"
  - "discountRate": list of dicts with "DiscExtId", "InvoiceScheduleTierName", "LowerBound", "UpperBound", "Discount" or "Discount (%)"
  - "discountSchedule": list of dicts with "DiscExtId", "DiscountUnit", "Type"

Output:
- JSON of the form:
{
    "AccountName": {
        "filename": <FileName>,
        "subscriptions": [
            {
                "SubscriptionName": <ProductName>,
                "subExternalId": <subExternalId>,
                "LineItemSources": [
                    {"LisName": <LisName>}, ...
                ],
                "DiscExtId": <DiscExtId>,
                "Discount": {
                    "DiscountSchedule": {
                        "DiscountUnit": <DiscountUnit>,
                        "Type": <Type>
                    },
                    "DiscountRate": [
                        {
                            "Name": <TierName>,
                            "Lower Bound": <LowerBound>,
                            "Upper Bound": <UpperBound>,
                            "Discount": <Discount>
                        }, ...
                    ]
                }
            }, ...
        ]
    }, ...
}

How it works:
- Loads and parses the input JSON data.
- Maps contracts to accounts, subscriptions, and filenames.
- Attaches line item sources and discount details to each subscription.
- Outputs a structured summary for each account, suitable for downstream comparison and analysis.
"""
import json

class ContractSubscriptionExtractor:
    def __init__(self, data):
        """
        Initialize the extractor with the loaded JSON data object.
        """
        self.data = data
        self.logs = None
        self.contractid_to_filename = None
        self.contractid_to_products = None

    def extract_logs(self):
        """
        Find and extract the logs section from the data, mapping Contractid to FileName.
        Sets self.logs and self.contractid_to_filename.
        """
        for key, value in self.data.items():
            if (
                isinstance(value, list)
                and value
                and isinstance(value[0], dict)
                and 'Contractid' in value[0]
                and 'FileName' in value[0]
            ):
                self.logs = value
                break
        if self.logs is None:
            raise ValueError('Could not find logs section with Contractid and FileName')
        self.contractid_to_filename = {log["Contractid"]: log["FileName"] for log in self.logs}

    def extract_subscriptions(self):
        """
        Extract the subscriptions section, mapping ContractExternalId to a list of ProductNames and their subExternalId and DiscExtId.
        Sets self.contractid_to_products.
        """
        self.contractid_to_products = {}
        for sub in self.data.get("Subscription", []):
            cid = sub["ContractExternalId"]
            pname = sub["ProductName"]
            sub_external_id = sub.get("subExternalId")
            disc_extid = sub.get("DiscExtId")
            self.contractid_to_products.setdefault(cid, []).append({
                "SubscriptionName": pname,
                "subExternalId": sub_external_id,
                "DiscExtId": disc_extid
            })

    def get_contract_subscription_map(self):
        """
        Main method to generate the mapping from AccountName to filename and subscriptions.
        Returns:
            dict: {AccountName: {filename: str, subscriptions: list}}
        """
        self.extract_logs()
        self.extract_subscriptions()
        # Build ContractExternalId to AccountName mapping
        contractid_to_accountname = {}
        for contract in self.data.get("Contract", []):
            contractid_to_accountname[contract["ContractExternalId"]] = contract["AccountName"]
        # Build subExternalId to lisName dict-list mapping
        subextid_to_lisnames = {}
        for lis in self.data.get("LineItemSource", []):
            subextid = lis.get("subExternalId")
            lisname = lis.get("lisName")
            if subextid and lisname:
                subextid_to_lisnames.setdefault(subextid, []).append({"LisName": lisname})
        # Build DiscExtId to discountRate list mapping
        disc_extid_to_rates = {}
        for rate in self.data.get("discountRate", []):
            disc_extid = rate.get("DiscExtId")
            if disc_extid:
                disc_extid_to_rates.setdefault(disc_extid, []).append({
                    "Name": rate.get("InvoiceScheduleTierName"),
                    "Lower Bound": rate.get("LowerBound"),
                    "Upper Bound": rate.get("UpperBound"),
                    "Discount": rate.get("Discount") or rate.get("Discount (%)")
                })
        # Build DiscExtId to discountSchedule mapping
        disc_extid_to_schedule = {}
        for sched in self.data.get("discountSchedule", []):
            disc_extid = sched.get("DiscExtId")
            if disc_extid:
                disc_extid_to_schedule[disc_extid] = {
                    "DiscountUnit": sched.get("DiscountUnit"),
                    "Type": sched.get("Type")
                }
        output = {}
        for cid, subs in self.contractid_to_products.items():
            account_name = contractid_to_accountname.get(cid, cid)
            if account_name not in output:
                output[account_name] = {
                    "filenames": [],
                    "subscriptions": []
                }
            filename = self.contractid_to_filename.get(cid)
            if filename and filename not in output[account_name]["filenames"]:
                output[account_name]["filenames"].append(filename)
            # Attach line item source dicts and discount tiers to each subscription
            for sub in subs:
                sub_copy = dict(sub)  # shallow copy
                subextid = sub.get("subExternalId")
                sub_copy["LineItemSources"] = subextid_to_lisnames.get(subextid, [])
                sub_copy["DiscExtId"] = sub.get("DiscExtId")
                # Add DiscountSchedule and DiscountRate fields
                disc_extid = sub.get("DiscExtId")
                if disc_extid:
                    sub_copy["Discount"] = {
                        "DiscountSchedule": disc_extid_to_schedule.get(disc_extid, {}),
                        "DiscountRate": disc_extid_to_rates.get(disc_extid, [])
                    }
                else:
                    sub_copy["Discount"] = {
                        "DiscountSchedule": {},
                        "DiscountRate": []
                    }
                output[account_name]["subscriptions"].append(sub_copy)
        # Optionally, if only one filename per account, flatten the list
        for account_name in output:
            if len(output[account_name]["filenames"]) == 1:
                output[account_name]["filename"] = output[account_name]["filenames"][0]
            del output[account_name]["filenames"]
        return output

def main(json_text):
    """
    Main entry point. Accepts a JSON string (not a file path).
    """
    data = json.loads(json_text)
    extractor = ContractSubscriptionExtractor(data)
    result = extractor.get_contract_subscription_map()
    # print(json.dumps(result, indent=2))  # Commented out to suppress output JSON in terminal
    return result

# if __name__ == "__main__":
#     INPUT_JSON_PATH = 'BACKEND/THIRDV/amendments/extracted_data_updated.json'
#     with open(INPUT_JSON_PATH, 'r') as f:
#         json_text = f.read()
#     result = main(json_text)
#     output_path = 'cmpr_og_sub_output.json'
#     with open(output_path, 'w') as f:
#         json.dump(result, f, indent=2)