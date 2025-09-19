"""
This script compares AI-extracted and Salesforce subscription data for each account.

Input:
- Two JSON strings/files: one from the AI-extracted pipeline (OG), one from Salesforce.
- Each input is a mapping of account names to their subscription details.

Output:
- A JSON object mapping each account to a list of subscription comparisons.
- For each subscription:
    - Compares SubscriptionName. If the only difference between AI-Extracted and Salesforce is the 'Transactions' suffix, the AI-Extracted value is set to the Salesforce value (with 'Transactions').
    - Compares LineItemSources (list of LisName). For each unique LisName, shows the name if present or 'Not found' if missing in either source.
    - Compares Discount fields:
        - DiscountSchedule: Compares DiscountUnit and Type fields.
        - DiscountRate: For each distinct tier name (e.g., "Tier 1"), compares Lower Bound, Upper Bound, and Discount fields. Values are formatted as integers if whole, or minimal decimals otherwise.
- Only accounts present in the original input file are included in the output.
- Subscription names are normalized (case, whitespace, and 'transactions' suffix ignored) for comparison.


How it works:
- Loads and parses both AI-extracted and Salesforce JSON data.
- Normalizes and compares subscription names, line item sources, and discount details for each account.
- Outputs a structured comparison for each field
"""
import json
import re
from cmpr_salesforce import main as sf_main
from cmpr_og_sub import main as og_main

class ComparisonPipeline:
    """
    Pipeline to compare AI-extracted and Salesforce subscription data for each account.
    """
    def __init__(self, sf_json_text, og_json_text):
        """
        Initialize with Salesforce and OG (AI-extracted) JSON strings.
        """
        self.sf_json_text = sf_json_text
        self.og_json_text = og_json_text
        self.sf_out = None
        self.og_out = None
        self.result = None

    def normalize_sub_name(self, name):
        """
        Normalize subscription name: lowercased, stripped, and removes 'transactions' suffix.
        """
        if not isinstance(name, str):
            return name
        name = name.strip().lower()
        name = re.sub(r'\s*transactions\s*$', '', name, flags=re.IGNORECASE)
        return name.strip()

    def run_extractors(self):
        """
        Run the Salesforce and OG extractors to get their outputs as dicts.
        Also store the set of input accounts from the Salesforce input JSON.
        """
        self.sf_out = sf_main(self.sf_json_text)
        self.og_out = og_main(self.og_json_text)
        self.input_accounts = set(json.loads(self.sf_json_text).keys())

    def compare_subscriptions(self):
        """
        Compare subscriptions for each account, normalizing names and formatting output.
        Now also compares LineItemSources (list of LisName) and Discount fields for each subscription.
        Only includes accounts present in the original input file.
        """
        output = {}
        all_accounts = self.input_accounts
        for account in all_accounts:
            og_entry = self.og_out.get(account, {})
            sf_entry = self.sf_out.get(account, {})
            og_subs = og_entry.get('subscriptions', [])
            sf_subs = sf_entry.get('subscriptions', [])
            og_norm_map = {self.normalize_sub_name(s['SubscriptionName']): s for s in og_subs}
            sf_norm_map = {self.normalize_sub_name(s['SubscriptionName']): s for s in sf_subs}
            all_norm_names = sorted(set(og_norm_map) | set(sf_norm_map))
            subscriptions = []
            for norm in all_norm_names:
                og_sub = og_norm_map.get(norm)
                sf_sub = sf_norm_map.get(norm)
                ai_val = og_sub['SubscriptionName'] if og_sub else "Not found"
                sf_val = sf_sub['SubscriptionName'] if sf_sub else "Not found"
                og_lis = [d['LisName'] for d in og_sub.get('LineItemSources', [])] if og_sub else []
                sf_lis = [d['LisName'] for d in sf_sub.get('LineItemSources', [])] if sf_sub else []
                # Compare all unique LisNames from both sources
                all_lisnames = sorted(set(og_lis) | set(sf_lis))
                lineitemsources_comparisons = []
                for lisname in all_lisnames:
                    ai_val_lis = lisname if lisname in og_lis else "Not found"
                    sf_val_lis = lisname if lisname in sf_lis else "Not found"
                    lineitemsources_comparisons.append({
                        "Field": "LineItemSourceName",
                        "AI-Extracted": ai_val_lis,
                        "Salesforce data": sf_val_lis
                    })
                subscription_details = [
                    {
                        "Field": "SubscriptionName",
                        "AI-Extracted": ai_val,
                        "Salesforce data": sf_val
                    }
                ]
                # --- Discount comparison ---
                og_discount = og_sub.get('Discount', {}) if og_sub else {}
                sf_discount = sf_sub.get('Discount', {}) if sf_sub else {}
                # DiscountSchedule comparison
                dsched = []
                for field in ["DiscountUnit", "Type"]:
                    dsched.append({
                        "Field": field,
                        "AI-Extracted": og_discount.get("DiscountSchedule", {}).get(field, "Not found"),
                        "Salesforce data": sf_discount.get("DiscountSchedule", {}).get(field, "Not found")
                    })
                # DiscountRate comparison
                og_rates = og_discount.get("DiscountRate", [])
                sf_rates = sf_discount.get("DiscountRate", [])
                # Map by Name for easy lookup
                og_rate_map = {r.get("Name"): r for r in og_rates if r.get("Name")}
                sf_rate_map = {r.get("Name"): r for r in sf_rates if r.get("Name")}
                all_rate_names = sorted(set(og_rate_map) | set(sf_rate_map))
                drate = {}
                for name in all_rate_names:
                    og_r = og_rate_map.get(name, {})
                    sf_r = sf_rate_map.get(name, {})
                    drate[name] = [
                        {
                            "Field": "Lower Bound",
                            "AI-Extracted": (lambda v: (str(int(float(v))) if str(v).replace('.','',1).isdigit() and float(v)%1==0 else str(float(v)).rstrip('0').rstrip('.') if str(v).replace('.','',1).isdigit() else v) if v not in [None, "Not found"] else v)(og_r.get("Lower Bound", "Not found")),
                            "Salesforce data": (lambda v: (str(int(float(v))) if str(v).replace('.','',1).isdigit() and float(v)%1==0 else str(float(v)).rstrip('0').rstrip('.') if str(v).replace('.','',1).isdigit() else v) if v not in [None, "Not found"] else v)(sf_r.get("Lower Bound", "Not found"))
                        },
                        {
                            "Field": "Upper Bound",
                            "AI-Extracted": (lambda v: (str(int(float(v))) if str(v).replace('.','',1).isdigit() and float(v)%1==0 else str(float(v)).rstrip('0').rstrip('.') if str(v).replace('.','',1).isdigit() else v) if v not in [None, "Not found"] else v)(og_r.get("Upper Bound", "Not found")),
                            "Salesforce data": (lambda v: (str(int(float(v))) if str(v).replace('.','',1).isdigit() and float(v)%1==0 else str(float(v)).rstrip('0').rstrip('.') if str(v).replace('.','',1).isdigit() else v) if v not in [None, "Not found"] else v)(sf_r.get("Upper Bound", "Not found"))
                        },
                        {
                            "Field": "Discount",
                            "AI-Extracted": (lambda v: (str(int(float(v))) if str(v).replace('.','',1).isdigit() and float(v)%1==0 else str(float(v)).rstrip('0').rstrip('.') if str(v).replace('.','',1).isdigit() else v) if v not in [None, "Not found"] else v)(og_r.get("Discount", "Not found")),
                            "Salesforce data": (lambda v: (str(int(float(v))) if str(v).replace('.','',1).isdigit() and float(v)%1==0 else str(float(v)).rstrip('0').rstrip('.') if str(v).replace('.','',1).isdigit() else v) if v not in [None, "Not found"] else v)(sf_r.get("Discount", "Not found"))
                        }
                    ]
                subscriptions.append({
                    "SubscriptionDetails": subscription_details,
                    "SubscriptionConsumptionSchedule": [],
                    "SubscriptionRates": [],
                    "LineItemSources": lineitemsources_comparisons,
                    "discounts": [
                        {
                            "DiscountSchedule": dsched,
                            "DiscountRate": drate
                        }
                    ]
                })
            filename = og_entry.get('filename')
            output[account] = {
                "filename": filename if filename is not None else "",
                "contract": [],
                "subscriptions": subscriptions
            }
        self.result = output
        return output

    def run(self):
        """
        Run the full pipeline: extract, compare, and return the result.
        """
        self.run_extractors()
        return self.compare_subscriptions()

def main(sf_json_text, og_json_text):
    """
    Run the comparison pipeline and print/save the result.
    """
    pipeline = ComparisonPipeline(sf_json_text, og_json_text)
    result = pipeline.run()
    print(json.dumps(result, indent=2))
    return result

# if __name__ == "__main__":
#     load_dotenv()
#     sf_input_path = 'BACKEND/THIRDV/COMPARISON/dummy_input.json'
#     og_input_path = 'BACKEND/THIRDV/amendments/extracted_data_updated.json'
#     with open(sf_input_path, 'r') as f:
#         sf_json_text = f.read()
#     with open(og_input_path, 'r') as f:
#         og_json_text = f.read()
#     result = main(sf_json_text, og_json_text)
#     # Save output as JSON file
#     output_path = 'BACKEND/THIRDV/COMPARISON/compr_main_output.json'
#     with open(output_path, 'w') as f:
#         json.dump(result, f, indent=2)