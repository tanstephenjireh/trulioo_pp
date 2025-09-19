"""
cmpr_delete_sub.py

This script deletes all records related to a specific subscription (by ProductName) from a contract output records JSON, appends a note to the relevant log entry, and updates a comparison records JSON to remove the relevant SubscriptionDetails.

Inputs:
- output_records JSON file from Results tab
- comparison_records JSON file from cmpr_main.py
- filename: the contract PDF filename to match in Logs and in comparison_records
- data: the ProductName (subscription) to delete and to remove from comparison_records

Process:
1. Finds the Contractid in Logs by matching the filename.
2. Finds the subExternalId and DiscExtId in Subscription by matching Contractid and ProductName (data).
3. Deletes all records in Subscription, LineItemSource, subConsumptionSchedule, subConsumptionRate, lisConsumptionSchedule, lisConsumptionRate, discountSchedule, and discountRate that match the subExternalId or DiscExtId.
4. Appends a note to the relevant log entry in Logs indicating the deletion.
5. In comparison_records, finds the account with the matching filename, and for each subscription, removes any SubscriptionDetails dict where Field == 'SubscriptionName' and AI-Extracted == data. If SubscriptionDetails becomes empty, the key is removed from the subscription.
6. The main() function takes loaded JSON objects and returns the updated results. All file I/O is handled in the __main__ block.

Outputs:
- Writes two new JSON files with _deleted.json appended to the original filenames:
    - <output_records_path>_deleted.json: updated output records
    - <comparison_records_path>_deleted.json: updated comparison records

"""

import json

class OutputRecordsEditor:
    def __init__(self, output_records_json):
        self.output_records = output_records_json

    def get_contractid_by_filename(self, filename):
        for log in self.output_records.get("Logs", []):
            if log.get("FileName") == filename:
                return log.get("Contractid")
        return None

    def get_sub_and_disc_extid_by_contractid_and_data(self, contractid, data):
        # First, try exact match
        for sub in self.output_records.get("Subscription", []):
            if sub.get("ContractExternalId") == contractid and sub.get("ProductName") == data:
                return sub.get("subExternalId"), sub.get("DiscExtId"), sub.get("ProductName")
        # Fallback: if data ends with ' Transactions', try removing it
        if data.endswith(" Transactions"):
            fallback_data = data[:-13].rstrip()
            for sub in self.output_records.get("Subscription", []):
                if sub.get("ContractExternalId") == contractid and sub.get("ProductName") == fallback_data:
                    return sub.get("subExternalId"), sub.get("DiscExtId"), sub.get("ProductName")
        return None, None, None

    def delete_records(self, subExternalId, DiscExtId):
        self.output_records["Subscription"] = [s for s in self.output_records.get("Subscription", []) if s.get("subExternalId") != subExternalId]
        self.output_records["LineItemSource"] = [l for l in self.output_records.get("LineItemSource", []) if l.get("subExternalId") != subExternalId]
        if "subConsumptionSchedule" in self.output_records:
            self.output_records["subConsumptionSchedule"] = [s for s in self.output_records["subConsumptionSchedule"] if s.get("subExternalId") != subExternalId]
        if "subConsumptionRate" in self.output_records:
            self.output_records["subConsumptionRate"] = [s for s in self.output_records["subConsumptionRate"] if s.get("subExternalId") != subExternalId]
        if "lisConsumptionSchedule" in self.output_records:
            self.output_records["lisConsumptionSchedule"] = [s for s in self.output_records["lisConsumptionSchedule"] if s.get("subExternalId") != subExternalId]
        if "lisConsumptionRate" in self.output_records:
            self.output_records["lisConsumptionRate"] = [s for s in self.output_records["lisConsumptionRate"] if s.get("subExternalId") != subExternalId]
        self.output_records["discountSchedule"] = [d for d in self.output_records.get("discountSchedule", []) if d.get("DiscExtId") != DiscExtId]
        self.output_records["discountRate"] = [d for d in self.output_records.get("discountRate", []) if d.get("DiscExtId") != DiscExtId]

    def add_note_to_logs(self, filename, subscription_name):
        for log in self.output_records.get("Logs", []):
            if log.get("FileName") == filename:
                note = f"Deleted {subscription_name} through Comparison Hub"
                note_str = str(log.get("Note", ""))
                if note not in note_str:
                    log["Note"] = f"{note_str}; {note}" if note_str else note
                break

    def process(self, filename, data):
        contractid = self.get_contractid_by_filename(filename)
        if not contractid:
            print(f"No contractid found for filename: {filename}")
            return self.output_records
        subExternalId, DiscExtId, subscription_name = self.get_sub_and_disc_extid_by_contractid_and_data(contractid, data)
        if not subExternalId and not DiscExtId:
            print(f"No subscription found for contractid: {contractid} and data: {data}")
            return self.output_records
        self.delete_records(subExternalId, DiscExtId)
        self.add_note_to_logs(filename, subscription_name or data)
        return self.output_records


def set_ai_extracted_not_found_in_comparison(comparison_records, filename, data, field="SubscriptionName"):
    """
    For the account in comparison_records with the given filename, remove the entire subscription
    where SubscriptionDetails contains Field == field and AI-Extracted == data.
    Also tries fallback if data ends with ' Transactions'.
    """
    for account, acc_data in comparison_records.items():
        if isinstance(acc_data, dict) and acc_data.get("filename") == filename:
            subscriptions_to_keep = []
            for sub in acc_data.get("subscriptions", []):
                should_remove = False
                if "SubscriptionDetails" in sub:
                    # Try exact match first
                    found = any(detail.get("Field") == field and detail.get("AI-Extracted") == data for detail in sub["SubscriptionDetails"])
                    if found:
                        should_remove = True
                    elif data.endswith(" Transactions"):
                        fallback_data = data[:-13].rstrip()
                        found_fallback = any(detail.get("Field") == field and detail.get("AI-Extracted") == fallback_data for detail in sub["SubscriptionDetails"])
                        if found_fallback:
                            should_remove = True
                
                if not should_remove:
                    subscriptions_to_keep.append(sub)
            
            acc_data["subscriptions"] = subscriptions_to_keep
    return comparison_records

def main(output_records_json, comparison_records_json, filename, data):
    """
    Main function to process both output_records and comparison_records, given loaded JSON objects.
    Returns the updated output_records and comparison_records.
    """
    # Process output records
    editor = OutputRecordsEditor(output_records_json)
    new_output_records = editor.process(filename, data)
    # Process comparison records
    new_comparison_records = set_ai_extracted_not_found_in_comparison(comparison_records_json, filename, data)
    return new_output_records, new_comparison_records

# if __name__ == "__main__":
#     # ---- HARDCODE YOUR INPUTS HERE ----
#     output_path = 'new_subscription_output_dev.json' # Define output path 
#     output_records_path = "BACKEND/THIRDV/COMPARISON/dataframe_1757303936178.json"  # Path to your output records JSON
#     comparison_records_path = "BACKEND/THIRDV/COMPARISON/comparison_accounts_results_1757303936178.json"  # Path to your comparison records JSON
#     filename = "Constellation_Payments_-_KYB_PM_-_USA-Trulioo-16968-12-Feb-2025-19-7-13-signed.pdf"  # Example filename
#     data = "Canada Transactions"  # Example data (ProductName)
#     # -----------------------------------
#     with open(output_records_path, "r") as f:
#         output_records_json = json.load(f)
#     with open(comparison_records_path, "r") as f:
#         comparison_records_json = json.load(f)
#     new_output_records, new_comparison_records = main(output_records_json, comparison_records_json, filename, data)
#     output_path = output_records_path.replace(".json", "_deleted.json")
#     comparison_output_path = comparison_records_path.replace(".json", "_deleted.json")
#     with open(output_path, "w") as f:
#         json.dump(new_output_records, f, indent=2)
#     with open(comparison_output_path, "w") as f:
#         json.dump(new_comparison_records, f, indent=2)
#     print(f"Updated output written to {output_path}")
#     print(f"Updated comparison written to {comparison_output_path}")