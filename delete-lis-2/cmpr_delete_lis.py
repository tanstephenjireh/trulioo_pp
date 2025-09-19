"""
cmpr_delete_lis.py

This script deletes all records related to a specific LineItemSource (by lisName, filename, and subscription name) from an output records JSON, appends a note to the relevant log entry, and updates a comparison records JSON to remove the relevant LineItemSources.

Inputs:
- output_records JSON file from Results tab
- comparison_records JSON file from cmpr_main.py
- filename: the contract PDF filename to match in Logs and in comparison_records
- data: the lisName (LineItemSource name) to delete and to remove from comparison_records
- subscription_name: the SubscriptionName to match in the relevant subscription in comparison_records and LineItemSource

Process:
1. Finds the Contractid in Logs by matching the filename.
2. Finds all LineItemSource records where lisName == data, lisExternalId endswith Contractid, and subscriptionName == input subscription_name.
3. For each such lisExternalId, deletes all records with that lisExternalId from LineItemSource, lisConsumptionSchedule, and lisConsumptionRate.
4. Appends a note to the relevant log entry in Logs indicating the deletion of the LineItemSource.
5. In comparison_records, finds the account with the matching filename, and for each subscription where SubscriptionDetails contains a dict with Field == 'SubscriptionName' and AI-Extracted == subscription_name, removes any LineItemSources dict where Field == 'LineItemSourceName' and AI-Extracted == data. If LineItemSources becomes empty, the key is removed from the subscription.
6. The main() function takes loaded JSON objects and returns the updated results. All file I/O is handled in the __main__ block.

Outputs:
- Writes two new JSON files with _deleted.json appended to the original filenames:
    - <output_records_path>_deleted.json: updated output records
    - <comparison_records_path>_deleted.json: updated comparison records
"""

import json

def get_contractid_by_filename(output_records, filename):
    for log in output_records.get("Logs", []):
        if log.get("FileName") == filename:
            return log.get("Contractid")
    return None

def delete_lis_records(output_records, contractid, data, filename, subscription_name):
    # Find all lisExternalId to delete
    lis_ids_to_delete = [
        lis.get("lisExternalId")
        for lis in output_records.get("LineItemSource", [])
        if lis.get("lisName") == data
           and str(lis.get("lisExternalId", "")).endswith(contractid)
           and lis.get("subscriptionName") == subscription_name
    ]
    # Remove from LineItemSource
    output_records["LineItemSource"] = [
        lis for lis in output_records.get("LineItemSource", [])
        if lis.get("lisExternalId") not in lis_ids_to_delete
    ]
    # Remove from lisConsumptionSchedule
    if "lisConsumptionSchedule" in output_records:
        output_records["lisConsumptionSchedule"] = [
            rec for rec in output_records["lisConsumptionSchedule"]
            if rec.get("lisExternalId") not in lis_ids_to_delete
        ]
    # Remove from lisConsumptionRate
    if "lisConsumptionRate" in output_records:
        output_records["lisConsumptionRate"] = [
            rec for rec in output_records["lisConsumptionRate"]
            if rec.get("lisExternalId") not in lis_ids_to_delete
        ]
    # Add note to Logs
    for log in output_records.get("Logs", []):
        if log.get("FileName") == filename:
            note = f"Deleted LineItemSource {data} through Comparison Hub"
            note_str = str(log.get("Note", ""))
            if note not in note_str:
                log["Note"] = f"{note_str}; {note}" if note_str else note
            break
    return output_records

def delete_lis_in_comparison(comparison_records, filename, data, subscription_name):
    """
    For the account in comparison_records with the given filename, for each subscription where SubscriptionDetails contains
    a dict with Field == 'SubscriptionName' and AI-Extracted == subscription_name, remove any dict in LineItemSources
    where Field == 'LineItemSourceName' and AI-Extracted == data. If LineItemSources becomes empty, remove the key.
    If no such subscription is found, and subscription_name ends with ' Transactions', try again with the suffix removed.
    """
    def process(comparison_records, filename, data, subscription_name):
        updated = False
        for account, acc_data in comparison_records.items():
            if isinstance(acc_data, dict) and acc_data.get("filename") == filename:
                for sub in acc_data.get("subscriptions", []):
                    details = sub.get("SubscriptionDetails", [])
                    if any(d.get("Field") == "SubscriptionName" and d.get("AI-Extracted") == subscription_name for d in details):
                        if "LineItemSources" in sub:
                            new_lis = [
                                lis for lis in sub["LineItemSources"]
                                if not (lis.get("Field") == "LineItemSourceName" and lis.get("AI-Extracted") == data)
                            ]
                            if new_lis:
                                sub["LineItemSources"] = new_lis
                            else:
                                del sub["LineItemSources"]
                            updated = True
        return updated

    updated = process(comparison_records, filename, data, subscription_name)
    # Fallback: try again with ' Transactions' removed if not found
    if not updated and subscription_name.endswith(" Transactions"):
        fallback_name = subscription_name[:-13]  # Remove ' Transactions'
        process(comparison_records, filename, data, fallback_name)
    return comparison_records

def main(output_records_json, comparison_records_json, filename, data, subscription_name):
    contractid = get_contractid_by_filename(output_records_json, filename)
    if not contractid:
        print(f"No contractid found for filename: {filename}")
        return output_records_json, comparison_records_json
    new_output_records = delete_lis_records(output_records_json, contractid, data, filename, subscription_name)
    new_comparison_records = delete_lis_in_comparison(comparison_records_json, filename, data, subscription_name)
    return new_output_records, new_comparison_records

# if __name__ == "__main__":
#     # ---- HARDCODE YOUR INPUTS HERE ----
#     output_records_path = "BACKEND/THIRDV/amendments/extracted_data_updated.json"  # Path to your output records JSON
#     comparison_records_path = "BACKEND/THIRDV/COMPARISON/compr_main_output_deleted_lis.json"  # Path to your comparison records JSON
#     filename = "AmWager Trulioo_DealRoom-Trulioo-17128-26-Feb-2025-19-52-26-signed.pdf"  # Example filename
#     data = "Enhanced Credit 1 SuperS"  # Example lisName
#     subscription_name = "United States Transactions"  # Example SubscriptionName
#     # -----------------------------------
#     with open(output_records_path, "r") as f:
#         output_records_json = json.load(f)
#     with open(comparison_records_path, "r") as f:
#         comparison_records_json = json.load(f)
#     new_output_records, new_comparison_records = main(output_records_json, comparison_records_json, filename, data, subscription_name)
#     output_path = output_records_path.replace(".json", "_deleted.json")
#     comparison_output_path = comparison_records_path.replace(".json", "_deleted_lis.json")
#     with open(output_path, "w") as f:
#         json.dump(new_output_records, f, indent=2)
#     with open(comparison_output_path, "w") as f:
#         json.dump(new_comparison_records, f, indent=2)
#     print(f"Updated output written to {output_path}")
#     print(f"Updated comparison written to {comparison_output_path}")