#!/usr/bin/env python3
"""
Amendment Overwrite Script (Optimized Version)

PURPOSE:
This script handles overwriting or appending amendment data to the original JSON based on
matching logic and selective field updates.

INPUT REQUIREMENTS:
- original_json: The original JSON data containing existing contract information
- new_data: Pipeline output JSON containing amendment data with Contractid field

LOGIC FLOW:
1. Extract Contractid from new_data automatically
2. Contract-level changes: Selective overwriting (skip NA/empty values, protect AccountName)
3. Subscription matching: ContractExternalId + ProductName
   - If MATCHED: Selectively overwrite existing subscription fields
   - If NOT MATCHED: Append new subscription
4. Related data overwriting: subConsumptionSchedule, subConsumptionRate, LineItemSource, etc.
   - External ID normalization (strip "amd_" prefix)
   - Selective field updates (skip NA/empty values)
5. subExternalId mapping: When subscriptions get amended with new subExternalId (format: amd_sub_{i}_xxx)
   - Track mapping from old subExternalId to new subExternalId
   - Update all related records in other tables to use new subExternalId
   - Handle both amendment data and existing records in original JSON
6. Deduplication: Remove duplicate records in consumption schedule tables
   - subConsumptionSchedule: Deduplicate by (subExternalId, subCsName) pairs
   - lisConsumptionSchedule: Deduplicate by (lisExternalId, scsName) pairs
   - subConsumptionRate: Deduplicate by (subExternalId, subCrName) pairs
   - lisConsumptionRate: Deduplicate by (subExternalId, scrName) pairs
   - Retain records with "amd_" prefixed external IDs, otherwise retain first

OUTPUT:
- Updated JSON with amendment changes applied
- XLSX file with all tables (in if__name__==__main)
- Comprehensive amendment note in Amendment Logs

USAGE:
- As module: updated_json = main(original_json, new_data)
- Standalone: python amd_overwrite_optimized.py (uses hardcoded paths)

"""

from typing import Dict, List, Optional


class AmendmentOverwriter:
    def __init__(self):
        """Initialize the Amendment Overwriter with optimized configurations."""
        self.related_tables = [
            "subConsumptionSchedule", "subConsumptionRate", "LineItemSource",
            "lisConsumptionSchedule", "lisConsumptionRate", "discountSchedule"
        ]
        
        # External ID field mapping to eliminate redundancy
        self.external_id_fields = {
            "subConsumptionSchedule": "subCsExternalId",
            "subConsumptionRate": "subCrExternalId",
            "lisConsumptionSchedule": "scsExternalId",
            "lisConsumptionRate": "scrExternalId",
            "LineItemSource": "lisExternalId",
            "discountSchedule": "DiscExtId"
        }
        
        # Name field mapping for better matching
        self.name_fields = {
            "LineItemSource": "lisName",
            "subConsumptionSchedule": ["scheduleName", "name"],
            "lisConsumptionSchedule": ["scheduleName", "name"],
            "subConsumptionRate": ["rateName", "name"],
            "lisConsumptionRate": ["rateName", "name"]
        }
        
        self._reset_amendment_summary()

    def _reset_amendment_summary(self):
        """Reset amendment summary for new processing run."""
        self.amendment_summary = {
            "contract_changes": [],
            "subscription_changes": {"added": [], "updated": []},
            "lineitem_changes": {"added": [], "updated": []},
            "discount_schedule_changes": {"added": [], "updated": []}
        }
        # Track subExternalId mappings for amendment processing
        self.sub_external_id_mappings = {}  # old_id -> new_id

    def _is_na_or_empty(self, value) -> bool:
        """Check if a value should be considered as NA or empty."""
        if value is None:
            return True
        if not isinstance(value, str):
            return False
        cleaned_value = value.strip().lower()
        na_formats = ["na", "n/a", "n\\a", "not applicable", "not available"]
        return cleaned_value in na_formats or cleaned_value == ""

    def _selectively_update_fields(self, existing_record: Dict, new_record: Dict, 
                                 record_type: str, skip_fields: List[str] = None) -> List[str]:
        """
        Selectively update fields from new record to existing record.
        
        LOGIC:
        1. Skip fields in skip_fields list (e.g., AccountName)
        2. Skip fields with NA/empty values to preserve original data
        3. Only update fields that have different values
        4. Track all changes for amendment summary
        
        Args:
            existing_record: The record to be updated
            new_record: The record with new values
            record_type: Type of record (Contract, Subscription, etc.) for logging
            skip_fields: List of fields to never update
            
        Returns:
            List of changed fields in format "field: old_value → new_value"
        """
        if skip_fields is None:
            skip_fields = []
        
        changed_fields = []
        
        for field, new_value in new_record.items():
            # Skip protected fields (e.g., AccountName)
            if field in skip_fields:
                print(f"{field} field ignored - keeping original: '{existing_record.get(field, '')}'")
                continue
            
            # Skip NA/empty values to preserve original data
            if self._is_na_or_empty(new_value):
                print(f"Skipping {record_type} field '{field}' - value is '{new_value}' (NA/empty)")
                continue
            
            current_value = existing_record.get(field, "")
            
            # Only update if values are different
            if new_value != current_value:
                print(f"{record_type} field '{field}' changed:")
                print(f"  Original: '{current_value}'")
                print(f"  New: '{new_value}'")
                existing_record[field] = new_value
                changed_fields.append(f"{field}: {current_value} → {new_value}")
            else:
                print(f"{record_type} field '{field}' unchanged: '{current_value}'")
        
        return changed_fields

    def _get_external_id(self, record: Dict, table_name: str) -> Optional[str]:
        """Get external ID from record based on table name."""
        if table_name in self.external_id_fields:
            return record.get(self.external_id_fields[table_name])
        return record.get("subExternalId")

    def _clean_external_id(self, external_id: str) -> str:
        """Strip 'amd_' prefix from external ID for matching."""
        return external_id.replace("amd_", "", 1) if external_id and external_id.startswith("amd_") else external_id

    def _find_best_match(self, candidates: List[tuple], new_record: Dict, table_name: str) -> Optional[int]:
        """Find best match from candidates using name fields."""
        if not candidates or table_name not in self.name_fields:
            return candidates[0][0] if candidates else None
        
        name_fields = self.name_fields[table_name]
        if isinstance(name_fields, str):
            name_fields = [name_fields]
        
        # Try to match on name fields
        for name_field in name_fields:
            new_name = new_record.get(name_field)
            if new_name:
                for i, record in candidates:
                    if record.get(name_field) == new_name:
                        print(f"  Returning {table_name} match by {name_field} at index {i}")
                        return i
        
        # Return first candidate if no name match found
        return candidates[0][0] if candidates else None

    def find_existing_subscription(self, original_json: Dict, contract_external_id: str, subscription_name: str) -> Optional[Dict]:
        """Find existing subscription in original JSON."""
        if "Subscription" not in original_json:
            return None
            
        for subscription in original_json["Subscription"]:
            if (subscription.get("ContractExternalId") == contract_external_id and 
                subscription.get("ProductName") == subscription_name):
                return subscription
                
        return None

    def find_related_records(self, original_json: Dict, sub_external_id: str, table_name: str) -> List[int]:
        """Find indices of related records in a table based on subExternalId."""
        if table_name not in original_json:
            return []
            
        matching_indices = []
        for i, record in enumerate(original_json[table_name]):
            if record.get("subExternalId") == sub_external_id:
                matching_indices.append(i)
                
        return matching_indices

    def find_specific_record_match(self, original_json: Dict, new_record: Dict, table_name: str) -> Optional[int]:
        """
        Find a specific record match based on external ID.
        
        LOGIC:
        1. Get external ID from new record using table-specific field mapping
        2. Clean external ID by stripping "amd_" prefix for matching
        3. Find all candidates that match either original or cleaned ID
        4. Use name fields to find best match if multiple candidates exist
        5. Return index of best match or None if no match found
        
        Args:
            original_json: The original JSON data
            new_record: The new record to match
            table_name: Name of the table to search
            
        Returns:
            Index of matching record if found, None otherwise
        """
        if table_name not in original_json:
            return None
            
        # Get external ID using table-specific field mapping
        new_external_id = self._get_external_id(new_record, table_name)
        if not new_external_id:
            return None
            
        # Clean external ID by stripping "amd_" prefix
        new_external_id_clean = self._clean_external_id(new_external_id)
        print(f"Looking for match in {table_name} with external ID: {new_external_id} (cleaned: {new_external_id_clean})")
        
        # Find all candidates that match either original or cleaned ID
        candidates = []
        for i, record in enumerate(original_json[table_name]):
            record_external_id = self._get_external_id(record, table_name)
            if record_external_id:
                record_external_id_clean = self._clean_external_id(record_external_id)
                
                # Match if either original IDs match or cleaned IDs match
                if (record_external_id == new_external_id or 
                    record_external_id_clean == new_external_id_clean):
                    candidates.append((i, record))
                    print(f"  Found candidate at index {i}: {record_external_id}")
        
        if not candidates:
            print(f"  No candidates found for {new_external_id}")
            return None
            
        # Find best match using name fields if multiple candidates exist
        return self._find_best_match(candidates, new_record, table_name)

    def update_disc_ext_ids(self, original_json: Dict, new_data: Dict):
        """
        Update DiscExtId values across discountSchedule and Subscription tables.
        
        DISCOUNT SCHEDULE & RATE LOGIC:
        
        PURPOSE:
        - Handle "amd_" prefixed DiscExtId values in amendment data
        - Ensure consistency between discountSchedule and Subscription tables
        - Update existing records to use the new "amd_" prefixed DiscExtId
        
        Args:
            original_json: The original JSON data to be updated
            new_data: Pipeline output containing amendment data
        """
        print("Processing DiscExtId updates...")
        
        # STEP 1: Find amendment records with "amd_" prefixed DiscExtId
        amendment_disc_ext_ids = []
        for record in new_data.get("output_records", []):
            if record.get("name") == "discountSchedule":
                for disc_record in record.get("data", []):
                    disc_ext_id = disc_record.get("DiscExtId")
                    if disc_ext_id and disc_ext_id.startswith("amd_"):
                        # Extract original ID by removing "amd_" prefix
                        original_disc_ext_id = disc_ext_id.replace("amd_", "", 1)
                        amendment_disc_ext_ids.append({
                            "original": original_disc_ext_id,
                            "amended": disc_ext_id
                        })
                        print(f"Found amendment DiscExtId: {original_disc_ext_id} → {disc_ext_id}")
        
        if not amendment_disc_ext_ids:
            print("No amendment DiscExtId updates found")
            return
        
        # STEP 2: Update records in both discountSchedule and Subscription tables
        for table_name in ["discountSchedule", "Subscription"]:
            if table_name in original_json:
                updated_count = 0
                for record in original_json[table_name]:
                    current_disc_ext_id = record.get("DiscExtId")
                    if current_disc_ext_id:
                        # Find matching update mapping
                        for update in amendment_disc_ext_ids:
                            if current_disc_ext_id == update["original"]:
                                # Update to new "amd_" prefixed DiscExtId
                                record["DiscExtId"] = update["amended"]
                                updated_count += 1
                                print(f"Updated {table_name} DiscExtId: {update['original']} → {update['amended']}")
                                break
                
                if updated_count > 0:
                    print(f"✅ Updated {updated_count} {table_name} DiscExtId records")

    def _process_contract_changes(self, original_json: Dict, new_data: Dict, contract_external_id: str):
        """Process Contract-level changes by selectively overwriting fields."""
        print(f"Processing Contract-level changes...")
        
        # Find Contract data in new_data
        new_contract_data = None
        for record in new_data.get("output_records", []):
            if record.get("name") == "Contract" and record.get("data"):
                new_contract_data = record["data"][0]
                break
        
        if not new_contract_data:
            print("No Contract data found in amendment")
            return
        
        # Find existing contract
        existing_contract = None
        if "Contract" in original_json:
            for contract in original_json["Contract"]:
                if contract.get("ContractExternalId") == contract_external_id:
                    existing_contract = contract
                    break
        
        if not existing_contract:
            print(f"No existing contract found with ContractExternalId: {contract_external_id}")
            return
        
        print(f"Selectively updating Contract fields for ContractExternalId: {contract_external_id}")
        
        # Selectively update fields (skip AccountName)
        changed_fields = self._selectively_update_fields(
            existing_contract, new_contract_data, "Contract", skip_fields=["AccountName"]
        )
        
        self.amendment_summary["contract_changes"].extend(changed_fields)
        
        if changed_fields:
            print(f"✅ Updated {len(changed_fields)} Contract fields")
        else:
            print("No Contract field changes detected")

    def _process_subscription_table(self, original_json: Dict, subscription_data: List[Dict], contract_external_id: str) -> set:
        """
        Process subscription table with overwrite/append logic.
        
        LOGIC:
        1. For each subscription in amendment data:
           a. Find existing subscription by ContractExternalId + ProductName
           b. If FOUND: Selectively update fields, track subExternalId for related data
           c. If NOT FOUND: Append new subscription
        2. Return set of processed subExternalIds for related data processing
        
        Args:
            original_json: The original JSON data
            subscription_data: List of subscription records from amendment
            contract_external_id: Contract external ID for matching
            
        Returns:
            Set of subExternalIds that were processed (for related data handling)
        """
        if "Subscription" not in original_json:
            original_json["Subscription"] = []
        
        processed_sub_external_ids = set()
        
        print(f"Processing {len(subscription_data)} subscription records...")
        print(f"Contract External ID: {contract_external_id}")
        print(f"Original JSON has {len(original_json.get('Subscription', []))} existing subscriptions")
            
        for new_subscription in subscription_data:
            subscription_name = new_subscription.get("ProductName")
            if not subscription_name:
                print(f"⚠️  Skipping subscription without ProductName")
                continue
                
            print(f"Processing subscription: {subscription_name}")
            
            # Find existing subscription by ContractExternalId + ProductName
            existing_subscription = self.find_existing_subscription(
                original_json, contract_external_id, subscription_name
            )
            
            if existing_subscription:
                # FOUND: Update existing subscription
                print(f"✅ Found existing subscription: {subscription_name}")
                print(f"   Existing ContractExternalId: {existing_subscription.get('ContractExternalId')}")
                print(f"   New ContractExternalId: {new_subscription.get('ContractExternalId')}")
                print(f"   Existing subExternalId: {existing_subscription.get('subExternalId')}")
                print(f"   New subExternalId: {new_subscription.get('subExternalId')}")
                
                old_sub_external_id = existing_subscription.get("subExternalId")
                new_sub_external_id = new_subscription.get("subExternalId")
                print(f"   Using new subExternalId: {new_sub_external_id}")
                
                # Selectively update fields (skip AccountName)
                changed_fields = self._selectively_update_fields(
                    existing_subscription, new_subscription, "Subscription", skip_fields=["AccountName"]
                )
                
                # Always update subExternalId if present
                if new_subscription.get("subExternalId"):
                    existing_subscription["subExternalId"] = new_subscription["subExternalId"]
                
                # If subExternalId changed, update all related records
                if old_sub_external_id and new_sub_external_id and old_sub_external_id != new_sub_external_id:
                    print(f"🔄 subExternalId changed from {old_sub_external_id} to {new_sub_external_id}")
                    print(f"   Updating all related records to use new subExternalId...")
                    
                    # Track the mapping for future reference
                    self.sub_external_id_mappings[old_sub_external_id] = new_sub_external_id
                    
                    # Update all related records in other tables
                    updated_records_count = self.update_related_records_for_subexternalid_change(
                        original_json, old_sub_external_id, new_sub_external_id
                    )
                    
                    if updated_records_count > 0:
                        print(f"✅ Successfully updated {updated_records_count} related records")
                    else:
                        print(f"ℹ️  No related records found to update")
                
                print(f"✅ Selectively updated subscription")
                self.amendment_summary["subscription_changes"]["updated"].append(subscription_name)
                
                # Track subExternalId for related data processing
                if new_sub_external_id:
                    self._overwrite_all_related_records(original_json, new_sub_external_id)
                    processed_sub_external_ids.add(new_sub_external_id)
            else:
                # NOT FOUND: Append new subscription
                print(f"❌ No existing subscription found for: {subscription_name}")
                print(f"   Looking for ContractExternalId: {contract_external_id}")
                print(f"   Available subscriptions in original JSON:")
                for i, sub in enumerate(original_json.get("Subscription", [])):
                    print(f"     {i}: {sub.get('ProductName')} (ContractExternalId: {sub.get('ContractExternalId')})")
                
                print(f"✅ Appending new subscription: {subscription_name}")
                original_json["Subscription"].append(new_subscription)
                self.amendment_summary["subscription_changes"]["added"].append(subscription_name)
                
                # Track subExternalId for related data processing
                sub_external_id = new_subscription.get("subExternalId")
                if sub_external_id:
                    processed_sub_external_ids.add(sub_external_id)
        
        return processed_sub_external_ids

    def _overwrite_all_related_records(self, original_json: Dict, sub_external_id: str):
        """Log related records processing."""
        print(f"Processing related records for subExternalId: {sub_external_id}")
        
        for table_name in self.related_tables:
            if table_name not in original_json:
                original_json[table_name] = []
                
            matching_indices = self.find_related_records(original_json, sub_external_id, table_name)
            if matching_indices:
                print(f"Found {len(matching_indices)} existing records in {table_name} for subExternalId: {sub_external_id} - will be upserted")
            else:
                print(f"No existing records found in {table_name} for subExternalId: {sub_external_id} - will be appended")

    def update_related_records_for_subexternalid_change(self, original_json: Dict, old_sub_external_id: str, new_sub_external_id: str):
        """
        Update all related records when a subExternalId changes from old to new value.
        
        This method ensures that when a subscription gets amended and receives a new subExternalId,
        all related records in other tables (LineItemSource, lisConsumptionSchedule, etc.) that
        reference the old subExternalId are updated to use the new subExternalId.
        
        Args:
            original_json: The original JSON data to be updated
            old_sub_external_id: The old subExternalId that needs to be replaced
            new_sub_external_id: The new subExternalId to use
        """
        print(f"🔄 Updating related records for subExternalId change: {old_sub_external_id} → {new_sub_external_id}")
        
        updated_tables = []
        total_updated_records = 0
        
        # Update all related tables that have subExternalId field
        for table_name in self.related_tables:
            if table_name not in original_json:
                continue
                
            table_updated_count = 0
            for record in original_json[table_name]:
                if record.get("subExternalId") == old_sub_external_id:
                    record["subExternalId"] = new_sub_external_id
                    table_updated_count += 1
                    print(f"  Updated {table_name} record: {old_sub_external_id} → {new_sub_external_id}")
            
            if table_updated_count > 0:
                updated_tables.append(f"{table_name}: {table_updated_count} records")
                total_updated_records += table_updated_count
        
        if updated_tables:
            print(f"✅ Updated {total_updated_records} records across {len(updated_tables)} tables:")
            for table_info in updated_tables:
                print(f"  - {table_info}")
        else:
            print(f"ℹ️  No related records found with subExternalId: {old_sub_external_id}")
        
        return total_updated_records

    def update_existing_records_with_mapped_subexternalids(self, original_json: Dict):
        """
        Update all existing records in the original JSON that have old subExternalId values
        that need to be updated to new ones based on the subExternalId mappings.
        
        This method handles cases where records in the original JSON might have old subExternalId
        values that should be updated to the new amended subExternalId values.
        
        Args:
            original_json: The original JSON data to be updated
        """
        if not self.sub_external_id_mappings:
            print("ℹ️  No subExternalId mappings found - skipping existing record updates")
            return
        
        print(f"🔄 Updating existing records with mapped subExternalIds...")
        print(f"   Mappings: {self.sub_external_id_mappings}")
        
        total_updated_records = 0
        updated_tables = []
        
        # Update all related tables that have subExternalId field
        for table_name in self.related_tables:
            if table_name not in original_json:
                continue
                
            table_updated_count = 0
            for record in original_json[table_name]:
                current_sub_external_id = record.get("subExternalId")
                if current_sub_external_id and current_sub_external_id in self.sub_external_id_mappings:
                    new_sub_external_id = self.sub_external_id_mappings[current_sub_external_id]
                    record["subExternalId"] = new_sub_external_id
                    table_updated_count += 1
                    print(f"  Updated {table_name} record: {current_sub_external_id} → {new_sub_external_id}")
            
            if table_updated_count > 0:
                updated_tables.append(f"{table_name}: {table_updated_count} records")
                total_updated_records += table_updated_count
        
        if updated_tables:
            print(f"✅ Updated {total_updated_records} existing records across {len(updated_tables)} tables:")
            for table_info in updated_tables:
                print(f"  - {table_info}")
        else:
            print(f"ℹ️  No existing records found with mapped subExternalIds")
        
        return total_updated_records

    def _deduplicate_table_by_key_pair(self, original_json: Dict, table_name: str, key_fields: tuple, external_id_field: str):
        """
        Generic deduplication method for any table based on key pairs.
        
        Args:
            original_json: The original JSON data
            table_name: Name of the table to deduplicate
            key_fields: Tuple of field names to use as key pair
            external_id_field: Field name containing the external ID to check for "amd_" prefix
        """
        if table_name not in original_json or not original_json[table_name]:
            return
        
        print(f"  Processing {table_name}...")
        original_count = len(original_json[table_name])
        
        # Group by key pair
        grouped_records = {}
        for record in original_json[table_name]:
            key = tuple(record.get(field, "") for field in key_fields)
            if key not in grouped_records:
                grouped_records[key] = []
            grouped_records[key].append(record)
        
        # Deduplicate each group
        deduplicated_records = []
        duplicates_removed = 0
        
        for key, records in grouped_records.items():
            if len(records) == 1:
                # No duplicates, keep as is
                deduplicated_records.append(records[0])
            else:
                # Has duplicates, apply deduplication logic
                print(f"    Found {len(records)} duplicates for key {key}")
                
                # Find record with "amd_" prefixed external ID
                amd_record = None
                for record in records:
                    external_id = record.get(external_id_field, "")
                    if external_id.startswith("amd_"):
                        amd_record = record
                        break
                
                if amd_record:
                    # Retain the record with "amd_" prefix
                    deduplicated_records.append(amd_record)
                    print(f"    Retained record with amd_ prefix: {amd_record.get(external_id_field)}")
                else:
                    # No "amd_" prefix found, retain the first record
                    deduplicated_records.append(records[0])
                    print(f"    No amd_ prefix found, retained first record: {records[0].get(external_id_field)}")
                
                duplicates_removed += len(records) - 1
        
        original_json[table_name] = deduplicated_records
        final_count = len(deduplicated_records)
        print(f"  ✅ {table_name}: {original_count} → {final_count} records (removed {duplicates_removed} duplicates)")

    def deduplicate_consumption_schedules(self, original_json: Dict):
        """
        Deduplicate records in consumption schedule tables based on specific field pairs.
        
        LOGIC:
        1. subConsumptionSchedule: Deduplicate by (subExternalId, subCsName) pairs
           - Retain record where subCsExternalId starts with "amd_"
           - If none have "amd_" prefix, retain the first record
           
        2. lisConsumptionSchedule: Deduplicate by (lisExternalId, scsName) pairs
           - Retain record where scsExternalId starts with "amd_"
           - If none have "amd_" prefix, retain the first record
           
        3. subConsumptionRate: Deduplicate by (subExternalId, subCrName) pairs
           - Retain record where subCrExternalId starts with "amd_"
           - If none have "amd_" prefix, retain the first record
           
        4. lisConsumptionRate: Deduplicate by (subExternalId, scrName) pairs
           - Retain record where scrExternalId starts with "amd_"
           - If none have "amd_" prefix, retain the first record
        
        Args:
            original_json: The original JSON data to be deduplicated
        """
        print("🔄 Deduplicating consumption schedule records...")
        
        # Use generic deduplication method for all tables
        self._deduplicate_table_by_key_pair(original_json, "subConsumptionSchedule", ("subExternalId", "subCsName"), "subCsExternalId")
        self._deduplicate_table_by_key_pair(original_json, "lisConsumptionSchedule", ("lisExternalId", "scsName"), "scsExternalId")
        self._deduplicate_table_by_key_pair(original_json, "subConsumptionRate", ("subExternalId", "subCrName"), "subCrExternalId")
        self._deduplicate_table_by_key_pair(original_json, "lisConsumptionRate", ("subExternalId", "scrName"), "scrExternalId")
        
        print("✅ Consumption schedule deduplication completed")

    def _process_other_table(self, original_json: Dict, table_name: str, table_data: List[Dict], processed_sub_external_ids: set):
        """Process other tables with selective field update logic."""
        if table_name not in original_json:
            original_json[table_name] = []
            
        if table_name in self.related_tables:
            print(f"Processing {table_name} with selective field update logic...")
            
            updated_count = 0
            added_count = 0
            
            for new_record in table_data:
                sub_external_id = new_record.get("subExternalId")
                
                # Check if this record should be processed
                should_process = False
                
                # SPECIAL HANDLING FOR DISCOUNT SCHEDULE:
                # Process discountSchedule records if they have DiscExtId (even without subExternalId)
                # This ensures discount schedules are processed independently of subscriptions
                if table_name == "discountSchedule" and new_record.get("DiscExtId"):
                    should_process = True
                    print(f"Processing discountSchedule record with DiscExtId: {new_record.get('DiscExtId')}")
                elif sub_external_id:
                    # For other tables, check if record belongs to processed subscriptions
                    # Check various subExternalId patterns (with and without "amd_" prefix)
                    old_sub_external_id = self._clean_external_id(sub_external_id)
                    should_process = (sub_external_id in processed_sub_external_ids or 
                                    old_sub_external_id in processed_sub_external_ids or
                                    any(self._clean_external_id(processed_id) == old_sub_external_id 
                                        for processed_id in processed_sub_external_ids))
                    
                    # Also check if this subExternalId is a mapped old ID that should be updated
                    if not should_process and sub_external_id in self.sub_external_id_mappings:
                        new_mapped_id = self.sub_external_id_mappings[sub_external_id]
                        print(f"🔄 Found mapped subExternalId: {sub_external_id} → {new_mapped_id}")
                        new_record["subExternalId"] = new_mapped_id
                        sub_external_id = new_mapped_id
                        should_process = True
                
                if not should_process:
                    print(f"Skipped record in {table_name} for subExternalId: {sub_external_id} (not in processed subscriptions)")
                    continue
                
                # Try to find existing record to update
                existing_record_index = self.find_specific_record_match(original_json, new_record, table_name)
                
                if existing_record_index is not None:
                    existing_record = original_json[table_name][existing_record_index]
                    print(f"Found existing record in {table_name} for subExternalId: {sub_external_id} - selectively updating fields")
                    
                    # Selectively update fields
                    self._selectively_update_fields(existing_record, new_record, table_name)
                    updated_count += 1
                    print(f"✅ Selectively updated existing record in {table_name}")
                else:
                    original_json[table_name].append(new_record)
                    print(f"✅ Added new record to {table_name} for subExternalId: {sub_external_id}")
                    added_count += 1
            
            # Track changes
            if table_name == "LineItemSource":
                if updated_count > 0 or added_count > 0:
                    self.amendment_summary["lineitem_changes"]["updated"].append(f"{updated_count} updated, {added_count} added")
                else:
                    self.amendment_summary["lineitem_changes"]["updated"].append("0 line items updated")
            elif table_name == "discountSchedule":
                if updated_count > 0 or added_count > 0:
                    self.amendment_summary["discount_schedule_changes"]["updated"].append(f"{updated_count} updated, {added_count} added")
                else:
                    self.amendment_summary["discount_schedule_changes"]["updated"].append("0 discount schedules updated")
        else:
            print(f"Appending {len(table_data)} records to {table_name}")
            original_json[table_name].extend(table_data)
            
            if table_name == "discountSchedule":
                self.amendment_summary["discount_schedule_changes"]["added"].append(f"{len(table_data)} new discount schedules")

    def _create_comprehensive_amendment_note(self, original_json: Dict):
        """Create a comprehensive amendment summary note."""
        if "Amendment Logs" not in original_json:
            original_json["Amendment Logs"] = []
        
        if original_json["Amendment Logs"]:
            latest_entry = original_json["Amendment Logs"][-1]
            
            # Build summary parts
            summary_parts = []
            
            # Contract Changes
            if self.amendment_summary["contract_changes"]:
                summary_parts.append("Contract Changes: " + "; ".join(self.amendment_summary["contract_changes"]))
            else:
                summary_parts.append("Contract Changes: None")
            
            # Subscription Changes
            sub_added = self.amendment_summary["subscription_changes"]["added"]
            sub_updated = self.amendment_summary["subscription_changes"]["updated"]
            
            if sub_added or sub_updated:
                sub_parts = []
                if sub_added:
                    sub_parts.append(f"Added: {', '.join(sub_added)}")
                if sub_updated:
                    sub_parts.append(f"Updated: {', '.join(sub_updated)}")
                summary_parts.append("Subscription Changes: " + "; ".join(sub_parts))
            else:
                summary_parts.append("Subscription Changes: None")
            
            # Line Item Source Changes
            lis_added = self.amendment_summary["lineitem_changes"]["added"]
            lis_updated = self.amendment_summary["lineitem_changes"]["updated"]
            
            if lis_added or lis_updated:
                lis_parts = lis_added + lis_updated
                summary_parts.append("Line Item Source Changes: " + "; ".join(lis_parts))
            else:
                summary_parts.append("Line Item Source Changes: None")
            
            # Discount Schedule Changes
            ds_added = self.amendment_summary["discount_schedule_changes"]["added"]
            ds_updated = self.amendment_summary["discount_schedule_changes"]["updated"]
            
            if ds_added or ds_updated:
                ds_parts = ds_added + ds_updated
                summary_parts.append("Discount Schedule Changes: " + "; ".join(ds_parts))
            else:
                summary_parts.append("Discount Schedule Changes: None")
            
            # Combine all parts
            comprehensive_note = " | ".join(summary_parts)
            latest_entry["Note"] = comprehensive_note
            
            print(f"✅ Created comprehensive amendment note: {comprehensive_note}")

    def overwrite_subscription_and_related(self, original_json: Dict, new_data: Dict, contract_external_id: str) -> Dict:
        """
        Main overwrite processing method - orchestrates the entire amendment processing flow.
        
        LOGIC FLOW:
        1. Validate input data (check for output_records)
        2. Reset amendment summary for new processing run
        3. Preserve Amendment Logs from pipeline output
        4. Process Contract-level changes first
        5. Update DiscExtId values across tables
        6. Process each output record:
           - Skip Contract (already processed)
           - Process Subscription table (returns processed subExternalIds)
           - Process other tables using processed subExternalIds
        7. Create comprehensive amendment summary note
        
        Args:
            original_json: The original JSON data to be updated
            new_data: Pipeline output containing amendment data
            contract_external_id: Contract external ID for matching
            
        Returns:
            Updated original JSON with amendment changes applied
        """
        if "output_records" not in new_data:
            print("No output_records found in new data")
            return original_json
        
        # Reset amendment summary for new processing run
        self._reset_amendment_summary()
            
        # Append Amendment Logs from new_data if it exists
        if "Amendment Logs" in new_data:
            if "Amendment Logs" not in original_json:
                original_json["Amendment Logs"] = []
            original_json["Amendment Logs"].extend(new_data["Amendment Logs"])
            print(f"✅ Appended {len(new_data['Amendment Logs'])} Amendment Log entries from pipeline output")
        
        processed_sub_external_ids = set()
        
        print(f"Processing amendment data...")
        print(f"Contract External ID: {contract_external_id}")
        print(f"New data has {len(new_data.get('output_records', []))} output records")
        
        # STEP 1: Process Contract-level changes first
        self._process_contract_changes(original_json, new_data, contract_external_id)
        
        # STEP 2: Process DiscExtId updates across discountSchedule and Subscription tables
        self.update_disc_ext_ids(original_json, new_data)
            
        # STEP 3: Process each output record
        for record in new_data["output_records"]:
            table_name = record.get("name")
            table_data = record.get("data", [])
            
            if not table_data:
                continue
                
            print(f"Processing table: {table_name} with {len(table_data)} records")
            
            # Skip Contract table - already processed in _process_contract_changes
            if table_name == "Contract":
                print(f"Skipping Contract table - already processed in _process_contract_changes")
                continue
            
            # Process Subscription table - returns processed subExternalIds
            if table_name == "Subscription":
                processed_ids = self._process_subscription_table(original_json, table_data, contract_external_id)
                processed_sub_external_ids.update(processed_ids)
            else:
                # Process other tables using processed subExternalIds for filtering
                self._process_other_table(original_json, table_name, table_data, processed_sub_external_ids)
        
        # STEP 4: Update existing records with mapped subExternalIds
        self.update_existing_records_with_mapped_subexternalids(original_json)
        
        # STEP 5: Deduplicate consumption schedule records
        self.deduplicate_consumption_schedules(original_json)
        
        # STEP 6: Create comprehensive amendment summary note
        self._create_comprehensive_amendment_note(original_json)
                
        return original_json

    def main(self, original_json: Dict, new_data: Dict, contract_external_id: str):
        """Main method to process amendment data."""
        try:
            updated_json = self.overwrite_subscription_and_related(original_json, new_data, contract_external_id)
            print(f"✅ Amendment processing completed successfully")
            return updated_json
        except Exception as e:
            print(f"❌ Amendment processing failed: {e}")
            return None


def main(original_json: Dict, new_data: Dict):
    """Standalone main function to process amendment data."""
    contract_external_id = new_data.get("Contractid")
    
    # If no Contractid found, just append Amendment Logs (for unmatched cases)
    if not contract_external_id:
        print("⚠️  No Contractid found in pipeline output - appending Amendment Logs only")
        
        if "Amendment Logs" in new_data and new_data["Amendment Logs"]:
            if "Amendment Logs" not in original_json:
                original_json["Amendment Logs"] = []
            
            original_json["Amendment Logs"].extend(new_data["Amendment Logs"])
            print(f"✅ Appended {len(new_data['Amendment Logs'])} Amendment Log entries")
        else:
            print("⚠️  No Amendment Logs found in pipeline output")
        
        return original_json
    
    # If Contractid found, proceed with full overwrite processing
    overwriter = AmendmentOverwriter()
    return overwriter.main(original_json, new_data, contract_external_id)


# if __name__ == "__main__":
#     # ========== HARDCODED INPUTS ==========
#     pipeline_output_path = "BACKEND/THIRDV/amendments_files_08_04/output/json_parsed_Plum_New_Markets_add_ons.docx.json"
#     original_json_path = "BACKEND/THIRDV/amendments_files_08_04/extracted_data.json"
#     # ======================================
    
#     print("=" * 60)
#     print("AMENDMENT OVERWRITE SCRIPT (OPTIMIZED)")
#     print("=" * 60)
#     print(f"Pipeline Output: {pipeline_output_path}")
#     print(f"Original JSON: {original_json_path}")
#     print("=" * 60)
    
#     try:
#         print("\n📋 Loading files...")
#         with open(original_json_path, 'r', encoding='utf-8') as f:
#             original_json = json.load(f)
#         print(f"✅ Loaded original JSON: {original_json_path}")
        
#         with open(pipeline_output_path, 'r', encoding='utf-8') as f:
#             new_data = json.load(f)
#         print(f"✅ Loaded pipeline output: {pipeline_output_path}")
        
#         print("\n🔄 Processing amendment overwrite...")
#         updated_json = main(original_json, new_data)
        
#         if updated_json:
#             output_path = f"{os.path.splitext(original_json_path)[0]}_updated.json"
#             with open(output_path, 'w', encoding='utf-8') as f:
#                 json.dump(updated_json, f, indent=4, ensure_ascii=False)
#             print(f"\n✅ Updated JSON saved to: {output_path}")
            
#             try:
#                 print("\n📊 Converting to XLSX...")
#                 import pandas as pd
                
#                 xlsx_path = f"{os.path.splitext(output_path)[0]}.xlsx"
                
#                 with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
#                     for section_name, section_data in updated_json.items():
#                         if isinstance(section_data, list) and section_data:
#                             df = pd.DataFrame(section_data)
#                             sheet_name = section_name[:31]
#                             df.to_excel(writer, sheet_name=sheet_name, index=False)
#                             print(f"✅ Added sheet '{sheet_name}' with {len(section_data)} records")
#                         elif isinstance(section_data, dict):
#                             df = pd.DataFrame([section_data])
#                             sheet_name = section_name[:31]
#                             df.to_excel(writer, sheet_name=sheet_name, index=False)
#                             print(f"✅ Added sheet '{sheet_name}' with 1 record")
                
#                 print(f"✅ XLSX file saved to: {xlsx_path}")
                
#             except Exception as e:
#                 print(f"❌ Error converting to XLSX: {e}")
            
#             print("🎉 Amendment overwrite completed successfully!")
#         else:
#             print("\n❌ Amendment overwrite failed")
        
#     except FileNotFoundError as e:
#         print(f"❌ Input files not found: {e}")
#     except Exception as e:
#         print(f"❌ Error processing amendment: {e}")


# """
# ARCHITECTURE SUMMARY:

# This optimized amendment overwrite script follows a modular, step-by-step approach:

# 1. CONFIGURATION LAYER:
#    - External ID field mappings (eliminates repetitive if-elif chains)
#    - Name field mappings (for better record matching)
#    - Related tables configuration

# 2. HELPER METHODS:
#    - _is_na_or_empty(): Centralized NA/empty detection
#    - _selectively_update_fields(): Centralized field update logic
#    - _get_external_id(): Table-specific external ID extraction
#    - _clean_external_id(): "amd_" prefix handling
#    - _find_best_match(): Name-based record matching

# 3. CORE PROCESSING METHODS:
#    - find_specific_record_match(): External ID-based record matching
#    - _process_contract_changes(): Contract-level selective updates
#    - _process_subscription_table(): Subscription overwrite/append logic
#    - _process_other_table(): Related data processing
#    - update_disc_ext_ids(): Cross-table DiscExtId updates
#    - update_related_records_for_subexternalid_change(): Handle subExternalId changes
#    - update_existing_records_with_mapped_subexternalids(): Update existing records with new subExternalIds
#    - deduplicate_consumption_schedules(): Remove duplicate consumption schedule records

# 4. ORCHESTRATION:
#    - overwrite_subscription_and_related(): Main processing flow
#    - _create_comprehensive_amendment_note(): Summary generation
#    - main(): Entry point with unmatched case handling

# 5. SUBEXTERNALID MAPPING SYSTEM:
#    - Tracks mappings from old subExternalId to new subExternalId when subscriptions are amended
#    - Updates all related records in other tables (LineItemSource, lisConsumptionSchedule, etc.)
#    - Handles both amendment data and existing records in original JSON
#    - Ensures referential integrity across all tables when subExternalId changes

# KEY OPTIMIZATIONS:
# - Eliminated code duplication through centralized methods
# - Reduced complexity with configuration mappings
# - Improved maintainability with clear separation of concerns
# - Preserved all original functionality while reducing code size by ~30%

# DISCOUNT SCHEDULE & RATE PROCESSING ARCHITECTURE:

# 1. CROSS-TABLE DISC EXT ID UPDATES:
#    - update_disc_ext_ids(): Handles "amd_" prefixed DiscExtId values
#    - Updates both discountSchedule and Subscription tables
#    - Maintains referential integrity between discounts and subscriptions
#    - Maps original DiscExtId → amended DiscExtId

# 2. DISCOUNT SCHEDULE PROCESSING:
#    - Special handling in _process_other_table() for discountSchedule
#    - Processes records with DiscExtId even without subExternalId
#    - Ensures discount schedules are processed independently of subscriptions
#    - Uses DiscExtId for matching and updating existing records

# 3. DISCOUNT RATE PROCESSING:
#    - Handled through standard related table processing
#    - Uses subExternalId for linking to processed subscriptions
#    - Applies selective field updates (skip NA/empty values)
#    - Tracks changes in amendment summary

# 4. REFERENTIAL INTEGRITY:
#    - Ensures discount schedules and rates stay linked to correct subscriptions
#    - Updates DiscExtId across all related tables
#    - Prevents orphaned discount records
#    - Maintains data consistency throughout the amendment process
# """
