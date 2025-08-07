import smartsheet
import pandas as pd
from dateutil.parser import parse as parse_date
from datetime import datetime, timedelta
import pytz

# ---- CONFIG ----
API_TOKEN = 'YOUR_SMARTSHEET_API_TOKEN_HERE'  # <-- INSERT YOUR API TOKEN
REFERENCE_SHEET_ID = 3239244454645636         # Source structure sheet
ARCHIVE_SHEET_ID = 7514584211476356           # Archive Master Sheet
HELPER_SHEET_ID = 2383431225790340            # Optional: Staging/Helper Sheet

# Set to True for testing (won't modify data), False for production
DRY_RUN = False

# Main mapping: {destination_col: source_col}
COLUMN_MAP = {
    'PROMAX': 'Units Total Price',
    'Job Number': 'Job #',
    'Work Order': 'Work Order',
    'Work Release': 'Work Release #',
    'Start Date': 'Start Date',
    'Scope Number': 'Scope Number',
    'Foreman': 'Foreman',  # Foreman maps to Foreman on both source and archive
    'Location': 'Location',
    'Crew': 'Crew',
    '% of Completion': '% of Completion',
    'Weekly Reference Logged Date': 'Weekly Reference Logged Date',  # Used for tracking weekly revenue per foreman
    'Expected Time of Completion': '',  # leave blank for now
    'Snapshot Date': 'Snapshot Date',
    'Work Request #': 'Work Request #'  # This will map to "Work Request" in archive
}

# ---- HELPER FUNCTIONS ----
def get_matching_sheets(ss_client, reference_sheet_id):
    """Finds all sheets that have the same columns as the reference structure."""
    print(f"  Getting reference sheet structure from ID: {reference_sheet_id}")
    ref_sheet = ss_client.Sheets.get_sheet(reference_sheet_id)
    ref_col_set = set([col.title for col in ref_sheet.columns])
    print(f"  Reference columns: {sorted(ref_col_set)}")
    
    # Updated to use new pagination API instead of deprecated include_all=True
    all_sheets = []
    page_size = 100  # Maximum allowed page size
    page_number = 1
    
    while True:
        try:
            response = ss_client.Sheets.list_sheets(page_size=page_size, page=page_number)
            if not response.data:
                break
            all_sheets.extend(response.data)
            
            # Check if we have more pages
            if len(response.data) < page_size:
                break
            page_number += 1
        except Exception as e:
            print(f"    ⚠️  Warning: Error fetching page {page_number}: {e}")
            break
    
    print(f"  Found {len(all_sheets)} total sheets in workspace")
    
    matching_sheets = []
    for s in all_sheets:
        try:
            this_sheet = ss_client.Sheets.get_sheet(s.id)
            this_col_set = set([col.title for col in this_sheet.columns])
            # At least all required columns exist (may have extras)
            if ref_col_set.issubset(this_col_set):
                matching_sheets.append({'id': s.id, 'name': s.name})
                print(f"    ✓ MATCH: {s.name} (ID: {s.id})")
            else:
                missing_cols = ref_col_set - this_col_set
                print(f"    ✗ SKIP: {s.name} - Missing columns: {missing_cols}")
        except Exception as e:
            print(f"    ✗ ERROR accessing {s.name}: {e}")
            continue
    return matching_sheets

def get_sheet_data(ss_client, sheet_id, sheet_name="Unknown"):
    """Pulls all rows from a Smartsheet, returns as list of dicts keyed by column name."""
    print(f"  Reading data from {sheet_name} (ID: {sheet_id})")
    sheet = ss_client.Sheets.get_sheet(sheet_id)
    col_map = {col.id: col.title for col in sheet.columns}
    data = []
    for row in sheet.rows:
        row_dict = {}
        for cell in row.cells:
            col_name = col_map.get(cell.column_id, None)
            if col_name:
                row_dict[col_name] = cell.value
        data.append(row_dict)
    print(f"    Found {len(data)} total rows")
    return data

def get_archive_existing_keys(ss_client, archive_sheet_id, unique_key_cols):
    """Get all existing archive records as a set of tuples for uniqueness checking."""
    print(f"  Reading existing archive records from ID: {archive_sheet_id}")
    try:
        sheet = ss_client.Sheets.get_sheet(archive_sheet_id)
        col_map = {col.title: col.id for col in sheet.columns}
        
        # First, let's see what columns actually exist in the archive sheet
        print(f"    Archive sheet columns: {sorted(col_map.keys())}")
        
        # Map the uniqueness columns to actual archive column names
        archive_col_mapping = {
            'Work Request #': 'Work Request',  # Archive uses "Work Request" not "Work Request #"
            'Work Release': 'Work Release #'   # Check if "Work Release #" exists in archive
        }
        
        # Check if all unique key columns exist (with mapping)
        missing_cols = []
        available_cols = []
        for col in unique_key_cols:
            archive_col_name = archive_col_mapping.get(col, col)  # Use mapping or original name
            if archive_col_name not in col_map:
                missing_cols.append(f"{col} (looking for '{archive_col_name}')")
            else:
                available_cols.append(col)
        
        if missing_cols:
            print(f"    WARNING: Archive sheet missing columns: {missing_cols}")
            print(f"    Will use available columns for duplicate detection: {available_cols}")
        
        # Use only available columns for duplicate detection
        effective_key_cols = available_cols if available_cols else unique_key_cols
        print(f"    Using columns for duplicate detection: {effective_key_cols}")
        
        keys = set()
        for row in sheet.rows:
            try:
                key_values = []
                for col in effective_key_cols:
                    archive_col_name = archive_col_mapping.get(col, col)
                    if archive_col_name in col_map:
                        value = row.get_column(col_map[archive_col_name]).value
                    else:
                        value = None
                    key_values.append(value)
                key = tuple(key_values)
                keys.add(key)
            except Exception as e:
                print(f"    WARNING: Error processing archive row: {e}")
                continue
        
        print(f"    Found {len(keys)} existing archive records")
        return keys, effective_key_cols
    except Exception as e:
        print(f"    ERROR: Could not read archive sheet: {e}")
        return set(), unique_key_cols

def week_start(date_obj):
    """Returns the most recent Sunday before or on the given date (week starts Sunday)."""
    # FIXED: Corrected week start calculation
    if not isinstance(date_obj, datetime):
        date_obj = parse_date(str(date_obj))
    
    # Python weekday(): Monday=0, Sunday=6
    # We want to go back to the most recent Sunday
    days_since_sunday = (date_obj.weekday() + 1) % 7
    return date_obj - timedelta(days=days_since_sunday)

def is_past_week(date_str):
    """Checks if a date string is before the start of the current week."""
    if not date_str:
        return False
    today = datetime.now()
    try:
        date_val = parse_date(str(date_str))
    except Exception:
        return False
    # Start of current week (Sunday)
    current_week_start = week_start(today)
    return date_val < current_week_start

# ---- MAIN WORKFLOW ----
def main():
    print("=== UPR REPORT MAPPING TOOL ===")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"Current time: {datetime.now()}")
    
    if API_TOKEN == 'YOUR_SMARTSHEET_API_TOKEN_HERE':
        print("\n❌ ERROR: Please set your API_TOKEN before running!")
        return
    
    try:
        ss_client = smartsheet.Smartsheet(API_TOKEN)
        print(f"\n✓ Connected to Smartsheet API")
    except Exception as e:
        print(f"\n❌ ERROR: Could not connect to Smartsheet: {e}")
        return
    
    # 1. Find all sheets matching the structure
    print('\n=== STEP 1: DISCOVERING MATCHING SHEETS ===')
    try:
        matching_sheets = get_matching_sheets(ss_client, REFERENCE_SHEET_ID)
        print(f'\nFound {len(matching_sheets)} matching sheets:')
        for sheet in matching_sheets:
            print(f"  - {sheet['name']} (ID: {sheet['id']})")
    except Exception as e:
        print(f"❌ ERROR in sheet discovery: {e}")
        return

    # 2. Pull and consolidate data from all source sheets
    print('\n=== STEP 2: PULLING AND FILTERING DATA ===')
    records = []
    total_rows_processed = 0
    rows_with_promax = 0
    rows_with_past_dates = 0
    rows_meeting_criteria = 0
    
    for sheet in matching_sheets:
        try:
            rows = get_sheet_data(ss_client, sheet['id'], sheet['name'])
            total_rows_processed += len(rows)
            
            sheet_records = 0
            for row in rows:
                # Only pull rows with Units Total Price (PROMAX) > 0 and a valid Week Ending date in past
                promax_val = row.get(COLUMN_MAP['PROMAX'], None)
                week_ending = row.get(COLUMN_MAP['Weekly Reference Logged Date'], None)
                
                # Track statistics
                has_promax = promax_val is not None
                if has_promax:
                    try:
                        has_promax = float(promax_val) > 0
                        if has_promax:
                            rows_with_promax += 1
                    except:
                        has_promax = False
                
                has_past_date = is_past_week(week_ending)
                if has_past_date:
                    rows_with_past_dates += 1
                
                if not promax_val or not has_past_date:
                    continue
                    
                try:
                    if float(promax_val) == 0:
                        continue
                except Exception:
                    continue
                
                # Build output row using mapping
                out_row = {}
                for dest_col, src_col in COLUMN_MAP.items():
                    out_row[dest_col] = row.get(src_col, '') if src_col else ''
                records.append(out_row)
                sheet_records += 1
                rows_meeting_criteria += 1
            
            print(f"    {sheet['name']}: {sheet_records} records meeting criteria")
            
        except Exception as e:
            print(f"    ❌ ERROR processing {sheet['name']}: {e}")
            continue

    print(f"\n📊 DATA FILTERING SUMMARY:")
    print(f"  Total rows processed: {total_rows_processed}")
    print(f"  Rows with PROMAX > 0: {rows_with_promax}")
    print(f"  Rows with past week dates: {rows_with_past_dates}")
    print(f"  Rows meeting both criteria: {rows_meeting_criteria}")

    df = pd.DataFrame(records)
    if df.empty:
        print('\n❌ No records meet the criteria.')
        return

    # Show sample data
    print(f"\n📋 SAMPLE OF {len(records)} RECORDS:")
    print(df.head().to_string())
    
    print(f"\n📊 COLUMN STATISTICS:")
    for col in df.columns:
        non_null_count = df[col].notna().sum()
        print(f"  {col}: {non_null_count}/{len(df)} non-null values")

    # 3. Group and consolidate records by foreman/job combination
    print('\n=== STEP 3: GROUPING RECORDS BY FOREMAN/JOB COMBINATION ===')
    print("Logic: Consolidate all line items into one summary record per [Job Number + Foreman + Work Release]")
    print("Each group will sum PROMAX values and use most recent dates")
    
    unique_key_cols = ['Job Number', 'Foreman', 'Work Release']
    print(f"Grouping criteria: {unique_key_cols}")
    
    # Group records by the unique combination
    grouped_records = {}
    for _, rec in df.iterrows():
        key = tuple(rec.get(col, None) for col in unique_key_cols)
        if key not in grouped_records:
            grouped_records[key] = []
        grouped_records[key].append(rec)
    
    print(f"\n📊 GROUPING ANALYSIS:")
    print(f"  Found {len(grouped_records)} unique job/foreman combinations")
    print(f"  Consolidating {len(records)} individual line items into {len(grouped_records)} summary records")
    
    # Create consolidated records
    consolidated_records = []
    for key, group_records in grouped_records.items():
        job_num, foreman, work_release = key
        
        # Sum up all PROMAX values for this group
        total_promax = sum(float(r.get('PROMAX', 0)) for r in group_records if r.get('PROMAX'))
        
        # Use the most recent dates and other fields from the last record
        latest_record = max(group_records, key=lambda x: x.get('Weekly Reference Logged Date', ''))
        
        # Create consolidated record
        consolidated_record = {
            'PROMAX': total_promax,
            'Job Number': job_num,
            'Foreman': foreman,
            'Work Release': work_release,
            'Work Order': latest_record.get('Work Order', ''),
            'Start Date': latest_record.get('Start Date', ''),
            'Scope Number': latest_record.get('Scope Number', ''),
            'Location': latest_record.get('Location', ''),
            'Crew': latest_record.get('Crew', ''),
            '% of Completion': latest_record.get('% of Completion', ''),
            'Weekly Reference Logged Date': latest_record.get('Weekly Reference Logged Date', ''),
            'Expected Time of Completion': latest_record.get('Expected Time of Completion', ''),
            'Snapshot Date': latest_record.get('Snapshot Date', ''),
            'Work Request #': latest_record.get('Work Request #', '')
        }
        
        consolidated_records.append(consolidated_record)
    
    # Show sample of consolidated data
    print(f"\n📋 SAMPLE OF {len(consolidated_records)} CONSOLIDATED RECORDS:")
    for i, rec in enumerate(consolidated_records[:5]):
        print(f"  Group {i+1}: Job {rec['Job Number']} + {rec['Foreman']} = ${rec['PROMAX']:,.2f}")
    
    if len(consolidated_records) > 5:
        print(f"    ... and {len(consolidated_records)-5} more groups")
    
    # Show top 5 by revenue
    sorted_by_revenue = sorted(consolidated_records, key=lambda x: x['PROMAX'], reverse=True)
    print(f"\n💰 TOP 5 BY REVENUE:")
    for i, rec in enumerate(sorted_by_revenue[:5]):
        print(f"  #{i+1}: Job {rec['Job Number']} + {rec['Foreman']} = ${rec['PROMAX']:,.2f}")
    
    # Check against archive for duplicates
    print('\n=== STEP 4: CHECKING CONSOLIDATED RECORDS AGAINST ARCHIVE ===')
    
    try:
        archive_keys, effective_key_cols = get_archive_existing_keys(ss_client, ARCHIVE_SHEET_ID, unique_key_cols)
        
        new_consolidated_rows = []
        existing_transitions = 0
        
        for rec in consolidated_records:
            key = tuple(rec.get(col, None) for col in effective_key_cols)
            if key not in archive_keys:
                new_consolidated_rows.append(rec)
            else:
                existing_transitions += 1
        
        print(f"\n📈 CONSOLIDATION RESULTS:")
        print(f"  Existing job/foreman combinations in archive: {len(archive_keys)}")
        print(f"  New job/foreman combinations to add: {len(new_consolidated_rows)}")
        print(f"  Combinations that already exist: {existing_transitions}")
        
        if new_consolidated_rows:
            print(f"\n📋 NEW CONSOLIDATED RECORDS TO ADD:")
            for i, rec in enumerate(new_consolidated_rows[:5]):
                print(f"  Record {i+1}: Job {rec['Job Number']} + {rec['Foreman']} = ${rec['PROMAX']:,.2f}")
            if len(new_consolidated_rows) > 5:
                print(f"    ... and {len(new_consolidated_rows)-5} more records")
            
            # Update the records list to use consolidated data
            records = new_consolidated_rows
            new_rows = new_consolidated_rows
    
    except Exception as e:
        print(f"❌ ERROR checking archive: {e}")
        # Proceed with all consolidated records if archive check fails
        new_rows = consolidated_records
        effective_key_cols = unique_key_cols
        print(f"⚠️  Proceeding with all {len(consolidated_records)} consolidated records")

    if not new_rows:
        print('\n✅ No new foreman/job combinations to add.')
        return

    # 5. Append to archive master sheet
    print(f'\n=== STEP 5: ARCHIVE OPERATION ===')
    if DRY_RUN:
        print(f"🔍 DRY RUN: Would append {len(new_rows)} consolidated records to archive")
        print("📋 Consolidated records that would be added:")
        for i, rec in enumerate(new_rows[:5]):  # Show first 5
            print(f"  Record {i+1}:")
            print(f"    Job Number: {rec.get('Job Number')}")
            print(f"    Foreman: {rec.get('Foreman')}")
            print(f"    PROMAX (Total): ${rec.get('PROMAX'):,.2f}")
            print(f"    Work Release: {rec.get('Work Release')}")
            print(f"    Weekly Reference Logged Date: {rec.get('Weekly Reference Logged Date')}")
            print()
        if len(new_rows) > 5:
            print(f"    ... and {len(new_rows)-5} more consolidated records")
    else:
        print(f"🔄 PRODUCTION MODE: Appending {len(new_rows)} consolidated records to archive...")
        try:
            archive_sheet = ss_client.Sheets.get_sheet(ARCHIVE_SHEET_ID)
            col_map = {col.title: col.id for col in archive_sheet.columns}
            
            # Map destination columns to actual archive column names
            archive_col_mapping = {
                'Work Request #': 'Work Request',  # Archive uses "Work Request" not "Work Request #"
                'Work Release': 'Work Release #'   # Check if "Work Release #" exists in archive
            }
            
            # Check if all required columns exist in archive (with mapping)
            missing_cols = []
            for col in COLUMN_MAP.keys():
                if col in new_rows[0]:  # Only check columns that have data
                    archive_col_name = archive_col_mapping.get(col, col)
                    if archive_col_name not in col_map:
                        missing_cols.append(f"{col} (looking for '{archive_col_name}')")
            
            if missing_cols:
                print(f"⚠️  WARNING: Archive sheet missing columns: {missing_cols}")
            
            new_sheet_rows = []
            for rec in new_rows:
                cells = []
                for col, val in rec.items():
                    if val:  # Only add non-empty values
                        archive_col_name = archive_col_mapping.get(col, col)
                        if archive_col_name in col_map:
                            cells.append({'column_id': col_map[archive_col_name], 'value': val})
                new_sheet_rows.append(smartsheet.models.Row(cells=cells, to_bottom=True))
            
            response = ss_client.Sheets.add_rows(ARCHIVE_SHEET_ID, new_sheet_rows)
            print(f"✅ Successfully added {len(response.data)} consolidated rows to the archive.")
            
        except Exception as e:
            print(f"❌ ERROR adding rows to archive: {e}")

    print(f"\n🎉 Process completed!")

if __name__ == '__main__':
    main()
