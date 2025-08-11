import time
import requests
from datetime import datetime
from auth_refresh import get_dynamic_headers
from sqlalchemy import create_engine, text
from typing import List, Tuple, Dict, Any, Optional

# --- Configuration ---
API_BASE_URL = "https://calljacob.api.filevineapp.com"

DB_USER = "postgres"
DB_PASSWORD = "kritagya"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "calljacob"
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL, echo=False)

# --- SQL Statements ---
UPSERT_NEGOTIATION = text("""
INSERT INTO negotiation(
  project_id, negotiator, settlement_date, settled,
  settled_amount, last_offer, last_offer_date, date_assigned_to_nego
) VALUES (
  :project_id, :negotiator, :settlement_date, :settled,
  :settled_amount, :last_offer, :last_offer_date, :date_assigned_to_nego
)
ON CONFLICT (project_id) DO UPDATE SET
  negotiator            = EXCLUDED.negotiator,
  settlement_date       = EXCLUDED.settlement_date,
  settled               = EXCLUDED.settled,
  settled_amount        = EXCLUDED.settled_amount,
  last_offer            = EXCLUDED.last_offer,
  last_offer_date       = EXCLUDED.last_offer_date,
  date_assigned_to_nego = EXCLUDED.date_assigned_to_nego;
""")

def mmddyyyy_to_iso(s: Optional[str]) -> Optional[str]:
    """Convert MM-DD-YYYY to YYYY-MM-DD or return None"""
    if not s or s == "N/A":
        return None
    try:
        m, d, y = s.split("-")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        return None

def fetch_json(endpoint: str) -> Dict:
    """Fetch JSON from API endpoint with retry logic"""
    headers = get_dynamic_headers()
    try:
        r = requests.get(API_BASE_URL + endpoint, headers=headers, timeout=30)
        if r.status_code == 401:
            headers = get_dynamic_headers()
            r = requests.get(API_BASE_URL + endpoint, headers=headers, timeout=30)
        elif r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 5))
            print(f"⚠️ Rate limited, sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            return fetch_json(endpoint)
        r.raise_for_status()
        return r.json() or {}
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request failed for {endpoint}: {e}")
        return {}

def format_date(datestr: Optional[str]) -> str:
    """Format date string to MM-DD-YYYY or return 'N/A'"""
    if not datestr:
        return "N/A"
    try:
        dt = datetime.fromisoformat(datestr[:10])
        return dt.strftime("%m-%d-%Y")
    except:
        return "N/A"

def get_projects_to_update() -> List[int]:
    """Get project IDs in specific phases that need negotiation updates"""
    query = text("""
        SELECT project_id 
        FROM projects 
        WHERE phase_name IN (
            'Nego', 'Neg/Rls', 'Breakdown', 'Litigation', 
            'Lit/Nego', 'Nego/UMARB', 'Lit/Breakdown', 
            'Accounting', 'Disbursement'
        )
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]

def get_current_negotiation_data(pid: int) -> Dict[str, Any]:
    """Get current negotiation data from database"""
    query = text("""
        SELECT negotiator, settlement_date, settled, 
               settled_amount, last_offer, last_offer_date, date_assigned_to_nego
        FROM negotiation
        WHERE project_id = :pid
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"pid": pid}).fetchone()
        return {
            "negotiator": result[0] if result else None,
            "settlement_date": result[1] if result else None,
            "settled": result[2] if result else None,
            "settled_amount": float(result[3]) if result and result[3] is not None else None,
            "last_offer": result[4] if result else None,
            "last_offer_date": result[5] if result else None,
            "date_assigned_to_nego": result[6] if result else None
        } if result else {}

def get_nego_info(pid: int) -> Dict[str, Any]:
    """Get negotiation info from API with enhanced error handling"""
    try:
        d = fetch_json(f"/core/projects/{pid}/Forms/negotiation")
        if not isinstance(d, dict):
            raise ValueError("Invalid response format")
            
        nego = d.get("negoAssignedTo", {}) or {}
        
        # Convert all values to consistent format
        return {
            "negotiator": str(nego.get("fullname", "N/A")),
            "settlement_date": format_date(d.get("settlementDate")),
            "settled": str(d.get("settled", "N/A")),
            "settled_amount": float(d["settledAmount"]) if d.get("settledAmount") not in [None, "N/A"] else None,
            "last_offer": str(d.get("lastOffer", "N/A")),
            "last_offer_date": format_date(d.get("lastOfferDate")),
            "date_assigned_to_nego": format_date(d.get("dateAssignedToNego"))
        }
        
    except Exception as e:
        print(f"⚠️ Error getting negotiation info for {pid}: {str(e)}")
        return {}

def has_changes(current: Dict[str, Any], new: Dict[str, Any]) -> Tuple[bool, Dict[str, Tuple[Any, Any]]]:
    """
    Compare current and new negotiation data.
    Returns (True, changes_dict) if changes found, (False, {}) otherwise.
    """
    changes = {}
    fields_to_compare = [
        "negotiator", "settlement_date", "settled",
        "settled_amount", "last_offer", "last_offer_date",
        "date_assigned_to_nego"
    ]
    
    for field in fields_to_compare:
        current_val = current.get(field)
        new_val = new.get(field)
        
        # Special handling for different field types
        if field == "settled_amount":
            current_val = float(current_val) if current_val not in [None, "N/A"] else None
            new_val = float(new_val) if new_val not in [None, "N/A"] else None
        elif field.endswith("_date"):
            current_val = str(current_val or "")
            new_val = str(new_val or "")
        
        # Compare values
        if str(current_val or "") != str(new_val or ""):
            changes[field] = (current_val, new_val)
    
    return (bool(changes), changes)

def update_negotiation(pid: int) -> Tuple[bool, bool, Optional[Dict]]:
    """Update negotiation info for a single project with detailed change tracking"""
    try:
        print(f"\n🔍 Checking project {pid}...")
        
        # Get current data from DB
        current_data = get_current_negotiation_data(pid)
        
        # Get new data from API
        new_data = get_nego_info(pid)
        if not new_data:
            print(f"⚠️ No negotiation data returned for project {pid}")
            return False, False, None
        
        # Prepare data for comparison
        new_data_processed = {
            "negotiator": new_data["negotiator"],
            "settlement_date": mmddyyyy_to_iso(new_data["settlement_date"]),
            "settled": new_data["settled"],
            "settled_amount": new_data["settled_amount"],
            "last_offer": new_data["last_offer"],
            "last_offer_date": mmddyyyy_to_iso(new_data["last_offer_date"]),
            "date_assigned_to_nego": mmddyyyy_to_iso(new_data["date_assigned_to_nego"])
        }
        
        # Check for changes
        has_change, changes = has_changes(current_data, new_data_processed)
        
        if not has_change:
            print(f"✅ Project {pid}: No changes detected")
            return True, False, None
        
        # Log the specific changes
        print(f"🔄 Project {pid}: Detected changes:")
        for field, (old_val, new_val) in changes.items():
            print(f"   - {field}: {old_val} → {new_val}")
        
        # Update the database
        with engine.begin() as conn:
            conn.execute(UPSERT_NEGOTIATION, {
                "project_id": pid,
                **new_data_processed
            })
        
        print(f"💾 Project {pid}: Successfully updated")
        return True, True, changes

    except Exception as e:
        print(f"❌ Project {pid}: Failed to update - {str(e)}")
        import traceback
        traceback.print_exc()
        return False, False, None

def main():
    """Main execution function"""
    # Get projects in target phases
    project_ids = get_projects_to_update()
    print(f"Found {len(project_ids)} projects to check for updates")
    
    if not project_ids:
        print("No projects found in target phases")
        return
    
    # Process projects
    failed_projects = []
    successful_updates = 0
    no_changes_count = 0
    changed_fields = {}
    batch_size = 20
    retry_delay = 5
    
    for i in range(0, len(project_ids), batch_size):
        batch = project_ids[i:i + batch_size]
        print(f"\n📦 Processing batch {i//batch_size + 1} (projects {i+1}-{min(i+batch_size, len(project_ids))})...")
        
        for pid in batch:
            success, updated, changes = update_negotiation(pid)
            if success:
                if updated:
                    successful_updates += 1
                    changed_fields[pid] = changes
                else:
                    no_changes_count += 1
            else:
                failed_projects.append(pid)
        
        # Add delay between batches
        if i + batch_size < len(project_ids):
            print(f"⏳ Waiting {retry_delay} seconds before next batch...")
            time.sleep(retry_delay)
    
    # Retry failed projects once
    if failed_projects:
        print(f"\n🔄 Retrying {len(failed_projects)} failed projects...")
        retry_failed = []
        
        for pid in failed_projects:
            print(f"\n🔍 Retrying project {pid}")
            success, updated, changes = update_negotiation(pid)
            if success:
                if updated:
                    successful_updates += 1
                    changed_fields[pid] = changes
                else:
                    no_changes_count += 1
            else:
                retry_failed.append(pid)
    
    # Final report
    print("\n" + "="*50)
    print("📊 Update Summary:")
    print(f"✅ Successfully updated: {successful_updates} projects")
    print(f"➖ No changes needed: {no_changes_count} projects")
    print(f"❌ Failed to update: {len(retry_failed)} projects")
    
    # Print detailed changes
    if changed_fields:
        print("\n🔍 Field Changes Breakdown:")
        for pid, changes in changed_fields.items():
            print(f"\nProject {pid} changes:")
            for field, (old_val, new_val) in changes.items():
                print(f"   - {field}: {old_val} → {new_val}")
    
    if retry_failed:
        print("\nFailed project IDs:", retry_failed)

if __name__ == "__main__":
    main()