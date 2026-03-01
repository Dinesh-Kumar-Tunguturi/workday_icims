import os
import csv
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CSV_FILE = "us_jobs_recent.csv"

def upload():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL or SUPABASE_KEY missing in environment variables.")
        exit(1)

    if not os.path.exists(CSV_FILE):
        print(f"WARNING: '{CSV_FILE}' not found. No jobs to upload.")
        exit(0)

    print("Initializing Supabase client...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"ERROR: Failed to initialize Supabase client: {e}")
        exit(1)

    # Read CSV Data
    try:
        with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"ERROR: Failed to read CSV: {e}")
        exit(1)

    if not rows:
        print("INFO: CSV is empty. No jobs to upload.")
        exit(0)

    # Prepare data payload
    data = []
    for r in rows:
        job_url = r.get("job_url", "").strip()
        if not job_url:
            continue
            
        data.append({
            "domain": r.get("domain", ""),
            "role_searched": r.get("role_searched", ""),
            "source_ats": r.get("source_ats", ""),
            "company": r.get("company", ""),
            "job_id": r.get("job_id", ""),
            "title": r.get("title", ""),
            "job_url": job_url,
            "description": r.get("description", ""),
            "location": r.get("location", ""),
            "city": r.get("city", ""),
            "state": r.get("state", ""),
            "country": r.get("country", ""),
            "department": r.get("department", ""),
            "job_category": r.get("job_category", ""),
            "employment_type": r.get("employment_type", ""),
            "time_type": r.get("time_type", ""),
            "posted_date": r.get("posted_date", ""),
            "salary": r.get("salary", ""),
            "experience_level": r.get("experience_level", ""),
            "company_logo": r.get("company_logo", ""),
            "remote_eligible": r.get("remote_eligible", "")
        })

    if not data:
        print("INFO: No valid records found in CSV to insert.")
        exit(0)

    # Supabase bulk insert limit is typically 1000, we'll use chunks of 100 to be safe
    chunk_size = 100
    total_successful = 0
    
    print(f"Attempting to push {len(data)} records to 'workday_icims' table...")
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        try:
            # Upsert using job_url as the unique conflict column
            # If the job_url already exists, it updates the row rather than creating duplicates
            supabase.table("workday_icims").upsert(
                chunk, 
                on_conflict="job_url"
            ).execute()
            
            total_successful += len(chunk)
            print(f"Pushed chunk {i // chunk_size + 1} ({len(chunk)} records)")
        except Exception as e:
            print(f"ERROR: Failed to upload chunk {i // chunk_size + 1}: {e}")

    print(f"SUCCESS: Successfully processed and pushed {total_successful} out of {len(data)} records to Supabase.")

if __name__ == "__main__":
    upload()
