import orchestrator
from ingestion import epa_echo
from ingestion import gleif_daily
import os

# --- Configuration ---
# EPA ECHO: The comprehensive US environmental compliance dataset
EPA_ZIP_URL = "https://echo.epa.gov/files/echodownloads/echo_exporter.zip"
EPA_LOCAL_PATH = "data/epa_sample.csv"

# GLEIF: The global "Source of Truth" for corporate legal entities
# This URL points to the latest concatenated Level 1 (Entity) CSV
GLEIF_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"
GLEIF_LOCAL_PATH = "data/gleif_sample.csv"

def run_local_test():
    print("\n" + "="*40)
    print("      GREEN COMPLIANCE PIPELINE: LOCAL TEST")
    print("="*40)

    # Ensure data directory exists
    if not os.path.exists("data"):
        os.makedirs("data")

    # --- PHASE 1: EPA DATA SYNC ---
    print("\n[PHASE 1] Checking EPA ECHO Dataset...")
    should_run_epa, epa_etag = orchestrator.should_run_sync('epa_us', EPA_ZIP_URL)
    
    if should_run_epa:
        try:
            epa_echo.sync_epa_data(EPA_ZIP_URL, EPA_LOCAL_PATH)
            orchestrator.update_sync_metadata('epa_us', epa_etag)
        except Exception as e:
            print(f"FAILED: EPA Sync encountered an error: {e}")
    else:
        print("SKIP: EPA data is already up to date.")

    # --- PHASE 2: GLEIF DATA SYNC ---
    print("\n[PHASE 2] Checking GLEIF Global Directory...")
    # Note: GLEIF might use ETag or Last-Modified; orchestrator handles both
    should_run_gleif, gleif_etag = orchestrator.should_run_sync('gleif_global', GLEIF_URL)
    
    if should_run_gleif:
        try:
            gleif_daily.sync_gleif_data(GLEIF_URL, GLEIF_LOCAL_PATH)
            orchestrator.update_sync_metadata('gleif_global', gleif_etag)
        except Exception as e:
            print(f"FAILED: GLEIF Sync encountered an error: {e}")
    else:
        print("SKIP: GLEIF data is already up to date.")

    print("\n" + "="*40)
    print("      LOCAL TEST SEQUENCE COMPLETE")
    print("="*40)

if __name__ == "__main__":
    run_local_test()