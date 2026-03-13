import orchestrator
from ingestion import epa_echo

# Primary EPA Weekly Export URL
EPA_ZIP_URL = "https://echo.epa.gov/files/echodownloads/echo_exporter.zip"
LOCAL_DATA_PATH = "data/epa_sample.csv"

def run_local_test():
    print("--- Starting Local Sync Test ---")
    
    # 1. Check if update is needed
    should_run, etag = orchestrator.should_run_sync('epa_us', EPA_ZIP_URL)
    
    if should_run:
        print("New data detected. Starting download...")
        try:
            # 2. Run Scraper
            epa_echo.sync_epa_data(EPA_ZIP_URL, LOCAL_DATA_PATH)
            
            # 3. Update Metadata
            orchestrator.update_sync_metadata('epa_us', etag)
            print("Test Complete: Local files updated.")
        except Exception as e:
            print(f"Scraper failed: {e}")
    else:
        print("Success: Orchestrator correctly skipped redundant download.")

if __name__ == "__main__":
    run_local_test()