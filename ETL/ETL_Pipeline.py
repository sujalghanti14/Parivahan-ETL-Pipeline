from Extract_MH_4W import run_extract
from Transform import batch_transform
from load import run_load

def run_pipeline():
    print("🚀 Starting ETL Pipeline...\n")

    print("📥 Running Extract...")
    for attempt in range(1, 4):
        try:
            run_extract()
            break
        except (Exception, KeyboardInterrupt) as e:
            print(f"Extract attempt {attempt} failed: {type(e).__name__}: {e}")
            if attempt == 3:
                print("All 3 attempts failed. Exiting.")
                raise
            print(f"Retrying ({attempt}/3)...")

    print("\n🔄 Running Transform...")
    batch_transform(
        input_folder=r"C:\Users\sujal\Documents\Major Project\Project on System\Data",
        output_file="consolidated_fuel_data.csv"
    )

    print("\n📤 Running Load...")
    run_load("consolidated_fuel_data.csv")

    print("\n✅ Pipeline Completed Successfully!")

if __name__ == "__main__":
    run_pipeline()
