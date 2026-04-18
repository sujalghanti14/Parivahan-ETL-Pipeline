from datetime import datetime
import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


def run_load(
    csv_path: str,
    table_name: str = "RTA_Maharashtra",
    batch_size: int = 500
):
    load_dotenv()

    supabase = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_KEY"],
    )

    df   = pd.read_csv(csv_path)
    df['batch_id'] = datetime.now().isoformat()

    # convert any datetime/Timestamp columns to ISO strings so they are JSON-serialisable
    for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    data = df.to_dict(orient="records")

    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        supabase.table(table_name).insert(batch).execute()
        print(f"Inserted {i} to {i + len(batch)}")

    print(f"Done — {len(data)} rows loaded into '{table_name}'.")


if __name__ == "__main__":
    run_load(
        csv_path=r"C:\Users\sujal\Documents\Major Project\Project on System\consolidated_fuel_data.csv"
    )