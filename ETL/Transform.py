import re  
import glob  
import os  
import pandas as pd  


# ============================================================
# FUNCTION 1: convert_fuel_report
# ------------------------------------------------------------
# This function takes ONE Excel file and converts it from a
# wide matrix layout into a clean, long tabular format (rows),
# then saves it as a CSV file.
# ============================================================

def convert_fuel_report(input_file: str, output_file: str = None):
    """
    Converts a Maker Wise Fuel Data Excel report (matrix format)
    into a long tabular format with columns: title, rto, brand, fuel, count, month, year.

    Args:
        input_file:  Path to the input .xlsx file
        output_file: Where to save the output CSV.
                     If None  → saves next to the Excel file.
                     If False → skip saving (used when processing multiple files in batch).
    """

    # ----------------------------------------------------------
    # STEP 1: Read ONLY the very first row of the Excel file.
    raw = pd.read_excel(input_file, header=None, nrows=1)
    title = str(raw.iloc[0, 0])

    # ----------------------------------------------------------
    # STEP 2: Pull out the RTO name from the title text.
    rto_match = re.search(r'\bof\s+(.+?)\s*,\s*Maharashtra', title, re.IGNORECASE)

    if rto_match:
        rto = rto_match.group(1).strip()
    else:
        # If the pattern wasn't found at all, use "UNKNOWN" as a safe fallback
        rto = "UNKNOWN"

    # Print the detected RTO name so we can see it while the script runs
    print(f"Detected RTO: {rto}")

    # ----------------------------------------------------------
    # STEP 3: Read the actual DATA from the Excel file.
    df = pd.read_excel(input_file, skiprows=3, header=0)

    # ----------------------------------------------------------
    # STEP 4: Clean up the column names.
    # Sometimes Excel columns have invisible "non-breaking spaces" (\xa0).
    # We strip those out so column names are clean plain text.
    # ----------------------------------------------------------

    # Loop through every column name, convert to string,
    # replace any invisible \xa0 space with nothing, and strip normal spaces
    df.columns = [str(c).replace("\xa0", "").strip() for c in df.columns]

    # ----------------------------------------------------------
    # STEP 5: Rename and remove unnecessary columns.
    # The brand column comes in with an auto-generated ugly name "Unnamed: 1".
    # We rename it to "brand".
    # Any other columns with "Unnamed" in their name are junk → delete them.
    # ----------------------------------------------------------

    # Rename the column called "Unnamed: 1" to "brand"
    df = df.rename(columns={"Unnamed: 1": "brand"})

    # Drop all other columns whose name starts with "Unnamed"
    # errors="ignore" means: don't crash if none exist
    df = df.drop(
        columns=[c for c in df.columns if c.startswith("Unnamed")],
        errors="ignore"
    )

    # ----------------------------------------------------------
    # STEP 6: Remove blank rows.
    # Some rows have no brand name (they are totals or spacer rows).
    # We keep only rows where the brand column has an actual value.
    # ----------------------------------------------------------

    # .notna() → True if the cell has a value, False if it's empty/blank
    # reset_index(drop=True) → renumber rows from 0 after filtering
    df = df[df["brand"].notna()].reset_index(drop=True)

    # ----------------------------------------------------------
    # STEP 7: Reshape the table from WIDE to LONG format.
    # This is called "melting" — turning column headers into row values.
    # ----------------------------------------------------------

    # Collect all column names except "brand" — these are the fuel type columns
    fuel_cols = [c for c in df.columns if c != "brand"]

    # Melt (unpivot) the DataFrame:
    # → id_vars="brand"      → keep brand as a fixed column
    # → value_vars=fuel_cols → turn each fuel column into a row
    # → var_name="fuel"      → the column header becomes a value in a column called "fuel"
    # → value_name="count"   → the number in each cell goes into a column called "count"
    long_df = df.melt(
        id_vars="brand",
        value_vars=fuel_cols,
        var_name="fuel",
        value_name="count"
    )

    # ----------------------------------------------------------
    # STEP 8: Make sure the "count" column contains whole numbers.
    # Some cells might be empty or text — we convert them safely.
    # ----------------------------------------------------------

    long_df["count"] = (
        # Try to convert each value to a number; if it fails, replace with NaN (not a number)
        pd.to_numeric(long_df["count"], errors="coerce")
        # Replace any NaN (blank/failed conversion) with 0
        .fillna(0)
        # Convert all values to plain integers (no decimals)
        .astype(int)
    )

    # ----------------------------------------------------------
    # STEP 9: Extract the MONTH and YEAR from the title.
    # The title ends with something like: (FEB,2026) or (MAR, 2026)
    # We use a regex pattern to find and extract these values.
    # ----------------------------------------------------------

    # Start with default values in case the pattern isn't found
    month, year = "UNKNOWN", "UNKNOWN"

    # Search for a pattern like (FEB,2026) anywhere in the title text
    # Pattern breakdown:
    #   \(          → literal opening bracket "("
    #   ([A-Za-z]+) → one or more letters (the month abbreviation, e.g. FEB)
    #   [,\s]+      → a comma or space separating month and year
    #   (\d{4})     → exactly 4 digits (the year, e.g. 2026)
    #   \)          → literal closing bracket ")"
    match = re.search(r"\(([A-Za-z]+)[,\s]+(\d{4})\)", title)

    if match:
        # .group(1) → the first captured group = month abbreviation → make it UPPERCASE
        month = match.group(1).upper()
        # .group(2) → the second captured group = year as a string
        year  = match.group(2)

    # Print what was detected so we can verify while running
    print(f"Detected Month: {month}, Year: {year}")

    # ----------------------------------------------------------
    # STEP 10: Add extra columns to the data.
    # We add "title", "rto", "month", and "year" so every row
    # knows which file, which office, and which time period it came from.
    # ----------------------------------------------------------

    # Insert "rto" as the first column (position 0) in the table
    long_df.insert(0, "rto", rto)

    # Insert "title" as the very first column (pushes rto to position 1)
    long_df.insert(0, "title", title)

    # Add "month" as a new column at the end — same value for all rows in this file
    long_df["month"] = month

    # Add "year" as a new column at the end — same value for all rows in this file
    long_df["year"]  = year

    # ----------------------------------------------------------
    # STEP 11: Remove rows where count is 0.
    # If a brand sold 0 vehicles in a fuel type, that row is useless.
    # We keep only rows where count > 0 (at least 1 vehicle was registered).
    # ----------------------------------------------------------

    # Filter rows: keep only where count is greater than 0
    # reset_index(drop=True) → renumber rows cleanly after filtering
    long_df = long_df[long_df["count"] > 0].reset_index(drop=True)

    # ----------------------------------------------------------
    # STEP 12: Save the cleaned data to a CSV file (if requested).
    # In batch mode (processing many files), saving is SKIPPED here
    # and done once at the end for all files together.
    # ----------------------------------------------------------

    # output_file=False means "skip saving" (batch mode)
    if output_file is not False:

        # If no output path was given, create one next to the original Excel file
        if output_file is None:
            output_file = input_file.replace(".xlsx", "_tabular.csv")

        # Write the cleaned DataFrame to a CSV file (without row numbers)
        long_df.to_csv(output_file, index=False)

        # Print a confirmation message showing how many rows were saved and where
        print(f"Saved {len(long_df)} rows → {output_file}")

    # Return the cleaned DataFrame so it can be used by the batch function
    return long_df


# ============================================================
# FUNCTION 2: batch_transform
# ------------------------------------------------------------
# This function processes ALL Excel files inside a given folder.
# It calls convert_fuel_report() for each file, collects all
# the results, and merges them into one big CSV file.
# ============================================================

def batch_transform(input_folder: str, output_file: str = "consolidated_fuel_data.csv"):
    """
    Processes ALL .xlsx files inside `input_folder` and writes a single
    consolidated CSV to `output_file`.

    Args:
        input_folder: Path to the folder containing .xlsx files.
        output_file:  Path for the merged output CSV.
    """

    # Build a search pattern like: "Data/*.xlsx"
    # This tells the computer: "find all Excel files inside the given folder"
    pattern = os.path.join(input_folder, "*.xlsx")

    # glob.glob() returns a list of all file paths that match the pattern
    # sorted() arranges them alphabetically so processing is orderly
    files = sorted(glob.glob(pattern))

    # If no Excel files were found in the folder, warn the user and stop
    if not files:
        print(f"[WARN] No .xlsx files found in: {input_folder}")
        return

    # Print how many files were found before we start processing
    print(f"Found {len(files)} file(s) — starting batch transform...\n")

    # 'frames' will collect the cleaned table from each processed file
    frames = []

    # 'errors' will collect info about any files that failed to process
    errors = []

    # Loop through every file one by one
    # enumerate(files, 1) gives us both the count (i) and the file path (fp)
    for i, fp in enumerate(files, 1):

        # Get just the file name (without the full folder path) for display
        fname = os.path.basename(fp)

        # Print progress: e.g. "[3/72] Processing: some_file.xlsx"
        print(f"[{i}/{len(files)}] Processing: {fname}")

        try:
            # Call the single-file function on this file
            # output_file=False → don't save a per-file CSV, just return the data
            df = convert_fuel_report(fp, output_file=False)

            # Add the resulting table to our collection list
            frames.append(df)

        except Exception as e:
            # If anything goes wrong with this file, print the error and keep going
            # (we don't want one bad file to stop all the others)
            print(f"  [ERROR] {fname}: {e}")

            # Save the error info so we can report it at the end
            errors.append((fname, str(e)))

    # If no data was collected at all (all files failed), stop here
    if not frames:
        print("No data extracted. Exiting.")
        return

    # Merge all individual tables into one big table
    # pd.concat() stacks multiple DataFrames on top of each other (like stacking spreadsheets)
    # ignore_index=True → renumber all rows from 0 in the merged table
    consolidated = pd.concat(frames, ignore_index=True)
   


    # Save the merged table as a single CSV file
    consolidated.to_csv(output_file, index=False)

    # ----------------------------------------------------------
    # Print a final summary of what was done
    # ----------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Batch complete!")
    print(f"  Files processed  : {len(files)}")       # Total files attempted
    print(f"  Files with errors: {len(errors)}")      # Files that had problems
    print(f"  Total rows       : {len(consolidated)}") # Total data rows in output
    print(f"  Output saved to  : {output_file}")       # Where the result was saved

    # If there were any errors, list the failed files and their error messages
    if errors:
        print("\nFailed files:")
        for fname, err in errors:
            print(f"  - {fname}: {err}")

    print('='*60)

    # Return the merged table in case the caller wants to use it further
    return consolidated


# ============================================================
# ENTRY POINT
# ------------------------------------------------------------
# This block runs only when you execute this script directly
# (e.g. python Transform.py).
# It calls batch_transform() pointing at the "Data" folder
# and saves everything into one CSV file.
# ============================================================

if __name__ == "__main__":
    batch_transform(
        input_folder=r"C:\Users\sujal\Documents\Major Project\Project on System\Data_25",                        # Folder containing all .xlsx files
        output_file="consolidated_fuel_data.csv"     # Name of the final merged output file
    )