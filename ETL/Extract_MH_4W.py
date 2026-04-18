from selenium import webdriver
from selenium.webdriver.common.by import By                         
from selenium.webdriver.support.ui import WebDriverWait             
from selenium.webdriver.support import expected_conditions as EC    
from playwright.sync_api import sync_playwright
import time      
import pandas as pd  
import os            
import re           


def run_extract(
    rto_list=None,
    months=None,
    download_path=r"C:\Users\sujal\Downloads\reportTable.xlsx",
    output_folder=r"C:\Users\sujal\Documents\Major Project\Project on System\Data"
):
  
    if rto_list is None:
        rto_list = [




                    "AKLUJ - MH45( 03-APR-2017 )",
                    "AMBEJOGAI - MH44( 02-MAY-2017 )",
                    "AMRAWATI - MH27( 21-JAN-2017 )",
                    "BARAMATI - MH42( 10-MAR-2017 )",
                    "BEED - MH23( 17-MAR-2017 )",
                    "BHADGAON - MH54( 20-MAR-2024 )",
                    "BHANDARA - MH36( 12-APR-2017 )",
                    "BULDHANA - MH28( 07-NOV-2017 )",
                    "CHALISGAON - MH52( 05-MAR-2024 )",
                    "CHHATRAPATI SAMBHAJINAGAR - MH20( 20-OCT-2016 )",
            "Chiplun Chiplun Track - MH202( 04-DEC-2019 )",
            "DHARASHIV - MH25( 31-OCT-2017 )",
            "DHULE - MH18( 03-JAN-2017 )",
            "DY REGIONAL TRANSPORT OFFICE, HINGOLI - MH38( 15-JUL-2017 )",
            "DY RTO RATNAGIRI - MH8( 10-APR-2017 )",
            "GADCHIROLI - MH33( 18-APR-2017 )",
            "GONDHIA - MH35( 11-APR-2017 )",
            "ICHALKARANJI - MH51( 07-MAR-2024 )",
            "JALANA - MH21( 03-AUG-2017 )",
            "KALYAN - MH5( 11-MAY-2017 )",
                    "KARAD - MH50( 20-MAR-2017 )",
                    "KHAMGAON - MH56( 15-APR-2025 )",
                    "KOLHAPUR - MH9( 02-MAR-2017 )",
                    "MALEGAON - MH41( 23-AUG-2017 )",
                    "MIRA BHAYANDAR - MH58( 07-MAY-2025 )",
                    "MUMBAI (CENTRAL) - MH1( 15-DEC-2016 )",
                    "MUMBAI (EAST) - MH3( 13-DEC-2016 )",
                    "MUMBAI (WEST) - MH2( 21-APR-2017 )",
                    "NAGPUR (EAST) - MH49( 17-APR-2017 )",
                    "NAGPUR (RURAL) - MH40( 17-JAN-2017 )",
            "NAGPUR (U) - MH31( 18-JAN-2017 )",
            "NANDED - MH26( 12-JAN-2017 )",
            "NANDURBAR - MH39( 02-MAY-2017 )",
            "NASHIK - MH15( 05-JAN-2017 )",
            "PANVEL - MH46( 31-JAN-2017 )",
            "PARBHANI - MH22( 25-APR-2017 )",
            "PEN (RAIGAD) - MH6( 16-MAY-2017 )",
            "PHALTAN - MH53( 03-SEP-2024 )",
            "PUNE - MH12( 25-JAN-2017 )",
            "RTO AHILYANAGAR - MH16( 16-MAR-2017 )",
                    "RTO AKOLA - MH30( 20-FEB-2017 )",
                    "R.T.O.BORIVALI - MH47( 21-APR-2017 )",
                    "RTO CHANDRAPUR - MH34( 25-APR-2017 )",
                    "RTO JALGAON - MH19( 24-MAR-2017 )",
                    "RTO LATUR - MH24( 15-MAR-2017 )",
                    "RTO MH04-Mira Bhayander FitnessTrack - MH203( 01-MAY-2022 )",
                    "RTO PIMPRI CHINCHWAD - MH14( 06-FEB-2017 )",
                    "RTO SATARA - MH11( 04-MAR-2017 )",
                    "RTO SOLAPUR - MH13( 05-APR-2017 )",
                    "SANGLI - MH10( 03-MAR-2017 )",
            "SINDHUDURG(KUDAL) - MH7( 10-APR-2017 )",
            "SRIRAMPUR - MH17( 22-MAR-2017 )",
            "TC OFFICE - MH99( 06-JUN-2018 )",
            "THANE - MH4( 08-MAR-2017 )",
            "UDGIR - MH55( 28-AUG-2024 )",
            "VAIJAPUR - MH57( 06-JUN-2025 )",
            "VASAI - MH48( 08-JUN-2017 )",
            "VASHI (NEW MUMBAI) - MH43( 07-JUL-2016 )",
            "WARDHA - MH32( 06-APR-2017 )",
            "WASHIM - MH37( 11-APR-2017 )",
                    "YAWATMAL - MH29( 07-JUL-2017 )",
        ]

    # DEFAULT: months to download data for
    if months is None:
        months = ["JAN", "FEB", "MAR"]

    # PHASE 1: Use Playwright to read dynamic element IDs from the page.
    print("Phase 1: Detecting dynamic element IDs using Playwright...")

    with sync_playwright() as p:
        # Launch a visible Chrome browser (headless=False = you can see it)
        browser = p.chromium.launch(headless=False)
        page    = browser.new_page()

        # Set a 30-second timeout for all page interactions
        page.set_default_timeout(30000)

        # Navigate to the Vahan dashboard report page
        page.goto("https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml")

        # Wait for the page's main content to finish loading
        page.wait_for_load_state("domcontentloaded")

        # Find all dropdown elements on the page (they use this CSS class)
        dropdowns = page.locator(".ui-selectonemenu.ui-widget")

        # Find all button elements on the page (they use this CSS class)
        buttons = page.locator(".ui-button.ui-widget")

        # Read the HTML "id" attribute of specific dropdowns and buttons
        # These IDs are needed by Selenium to click on the correct elements
        State_dropdown_id           = dropdowns.nth(1).get_attribute("id")  # The state selector dropdown
        refresh_button_id           = buttons.nth(0).get_attribute("id")    # The main refresh button
        vehicle_category_refresh_id = buttons.nth(1).get_attribute("id")    # The vehicle category refresh button

        browser.close()

    print(f"  State dropdown ID       : {State_dropdown_id}")
    print(f"  Refresh button ID       : {refresh_button_id}")
    print(f"  Category refresh ID     : {vehicle_category_refresh_id}")

    # PHASE 2: Use Selenium to actually interact with the page and
    # download the Excel files.
    print("\nPhase 2: Starting Selenium browser for downloads...")

    # Open a Chrome browser window controlled by Selenium
    driver = webdriver.Chrome()

    # Navigate to the Vahan dashboard
    driver.get("https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml")

    # Create a "wait" object that will wait up to 30 seconds for elements to appear
    wait = WebDriverWait(driver, 30)

    # ----------------------------------------------------------
    # Inner helper: wait until the loading spinner disappears.
    # The Vahan site shows a "ui-blockui" overlay while it loads data.
    # We must wait for it to vanish before clicking anything else.
    # ----------------------------------------------------------
    def wait_for_loading():
        try:
            # Wait until the blocking overlay is no longer visible
            wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "ui-blockui")))
        except:
            pass  # If no overlay appeared, that's fine — just continue

    # ----------------------------------------------------------
    # Inner helper: click the Excel download button safely.
    # First waits for loading to finish, then clicks the download button.
    # ----------------------------------------------------------
    def safe_click_download():
        wait_for_loading()
        # Wait for the download button to become clickable, then click it
        btn = wait.until(EC.element_to_be_clickable((By.ID, "groupingTable:xls")))
        btn.click()

    # Click the state dropdown to open it
    dropdown_state = wait.until(EC.element_to_be_clickable((By.ID, State_dropdown_id)))
    dropdown_state.click()
    time.sleep(1)  # Give the dropdown a moment to open

    # Find and click the "Maharashtra(59)" option in the dropdown list
    state_option = wait.until(EC.element_to_be_clickable((By.XPATH, "//li[contains(text(), 'Maharashtra(59)')]")))
    state_option.click()
    time.sleep(2)  # Wait for the page to reload after state selection

    # ----------------------------------------------------------
    # OPEN VEHICLE FILTER PANEL
    # ----------------------------------------------------------

    # Click the toggle button to expand the vehicle filter options
    vehicle_filter_button = wait.until(EC.element_to_be_clickable((By.ID, "filterLayout-toggler")))
    vehicle_filter_button.click()
    time.sleep(1)

    # ----------------------------------------------------------
    # SET Y-AXIS = MAKER (Brand name goes on the vertical axis)
    # ----------------------------------------------------------

    # Click the Y-Axis dropdown to open it
    dropdown_y_axis = wait.until(EC.element_to_be_clickable((By.ID, "yaxisVar_label")))
    dropdown_y_axis.click()
    time.sleep(1)

    # Select the "Maker" option from the Y-Axis dropdown
    y_axis_option = wait.until(EC.element_to_be_clickable((By.XPATH, "//li[contains(., 'Maker')]")))
    y_axis_option.click()
    time.sleep(2)

    # ----------------------------------------------------------
    # SET X-AXIS = FUEL (Fuel type goes on the horizontal axis)
    # ----------------------------------------------------------

    # Click the X-Axis dropdown to open it
    dropdown_x_axis = wait.until(EC.element_to_be_clickable((By.ID, "xaxisVar_label")))
    dropdown_x_axis.click()
    time.sleep(1)

    # Select the "Fuel" option (element with ID "xaxisVar_3")
    fuel_option = wait.until(EC.element_to_be_clickable((By.ID, "xaxisVar_3")))
    fuel_option.click()
    time.sleep(2)

    # ----------------------------------------------------------
    # MAIN LOOP: Iterate over every RTO and every month
    # ----------------------------------------------------------

    print(f"\nStarting downloads: {len(rto_list)} RTO(s) × {len(months)} month(s) = {len(rto_list) * len(months)} file(s)\n")

    # Loop through each RTO in the list
    for rto_index, rto_name in enumerate(rto_list, 1):

        print(f"[RTO {rto_index}/{len(rto_list)}] {rto_name}")

        # Click the RTO dropdown to open it
        dropdown_rta = wait.until(EC.element_to_be_clickable((By.ID, "selectedRto")))
        dropdown_rta.click()
        time.sleep(1)

        # Find the matching option in the RTO dropdown and click it
        # contains(., '...') matches the option that contains the rto_name text
        rto_option = wait.until(EC.element_to_be_clickable((By.XPATH, f"//li[contains(., '{rto_name}')]")))

        # Scroll the option into view (in case it's below the visible area of the dropdown)
        driver.execute_script("arguments[0].scrollIntoView(true);", rto_option)
        rto_option.click()
        time.sleep(2)

        # Click the main Refresh button to load data for the selected RTO
        refresh_button = wait.until(EC.element_to_be_clickable((By.ID, refresh_button_id)))
        refresh_button.click()
        wait_for_loading()  # Wait for the page to finish refreshing

        # ── Select vehicle categories (checkboxes) ──────────

        # These checkboxes filter the report to show only 4-wheeler passenger vehicles.
        # We select 6 categories that cover all relevant 4W vehicle types.

        # Check "MOTOR CAR" (standard private cars)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//label[normalize-space()='MOTOR CAR']"))).click()
        wait_for_loading()

        # Check "MAXI CAB" (large taxis/vans)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//label[normalize-space()='MAXI CAB']"))).click()
        wait_for_loading()

        # Check "LUXURY CAB" (luxury taxis)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//label[normalize-space()='LUXURY CAB']"))).click()
        wait_for_loading()

        # Check "MOTOR CAB" (standard taxis)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//label[normalize-space()='MOTOR CAB']"))).click()
        wait_for_loading()

        # Check "VINTAGE MOTOR VEHICLE" (classic/antique vehicles)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//label[normalize-space()='VINTAGE MOTOR VEHICLE']"))).click()
        wait_for_loading()

        # Check "LIGHT MOTOR VEHICLE" (LMVs — includes cars and small vans)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//label[normalize-space()='LIGHT MOTOR VEHICLE']"))).click()
        wait_for_loading()

        # ── Inner loop: download one file per month ──────────

        for month in months:

            print(f"  Downloading: {rto_name} — {month}")

            # Open the month selection dropdown
            month_dropdown = wait.until(EC.element_to_be_clickable((By.ID, "groupingTable:selectMonth_label")))
            month_dropdown.click()
            time.sleep(1)

            # Click the option that exactly matches the month abbreviation (e.g. "FEB")
            Month_option = wait.until(EC.element_to_be_clickable((By.XPATH, f"//li[normalize-space()='{month}']")))
            Month_option.click()
            time.sleep(1)

            # Click the vehicle category refresh button to reload the table for this month
            refresh_button = wait.until(EC.element_to_be_clickable((By.ID, vehicle_category_refresh_id)))
            refresh_button.click()
            wait_for_loading()  # Wait for the table to reload

            # Click the Excel download button — this saves "reportTable.xlsx" to Downloads
            safe_click_download()
            print(f"    ✅ Download triggered for {rto_name} / {month}")

            # Wait for the file to finish downloading before reading it
            time.sleep(5)

            # ── Read the downloaded file and rename it ──────

            # Read just the first row of the downloaded Excel to get the report title
            # The title contains the RTO name and period, e.g.:
            #   "Maker Wise Fuel Data  of AKLUJ - MH45 , Maharashtra (FEB,2026)"
            raw   = pd.read_excel(download_path, header=None, nrows=1)
            title = str(raw.iloc[0, 0])

            print(f"    Title: {title}")

            # Remove characters that are not allowed in Windows file names:
            # \ / * ? : " < > |  are all illegal in file names
            safe_title = re.sub(r'[\\/*?:"<>|]', "", title)

            # Build the full destination path: Data/<safe_title>.xlsx
            dst = os.path.join(output_folder, f"{safe_title}.xlsx")

            # Move and rename the downloaded file from Downloads to the Data folder
            os.rename(download_path, dst)
            print(f"    Saved → {dst}")

    # ----------------------------------------------------------
    # CLEANUP: Wait for any remaining downloads, then close the browser
    # ----------------------------------------------------------
    time.sleep(5)   # Extra buffer in case the last download is still finishing
    driver.quit()   # Close the Selenium-controlled browser window

    print(f"\n✅ Extract complete. All files saved to: {output_folder}")


if __name__ == "__main__":
    run_extract()
