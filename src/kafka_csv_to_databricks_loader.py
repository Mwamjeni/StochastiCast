"""
Playwright Automation Script for Uploading Kafka CSV to Databricks

This script uses Playwright for web automation to load stock data stored in a CSV file 
(from a Kafka consumer) into Databricks for further analysis. It interacts with the 
Databricks web interface to automate the upload process.

Reason for Using Playwright Automation:
    Databricks Community Edition does not support Kafka clusters, 
preventing direct stock data streaming from the Polygon API into a Delta database table. 
To work around this, the script automates the batch upload of CSV files via Playwright, 
ensuring smooth data transfer into Databricks for further analysis.

Modules:
    - playwright.sync_api: Handles browser automation.
    - time: Manages delays.
    - datetime: Tracks execution timestamps.
    - os: Handles file paths.
    - kafka_consumer: Fetches the generated CSV filename.

Functionality:
    - Logs into Databricks and navigates to the upload section.
    - Uploads the Kafka-generated CSV file.
    - Confirms successful processing.

Usage:
    - Install Playwright (`pip install playwright`).
    - Ensure the Kafka consumer has generated the CSV file.

Author: Charlene Mwamjeni
Date: 2025-06-05
"""

from playwright.sync_api import sync_playwright
import time
from datetime import datetime
import os
from kafka_consumer import kafka_csv_filename

# Databricks Community Edition URL
DATABRICKS_URL = "https://community.cloud.databricks.com"

# User credentials
USERNAME = "mwamjeni.charlene@gmail.com"
VERIFICATION_CODE = "" 
CSV_FILE_PATH = os.path.abspath(kafka_csv_filename)  #References the file output from the Kafka Consumer Script

def upload_csv_to_databricks():
    with sync_playwright() as p:
        # Launch Firefox browser
        browser = p.firefox.launch(headless=False, slow_mo=500)  # Set to True for silent automation
        page = browser.new_page()

        # Navigate to Databricks login page
        page.goto(DATABRICKS_URL)

        # Enter username
        page.fill("input[name='username']", USERNAME)
        page.click("button:has-text('Next')")  # Click Next to proceed

        # Wait for verification code input field
        page.wait_for_selector("input[name='verification_code']", timeout=30000)  # Wait for user to enter the code

        # ⚠️ PAUSE SCRIPT: User must manually enter verification code
        print("⏳ Please enter the verification code manually... Playwright will wait.")
        
        page.wait_for_url("https://community.cloud.databricks.com/?o=*", timeout=90000)

        time.sleep(10)
        print("Current URL:", page.url)
        print("Current page after navigation:", page.url)

        # Webpage Navigation
        page.locator("div.css-19idom button#radix-\:r2\:.css-1fvpjdk").highlight()
        page.locator("div.css-19idom button#radix-\:r2\:.css-1fvpjdk").click()        
        
        # Navigate to "Add or upload data"        
        page.locator("div#radix-\:r3\:.css-1opjza6 a.du-bois-light-typography.css-q4ps4h").highlight()
        page.locator("div#radix-\:r3\:.css-1opjza6 a.du-bois-light-typography.css-q4ps4h").click()

        #page.get_by_placeholder("(optional)").highlight()
        #dbfs_target_input = page.get_by_placeholder("(optional)")
        #dbfs_target_input.fill("stocks_fx_crypto_data")
             
        # Upload the Kafka CSV file
        page.locator("div#filePicker.dropzone.control-field.dz-clickable").highlight()
        csv_input = page.locator("div#filePicker.dropzone.control-field.dz-clickable")
        with page.expect_file_chooser() as fc_info:          
            csv_input.click()
            file_chooser = fc_info.value            
            file_chooser.set_files(CSV_FILE_PATH)
            
            # Confirm upload        
            print("✅ CSV file uploaded to Databricks successfully!")

        #Create Table using Notebook in Databricks
        page.locator("button.btn-default").highlight()
        page.locator("button.btn-default").click()

        browser.close()

upload_csv_to_databricks()