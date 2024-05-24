import pandas as pd
import sqlite3
import os
import azure.functions as func
import pyodbc
import logging

# Read the Excel file
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, '..', 'data', 'citiq112023.xlsx')
citiq_data = pd.read_excel(file_path)

# Get the path of the 'data' folder (one level above the 'scripts' folder)
data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def main():
    try:
        # Connect to your Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()

        # Iterate over the rows of transaction_data and insert each transaction into the Transactions table
        for _, row in citiq_data.iterrows():
            property_ref = row['property_ref']
            e_102023 = row['e_102023']
            w_102023 = row['w_102023']

            cursor.execute('''UPDATE Properties SET citiq_elec = ?, citiq_water = ? WHERE property_ref = ?''', (e_102023, w_102023, property_ref))

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        print("Citiq values imported successfully.")
    except Exception as e:
        logging.error('An error occurred: %s', str(e))
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)

