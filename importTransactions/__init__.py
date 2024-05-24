import azure.functions as func
import io
import openpyxl
import logging
import os
import pyodbc
from datetime import datetime, timedelta


username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.basicConfig(level=logging.INFO)

        # Check if 'excelFile' is in the files dictionary
        if 'excelFile' in req.files:
            # Get the uploaded file
            file = req.files['excelFile']

            # Read the uploaded file content as bytes
            file_content = file.read()

            # Create a BytesIO object to load the content into openpyxl
            file_content_io = io.BytesIO(file_content)

            # Load the Excel file from BytesIO
            workbook = openpyxl.load_workbook(file_content_io)

            # Assuming the data is in the first sheet of the Excel file
            worksheet = workbook.active

            # Connect to your Azure SQL Database
            conn = pyodbc.connect(
                f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
            )
            cursor = conn.cursor()

            # Iterate over the rows of the Excel sheet and insert each transaction into the database
            for row in worksheet.iter_rows(min_row=2, values_only=True):
                rrn = row[3]  # Assuming 'rrn' is in the fourth column (column D)

                # Check if the transaction already exists in the database
                cursor.execute('SELECT COUNT(*) FROM Transactions WHERE rrn = ?', (rrn,))
                existing_transaction_count = cursor.fetchone()[0]

                if existing_transaction_count == 0:
                    owner = row[1]  # Modify this to match the column containing owner data
                    unit_no = row[2]  # Modify this to match the column containing unit_no data
                    offer_ref = row[4]  # Modify this to match the column containing offer_ref data
                    client_reference = row[5]  # Modify this to match the column containing client_reference data
                    amount = row[6]  # Modify this to match the column containing amount data
                    deposit = row[7]  # Modify this to match the column containing deposit data
                    ref_no = row[8]  # Modify this to match the column containing ref_no data
                    status = row[9]  # Modify this to match the column containing status data
                    status_name = row[10]  # Modify this to match the column containing status_name data
                    status_date = str(row[11])  # Modify this to match the column containing status_date data
                    is_investor_transaction = row[12]  # Modify this to match the column containing is_investor_transaction data

                    # Calculate rent
                    rent = amount - deposit

                    # Set the 'month' and 'year' variables based on the transaction date
                    transaction_date = datetime.strptime(status_date.split("T")[0], "%Y-%m-%d")
                    if transaction_date.day <= 14:
                        month = transaction_date.month
                        year = transaction_date.year
                    else:
                        next_month_date = transaction_date + timedelta(days=17)
                        month = next_month_date.month
                        year = next_month_date.year

                    # Insert the transaction into the database
                    cursor.execute('''
                        INSERT INTO Transactions (owner, unit_no, rrn, offer_ref, client_reference, amount, deposit, ref_no, status, status_name, status_date, is_investor_transaction, rent, month, year, rent_reconciled, deposit_reconciled, is_reconciled)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (owner, unit_no, rrn, offer_ref, client_reference, amount, deposit, ref_no, status, status_name, status_date, is_investor_transaction, rent, month, year, 0, 0, 0))

            now = datetime.now()
            formatted_now = now.strftime('%Y-%m-%d %H:%M:%S')

            # Write the SQL query using the formatted timestamp
            query = f"UPDATE metadata SET meta_value = '{formatted_now}' WHERE meta_key = 'last_updated';"

            cursor.execute(query)
            
            # Commit the changes and close the connection
            conn.commit()
            conn.close()

            return func.HttpResponse("Excel file processed successfully.", status_code=200)
        else:
            # If 'excelFile' is not in the files dictionary, return a bad request response
            return func.HttpResponse("Missing 'excelFile' in request.", status_code=400)

    except Exception as e:
        logging.error('An error occurred: %s', str(e))
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)