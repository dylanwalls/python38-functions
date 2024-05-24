import azure.functions as func
import io
import csv
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

        # Accessing form data
        month = req.form.get('month')
        year = req.form.get('year')
        monthYear = f'{month}{year}'
        # property_ref = req.form.get('propertyRef')

        logging.info(f'Month: {month}, Year: {year}')

        # Check if files are present in the request
        if req.files:
            # Connect to your Azure SQL Database
            conn = pyodbc.connect(
                f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
            )
            cursor = conn.cursor()

            # Iterate over each uploaded file
            for file_key in req.files:
                logging.info(f'file key: {file_key}')
                file = req.files[file_key]
                file_content = file.read()
                file_content_io = io.StringIO(file_content.decode('utf-8'))
                reader = csv.reader(file_content_io)

                # Skip the header row (if present) and iterate over rows
                next(reader, None)  # Skip header
                for row in reader:
                   
                    Meter = row[3]

                    # Check if a row with the same meter number, month, year, and property_ref already exists
                    cursor.execute('''
                        SELECT COUNT(*) 
                        FROM remitMeter 
                        WHERE Meter = ? AND monthYear = ?
                    ''', (Meter, monthYear))
                    row_count = cursor.fetchone()[0]

                    if row_count == 0:
                        logging.info(f'ROW: {row}')
                        Building = row[0]
                        Unit = row[1]
                        Description = row[2]
                        UoM = row[4]
                        amount = float(row[5])

                        cursor.execute('''
                            INSERT INTO remitMeter
                            (Building, Unit, Description, Meter, UoM, amount, monthYear)
                            VALUES
                            (?, ?, ?, ?, ?, ?, ?)
                        ''',
                        (Building, Unit, Description, Meter, UoM, amount, monthYear))

            # Commit the changes and close the connection
            conn.commit()
            conn.close()

            return func.HttpResponse("CSV files processed successfully.", status_code=200)
        else:
            # If no files are present in the request, return a bad request response
            return func.HttpResponse("No files found in request.", status_code=400)

    except Exception as e:
        logging.error('An error occurred: %s', str(e))
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
