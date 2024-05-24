import azure.functions as func
import logging
import pyodbc
import openpyxl
import io

username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def import_indlu_payout_list(excel_data):
    try:
        # Connect to your Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()

        # Load the uploaded Excel file from BytesIO
        workbook = openpyxl.load_workbook(io.BytesIO(excel_data))
        worksheet = workbook.active

        logging.info('Starting Excel file processing...')

        for row in worksheet.iter_rows(min_row=2, values_only=True):
            refNo = row[0]
            origin = row[1]
            reference = row[2]
            internalReference = row[3]
            referenceCode = row[4]
            referenceDescription = row[5]
            amount = row[6]
            externalReference = row[7]
            statusName = row[8]
            status = row[9]
            statusDate = row[10]
            bankAccInfo = row[11]
            log = row[12]
            comments = row[13]
            id = row[14]
            isDeleted = row[15]

            # Check if the property already exists in the indluPayoutList table
            cursor.execute('SELECT internalReference FROM indluPayoutList WHERE internalReference = ?', (internalReference,))
            existingPayment = cursor.fetchone()

            # If the payment does not exist, insert it into the indluPayoutList table
            if existingPayment is None:
                logging.info(f'No payment exists: {internalReference}')
                cursor.execute('''
                    INSERT INTO indluPayoutList (refNo, origin, reference, internalReference, referenceCode, referenceDescription, amount, externalReference, statusName, status, statusDate, bankAccInfo, log, comments, id, isDeleted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?)
                ''', (refNo, origin, reference, internalReference, referenceCode, referenceDescription, amount, externalReference, statusName, status, statusDate, bankAccInfo, log, comments, id, isDeleted))
                logging.info(f'Inserted payment: {internalReference}')

        # After importing, check indluPayoutSchedule for missing entries and insert if necessary
        cursor.execute('''
            SELECT DISTINCT externalReference
            FROM indluPayoutList
        ''')
        unique_references = cursor.fetchall()

        for ref in unique_references:
            logging.info(f'Unique batch payout ref found: {ref}')
            # Get the statusDate for the externalReference
            cursor.execute('''
                SELECT TOP 1 statusDate
                FROM indluPayoutList
                WHERE externalReference = ?
            ''', (ref[0],))
            status_date = cursor.fetchone()[0]

            cursor.execute('''
                SELECT COUNT(*)
                FROM indluPayoutSchedule
                WHERE payout_reference = ?
            ''', (ref[0],))
            existing_count = cursor.fetchone()[0]

            if existing_count == 0:
                # Insert a row into indluPayoutSchedule if the payout_reference does not exist
                cursor.execute('''
                    INSERT INTO indluPayoutSchedule (payout_reference, payout_date)
                    VALUES (?, ?)
                ''', (ref[0], status_date))
                logging.info(f'Inserted payout_reference: {ref[0]}')



        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        logging.info('Excel file processing completed successfully.')
        return "Payment data imported successfully."

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return f"An error occurred: {str(e)}"


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.basicConfig(level=logging.INFO)

        # Check if 'excelFile' is in the files dictionary
        if 'excelFile' in req.files:
            # Get the uploaded file
            file = req.files['excelFile']

            # Read the uploaded file content as bytes
            file_content = file.read()

            result = import_indlu_payout_list(file_content)

            return func.HttpResponse(result, status_code=200)
        else:
            # If 'excelFile' is not in the files dictionary, return a bad request response
            logging.error("Missing 'excelFile' in request.")
            return func.HttpResponse("Missing 'excelFile' in request.", status_code=400)

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
