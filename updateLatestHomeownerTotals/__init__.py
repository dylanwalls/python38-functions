import azure.functions as func
import pyodbc
import logging
import json

# Define your Azure SQL Database connection parameters
username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'
invoice_type = 'rent'  # Define the invoice type, if needed

def update_latest_homeowner_total(month, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM PROPERTIES WHERE is_active = 1')
    properties = cursor.fetchall()


    for property_data in properties:
        property_id = property_data[0]
        property_ref = property_data[1]

        cursor.execute("SELECT * FROM Invoices WHERE property_id = ? AND payout_status IN ('Unpaid', 'Partially Paid') AND invoice_type = ? AND month <= ?", (property_id, invoice_type, month))
        invoices = cursor.fetchall()

        total_unpaid = 0
        total_partially_paid = 0

        for invoice in invoices:
            logging.info(f'invoice id: {invoice[1]}')
            if invoice[12] == 'Unpaid':
                total_unpaid += invoice[8]
            elif invoice[12] == 'Partially Paid':
                cursor.execute("""
                    SELECT SUM(amount) AS unpaidAmount
                    FROM InvoicePayments
                    WHERE invoice_id = ? AND included_in_payout = 0
                """, (invoice[0],))
                unpaid_result = cursor.fetchone()
                if unpaid_result and unpaid_result[0] is not None:
                    unpaid_amount = unpaid_result[0]
                else:
                    unpaid_amount = 0

                logging.info(f'unpaid amount: {unpaid_amount}')
                total_partially_paid += unpaid_amount

        total_amount = total_unpaid + total_partially_paid
        logging.info(f'total unpaid: {total_unpaid}')
        logging.info(f'total partially paid: {total_partially_paid}')
        logging.info(f'total result: {total_amount}')

        homeowner_rent = 0
        if property_ref == '26B':
            homeowner_rent = round(0.175 * total_amount, 2)
        elif property_ref == '59N':
            homeowner_rent = round(0.35 * total_amount, 2)
        else:
            homeowner_rent = round(0.15 * total_amount, 2)

        cursor.execute('UPDATE Properties SET latest_homeowner_total = ? WHERE property_ref = ?', (homeowner_rent, property_ref))
        logging.info(f'Property {property_ref} updated with new homeowner total: {homeowner_rent}')
    conn.commit()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Attempt to parse the 'month' parameter from the request
        month = req.params.get('month')
        if not month:  # Check if 'month' is not in the query string
            try:
                req_body = req.get_json()
                month = req_body.get('month')
            except ValueError:
                pass  # In case the request body does not contain valid JSON

        if month:
            month = int(month)  # Convert month to an integer
            logging.info(f'Month: {month}')

            conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
            conn = pyodbc.connect(conn_str)
            logging.info('Connected to database')

            update_latest_homeowner_total(month, conn)
            logging.info('Function completed')

            conn.close()
            return func.HttpResponse(f"Latest homeowner totals updated successfully for month {month}.")
        else:
            return func.HttpResponse("Please pass a month on the query string or in the request body", status_code=400)
    except Exception as e:
        logging.info(f'Error: {e}')
        return func.HttpResponse(
            f"An error occurred: {str(e)}",
            status_code=500
        )