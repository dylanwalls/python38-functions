import azure.functions as func
import pyodbc
import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the desired logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def generate_invoices(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Connect to your Azure SQL Database

        username = 'dylanwalls'
        password = '950117Dy!'
        server = 'scorecard-server.database.windows.net'
        database = 'dashboard-new-server'
        driver = '{ODBC Driver 17 for SQL Server}'
        
        # Log connection information
        logging.info(f"Connecting to database: {server}/{database}")
        
        # Connect to the Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        
        cursor = conn.cursor()

        # Get the current month and year
        current_month = datetime.datetime.now().month
        current_year = datetime.datetime.now().year

        # Query rental units and their corresponding properties
        cursor.execute('''
            SELECT RentalUnits.unit_id, RentalUnits.unit_ref, RentalUnits.rent, Properties.property_id
            FROM RentalUnits
            INNER JOIN Properties ON RentalUnits.property_id = Properties.property_id
        ''')
        rental_units = cursor.fetchall()

        # Log the number of rental units
        logging.info(f"Found {len(rental_units)} rental units")

        # Generate invoices for each rental unit
        invoices = []
        for unit_id, unit_ref, rent, property_id in rental_units:
            # Check if the invoice for the current month already exists
            cursor.execute('''
                SELECT invoice_id
                FROM Invoices
                WHERE unit_id = ? AND month = ? AND year = ?
            ''', (unit_id, current_month, current_year))
            existing_invoice = cursor.fetchone()

            if not existing_invoice:
                # Invoice doesn't exist, generate a new one
                invoice_type = 'rent'
                payout_status = 'Unpaid'
                invoice_payments = ''

                # Log invoice generation
                logging.info(f"Generating invoice for unit {unit_id}, month {current_month}, year {current_year}")

                # Insert the invoice data directly into the database
                cursor.execute('''
                    INSERT INTO Invoices (property_id, unit_id, unit_ref, month, year, invoice_type, amount_due, amount_paid, date_paid, transaction_ref, is_filled, payout_status, payout_date, invoice_payments)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (property_id, unit_id, unit_ref, current_month, current_year, invoice_type, rent, 0, None, None, 0, payout_status, None, invoice_payments))

            # Determine the next month and year
            if current_month == 12:
                next_month = 1
                next_year = current_year + 1
            else:
                next_month = current_month + 1
                next_year = current_year

            # Check if the invoice for the following month already exists
            cursor.execute('''
                SELECT invoice_id
                FROM Invoices
                WHERE unit_id = ? AND month = ? AND year = ?
            ''', (unit_id, next_month, next_year))
            existing_invoice = cursor.fetchone()

            if not existing_invoice:
                # Invoice doesn't exist, generate a new one
                invoice_type = 'rent'
                payout_status = 'Unpaid'
                invoice_payments = ''

                # Log invoice generation
                logging.info(f"Generating invoice for unit {unit_id}, month {next_month}, year {next_year}")

                # Insert the invoice data directly into the database
                cursor.execute('''
                    INSERT INTO Invoices (property_id, unit_id, unit_ref, month, year, invoice_type, amount_due, amount_paid, date_paid, transaction_ref, is_filled, payout_status, payout_date, invoice_payments)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (property_id, unit_id, unit_ref, next_month, next_year, invoice_type, rent, 0, None, None, 0, payout_status, None, invoice_payments))

            # Check if a deposit invoice already exists for the unit
            cursor.execute('''
                SELECT invoice_id
                FROM Invoices
                WHERE unit_id = ? AND invoice_type = ?
            ''', (unit_id, 'deposit'))
            existing_deposit_invoice = cursor.fetchone()

            if not existing_deposit_invoice:
                # Deposit invoice doesn't exist, generate a new one
                payout_status = 'Unpaid'
                invoice_payments = ''

                # Log deposit invoice generation
                logging.info(f"Generating deposit invoice for unit {unit_id}, month {current_month}, year {current_year}")

                # Insert the deposit invoice data directly into the database
                cursor.execute('''
                    INSERT INTO Invoices (property_id, unit_id, unit_ref, month, year, invoice_type, amount_due, amount_paid, date_paid, transaction_ref, is_filled, payout_status, payout_date, invoice_payments)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (property_id, unit_id, unit_ref, current_month, current_year, 'deposit', rent, 0, None, None, 0, payout_status, None, invoice_payments))

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        # Log success message
        logging.info("Invoices generated successfully.")

        return func.HttpResponse("Invoices generated successfully.", status_code=200)
    except Exception as e:
        # Log error message
        logging.error(f"An error occurred: {str(e)}")

        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
    
# Azure Functions entry point
def main(req: func.HttpRequest) -> func.HttpResponse:
    return generate_invoices(req)
