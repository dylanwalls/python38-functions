import azure.functions as func
import pyodbc
import logging
from datetime import datetime, timedelta

# Define your Azure SQL Database connection parameters
username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def update_rental_units_status():
    try:
        # Create a connection to Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # For each row in RentalUnits
        cursor.execute("SELECT unit_id, deposit_due FROM RentalUnits")
        rental_units = cursor.fetchall()

        for unit in rental_units:
            unit_id, deposit_due = unit
            logging.info(f'unit_id: {unit_id}, deposit due: {deposit_due}')

            # Calculate the sum of the amount for this unit
            cursor.execute("SELECT SUM(amount), MAX(start_date) FROM Deposits WHERE unit_id=? AND is_active=1", unit_id)
            sum_amount, latest_start_date = cursor.fetchone()
            logging.info(f'sum_amount: {sum_amount}, latest_start_date: {latest_start_date}')
            # Handle the case where sum_amount is None
            if sum_amount is None:
                logging.info('sum is None')
                sum_amount = 0.0

            # Decide the status value based on the conditions
            if sum_amount == 0:
                if deposit_due == 0:
                    status = 'Paid'
                else:
                    status = 'Overdue'
            elif deposit_due <= sum_amount <= deposit_due + 10:
                logging.info('Paid')
                status = 'Paid'
            elif sum_amount > deposit_due + 10:
                logging.info('Overpaid')
                status = 'Overpaid'
            else:
                difference = (datetime.now().date() - latest_start_date).days
                if difference < 30:
                    logging.info('difference < 30, Partially Paid')
                    status = 'Partially Paid'
                else:
                    logging.info('difference > 30, Overdue')
                    status = 'Overdue'

            # Update the status in the RentalUnits table
            cursor.execute("UPDATE RentalUnits SET status=? WHERE unit_id=?", status, unit_id)

        # Commit the transaction
        conn.commit()
        conn.close()

        logging.info('Status update completed successfully.')
        return "Status update completed successfully."

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return f"An error occurred: {str(e)}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        message = update_rental_units_status()
        return func.HttpResponse(message, status_code=200)

    except Exception as e:
        import traceback
        logging.error(f'An error occurred: {str(e)}\n{traceback.format_exc()}')
        return func.HttpResponse(f"An error occurred: {str(e)}\n{traceback.format_exc()}", status_code=500)
