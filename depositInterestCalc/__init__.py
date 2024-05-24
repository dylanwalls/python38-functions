import azure.functions as func
import logging
import pyodbc
from datetime import datetime
from calendar import monthrange

username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def process_deposit_data():
    try:
        logging.info('script started')

        def days_until_end_of_month(date):
            # logging.info(f'date (in days until end of month): {date}')
            _, last_day = monthrange(date.year, date.month)
            end_of_month = datetime(date.year, date.month, last_day).date()
            delta = end_of_month - date
            # logging.info(f'end_of_month: {end_of_month}')
            # logging.info(f'DAYS REMAINING: {delta}')
            # logging.info(f'DAYS REMAINING: {delta.days}')
            return delta.days

        # Connect to your Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()
        logging.info('Connection made')
        # Fetch deposit records from Deposits table
        cursor.execute('SELECT * FROM Deposits WHERE is_active = 1')
        deposits = cursor.fetchall()


        # Iterate over each deposit record
        for deposit in deposits:
            try:
                deposit_id = deposit[0]
                payment_date = deposit[6]
                deposit_month_year_str = deposit[9]
                logging.info(f'Parsing date from string: {deposit_month_year_str + "-01"}')
                deposit_month_year = datetime.strptime(deposit_month_year_str + '-01', '%Y-%m-%d').date()
                amount = deposit[3]

                logging.info(f'DEPOSIT ID: {deposit_id}')
                logging.info(f'Payment date: {payment_date}')

                # Check if the deposit is already in DepositInterest table
                cursor.execute('SELECT deposit_id FROM DepositInterest WHERE deposit_id = ?', (deposit_id,))
                existing_deposit = cursor.fetchone()

                if not existing_deposit:
                    cursor.execute("SET IDENTITY_INSERT DepositInterest ON")
                    # Insert the deposit record into DepositInterest table
                    query = 'INSERT INTO DepositInterest (deposit_id) VALUES (?)'
                    cursor.execute(query, (deposit_id,))
                    cursor.execute("SET IDENTITY_INSERT DepositInterest OFF")

                # Calculate interest for each month
                cursor.execute('SELECT * FROM InterestRates ORDER BY month_year')
                interest_rates = cursor.fetchall()

                # Iterate over interest rates and calculate interest for each month
                for i in range(len(interest_rates)):
                    interest_month_year_str = interest_rates[i][1]
                    annual_interest_rate = float(interest_rates[i][2])
                    daily_interest_rate = (1 + annual_interest_rate) ** (1/365) - 1
                    days = interest_rates[i][3]

                    interest_month_year_lower = interest_month_year_str
                    interest_month_year_upper = interest_month_year_str.replace('_', '-')
                    # logging.info(f'deposit month year str: {deposit_month_year_str}')
                    # logging.info(f'interest month year str: {interest_month_year_upper}')
                    # Convert month strings to date objects for comparison
                    # logging.info(f'Parsing date from string: {interest_month_year_upper + "-01"}')
                    interest_month_year = datetime.strptime(interest_month_year_upper + '-01', '%Y-%m-%d').date()
                    # logging.info(f'deposit month {deposit_month_year}')
                    # logging.info(f'interest month {interest_month_year}')

                    # Calculate interest for the month
                    if deposit_month_year == interest_month_year:
                        # logging.info(f'deposit month = interest month: {deposit_month_year} / {interest_month_year}')
                        days_remaining = days_until_end_of_month(payment_date)
                        # logging.info(f'Days remaining: {days_remaining}')
                        balance = round(amount * (1 + (annual_interest_rate / 365)) ** days_remaining, 2)
                    elif deposit_month_year < interest_month_year:
                        # logging.info(f'deposit month < interest month: {deposit_month_year} / {interest_month_year}')
                        previous_month = interest_rates[i - 1][1]
                        # logging.info(f'PREVIOUS MONTH: {previous_month}')
                        previous_balance = cursor.execute("SELECT [{}] FROM DepositInterest WHERE deposit_id = ?".format(previous_month), (deposit_id,)).fetchone()[0]
                        # logging.info(f'PREVIOUS BALANCE: {previous_balance}')
                        balance = round(previous_balance * (1 + (annual_interest_rate / 365)) ** days, 2)
                    else:
                        # logging.info(f'deposit month > interest month: {deposit_month_year} / {interest_month_year}')
                        balance = None

                    # Update the corresponding month column in DepositInterest table
                    query = "UPDATE DepositInterest SET [{}] = ? WHERE deposit_id = ?".format(interest_month_year_lower)
                    cursor.execute(query, (balance, deposit_id))

                    if balance:
                        interest = round(balance - amount, 2)
                    else:
                        interest = 0
                    cursor.execute('UPDATE Deposits SET interest = ?, total = ? WHERE deposit_id = ?', (interest, balance, deposit_id))

            except Exception as e:
                logging.error(f'Error processing deposit ID {deposit_id}: {e}')
                continue  # Skip to the next deposit

        # Update deposit total per unit
        cursor.execute('SELECT * FROM RentalUnits')
        units = cursor.fetchall()

        for unit in units:
            unit_ref = unit[2]

            cursor.execute('SELECT SUM(amount) FROM Deposits WHERE unit_ref = ? AND total > 0 AND is_active = 1', (unit_ref,))
            deposit_paid = cursor.fetchone()[0] or 0

            cursor.execute('SELECT SUM(total) FROM Deposits WHERE unit_ref = ? AND is_active = 1', (unit_ref,))
            deposit_balance = cursor.fetchone()[0] or 0

            cursor.execute('UPDATE RentalUnits SET deposit_balance = ?, deposit_paid = ? WHERE unit_ref = ?', (deposit_balance, deposit_paid, unit_ref))

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        logging.info('Data processing completed successfully.')
        return "Data processed successfully."

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return f"An error occurred: {str(e)}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        result = process_deposit_data()
        return func.HttpResponse(result, status_code=200)

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)