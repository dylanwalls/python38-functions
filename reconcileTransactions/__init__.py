import azure.functions as func
import pyodbc
import logging
from datetime import datetime

def reconcile_transactions(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Configure the logging level and format
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

        username = 'dylanwalls'
        password = '950117Dy!'
        server = 'scorecard-server.database.windows.net'
        database = 'dashboard-new-server'
        driver = '{ODBC Driver 17 for SQL Server}'

        # Connect to the Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()

        # Log a message indicating that the database connection is successful
        logging.info("Connected to the Azure SQL Database")

        # Retrieve transactions and invoices from the database
        cursor.execute("SELECT * FROM Transactions WHERE is_reconciled = 0")
        transactions = cursor.fetchall()

        cursor.execute("SELECT * FROM Invoices WHERE closed is null")
        invoices = cursor.fetchall()
        logging.info(f'Invoices: {invoices}')

        # Initialize lists for incomplete invoices and unmatched transactions
        incomplete_invoices = []
        unmatched_transactions = []

        # Iterate over transactions and reconcile with invoices
        for transaction in transactions:
            transaction_id = transaction[0]
            owner = transaction[1]
            unit_no = transaction[2]
            rrn = transaction[3]
            amount = transaction[6]
            deposit = transaction[7]
            rent = transaction[8]
            date = transaction[12]
            rent_reconciled = transaction[14]
            deposit_reconciled = transaction[15]
            is_reconciled = transaction[16]
            month = transaction[17]
            year = transaction[18]
            transaction_invoice_id = transaction[19]

            # If already reconciled, skip
            if is_reconciled == 1:
                logging.info('Already reconciled. Skipping.')
                continue

            # RENT reconciliation logic
            if rent_reconciled == 0:
                matching_rent_invoice = None
                if rent == 0:
                    rent_reconciled = 1
                else:
                    for invoice in invoices:
                        if (
                            invoice[3] == unit_no
                            and invoice[4] == month
                            and invoice[5] == year
                            and invoice[6] == "rent"
                        ):
                            matching_rent_invoice = invoice
                            break

                if matching_rent_invoice:
                    matching_invoice_id = matching_rent_invoice[0]
                    if transaction_invoice_id is not None:
                        transaction_invoice_id = f"{transaction_invoice_id} / {matching_invoice_id}"
                    else:
                        transaction_invoice_id = str(matching_invoice_id)
                    rent_reconciled = 1
                    cursor.execute(
                        "UPDATE Transactions SET invoice_id = ?, rent_reconciled = ? WHERE transaction_id = ?",
                        (transaction_invoice_id, rent_reconciled, transaction_id),
                    )

                    # Create a new InvoicePayments record for the transaction
                    payment_type = "rent"
                    included_in_payout = 0
                    payout_date = None
                    payment_date = date
                    cursor.execute(
                        "INSERT INTO InvoicePayments (invoice_id, payment_date, payment_type, amount, unit_ref, rrn, included_in_payout, payout_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            matching_invoice_id,
                            payment_date,
                            payment_type,
                            rent,
                            unit_no,
                            rrn,
                            included_in_payout,
                            payout_date,
                        ),
                    )
                    logging.info("Point 1")
                    # Get the payment_id of the inserted InvoicePayments record
                    cursor.execute(
                        "SELECT SCOPE_IDENTITY() AS last_identity"
                    )  # Get the last inserted identity value
                    payment_id = cursor.fetchone().last_identity

                    cursor.execute(
                        "SELECT invoice_payments FROM Invoices WHERE invoice_id = ?",
                        (matching_invoice_id,),
                    )
                    existing_invoice_payments = cursor.fetchone()[0]

                    if existing_invoice_payments:
                        invoice_payments = (
                            f"{existing_invoice_payments}, {payment_id}"
                        )
                    else:
                        invoice_payments = str(payment_id)

                    cursor.execute(
                        "UPDATE Invoices SET invoice_payments = ? WHERE invoice_id = ?",
                        (invoice_payments, matching_invoice_id),
                    )
                    logging.info("Point 2")
            # DEPOSIT reconciliation logic
            if deposit_reconciled == 0:
                matching_deposit_invoice = None
                if deposit == 0:
                    deposit_reconciled = 1
                else:
                    for deposit_invoice in invoices:
                        if (
                            deposit_invoice[3] == unit_no
                            and deposit_invoice[6] == "deposit"
                        ):
                            matching_deposit_invoice = deposit_invoice
                            break

                if matching_deposit_invoice:
                    matching_invoice_id = matching_deposit_invoice[0]
                    if transaction_invoice_id is not None:
                        transaction_invoice_id = f"{transaction_invoice_id} / {matching_invoice_id}"
                    else:
                        transaction_invoice_id = str(matching_invoice_id)
                    deposit_reconciled = 1
                    cursor.execute(
                        "UPDATE Transactions SET invoice_id = ?, deposit_reconciled = ? WHERE transaction_id = ?",
                        (transaction_invoice_id, deposit_reconciled, transaction_id),
                    )
                    logging.info("Point 3")
                    # Create a new InvoicePayments record for the transaction
                    payment_type = "deposit"
                    included_in_payout = 0
                    payout_date = None
                    payment_date = date
                    cursor.execute(
                        "INSERT INTO InvoicePayments (invoice_id, payment_date, payment_type, amount, unit_ref, rrn, included_in_payout, payout_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            matching_invoice_id,
                            payment_date,
                            payment_type,
                            deposit,
                            unit_no,
                            rrn,
                            included_in_payout,
                            payout_date,
                        ),
                    )

                    # Get the payment_id of the inserted InvoicePayments record
                    cursor.execute(
                        "SELECT SCOPE_IDENTITY() AS last_identity"
                    )  # Get the last inserted identity value
                    payment_id = cursor.fetchone().last_identity
                    logging.info("Point 4")
                    cursor.execute(
                        "SELECT invoice_payments FROM Invoices WHERE invoice_id = ?",
                        (matching_invoice_id,),
                    )
                    existing_invoice_payments = cursor.fetchone()[0]

                    if existing_invoice_payments:
                        invoice_payments = (
                            f"{existing_invoice_payments}, {payment_id}"
                        )
                    else:
                        invoice_payments = str(payment_id)

                    cursor.execute(
                        "UPDATE Invoices SET invoice_payments = ? WHERE invoice_id = ?",
                        (invoice_payments, matching_invoice_id),
                    )

                    # Add deposit transaction to Deposits table
                    month_year = str(date)[:7]
                    cursor.execute('SELECT unit_id FROM RentalUnits WHERE unit_ref = ?', (unit_no,))
                    unit_id = cursor.fetchone()[0]
                    query = "INSERT INTO Deposits (unit_id, unit_ref, amount, interest, total, start_date, end_date, is_active, month_year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cursor.execute(query, (unit_id, unit_no, deposit, None, None, date, None, 1, month_year))

                    # Get new sum for deposit balance
                    cursor.execute('SELECT SUM(amount) FROM Deposits WHERE unit_ref = ?', (unit_no,))
                    deposit_paid = cursor.fetchone()[0] or 0

                    cursor.execute('UPDATE RentalUnits SET deposit_paid = ? WHERE unit_ref = ?', (deposit_paid, unit_no))




            # Check if the transaction is unmatched
            if rent_reconciled == 1 and deposit_reconciled == 1:
                is_reconciled = 1
                cursor.execute(
                    "UPDATE Transactions SET is_reconciled = 1 WHERE transaction_id = ?",
                    (transaction_id,),
                )
            else:
                unmatched_transactions.append(transaction)

        # Handle Invoices
        logging.info("Point 5")
        cursor.execute("SELECT * FROM InvoicePayments")
        invoice_payments = cursor.fetchall()
        for invoice in invoices:
            logging.info("Point 5a")
            if invoice[6] == "rent":
                # Handle rent invoices
                matching_rent_transactions = [
                    transaction for transaction in transactions
                    if transaction[19] and str(invoice[0]) in transaction[19]
                ]
                logging.info("Point 5b")
                # Calculate the total rent paid for this invoice
                total_rent_paid = sum(
                    payment[4] for payment in invoice_payments
                    if payment[1] == invoice[0]  # Check if payment is associated with this invoice
                )
                logging.info("Point 5c")
                # Update the invoice with the calculated amount_paid
                cursor.execute(
                    "UPDATE Invoices SET amount_paid = ? WHERE invoice_id = ?",
                    (total_rent_paid, invoice[0])
                )
                logging.info("Point 5d")
            elif invoice[6] == "deposit":
                logging.info("Point 6")
                # Handle deposit invoices
                matching_deposit_transactions = [
                    transaction for transaction in transactions
                    if transaction[19] and str(invoice[0]) in transaction[19]
                ]

                if len(matching_deposit_transactions) == 0:
                    # No matching transactions, mark the invoice as incomplete
                    incomplete_invoices.append(invoice)

        
        cursor.execute('SELECT * FROM Invoices')
        invoice_selection = cursor.fetchall()
        logging.info("Point 7")

        for invoice in invoice_selection:
            cursor.execute('SELECT * FROM InvoicePayments WHERE invoice_id = ? AND payment_type = ?', (invoice[0], invoice[6]))
            payments = cursor.fetchall()
            
            if payments:
                print('there are payments')
                payment_dates = [payment[2] for payment in payments]
                print(f'payment_dates = {payment_dates}')
                logging.info('Least recent date calc coming up:')
                least_recent_date = min(payment_dates)
                print(f'least_recent_date = {least_recent_date}')
                total_paid = sum(payment[4] for payment in payments)
                print(f'total_paid = {total_paid}')
                rrn = ' / '.join(payment[6] for payment in payments)
                print(f'rrn = {rrn}')

                if invoice[7] == total_paid:
                    is_filled = 1
                else:
                    is_filled = 0

                cursor.execute('UPDATE Invoices SET amount_paid = ?, date_paid = ?, transaction_ref = ?, is_filled = ? WHERE invoice_id = ?', (total_paid, least_recent_date, rrn, is_filled, invoice[0]))


        # Check if the invoice is incomplete (is_filled = 0)
        cursor.execute('SELECT * FROM Invoices')
        invoices = cursor.fetchall()
        logging.info("Point 8")

        for invoice in invoices:
            if invoice[11] == 0:
                incomplete_invoices.append(invoice)

        for incomplete_invoice in incomplete_invoices:
            logging.info(f'Incomplete invoice: {incomplete_invoice}')


        # Commit the changes and close the connection
        conn.commit()
        conn.close()
        logging.info("Point 9 - connection closed")
        # Log a message indicating the completion of transaction reconciliation
        logging.info("Transaction reconciliation completed")

        # Check if the invoice is incomplete
        logging.info("Incomplete Invoices:")
        for invoice in incomplete_invoices:
            logging.info(invoice)

        logging.info("Unmatched Transactions:")
        for transaction in unmatched_transactions:
            logging.info(transaction)

        return func.HttpResponse("Transaction reconciliation completed.")
    except Exception as e:
        # Log the error and return an error response
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)

# Azure Functions entry point
def main(req: func.HttpRequest) -> func.HttpResponse:
    return reconcile_transactions(req)
