import azure.functions as func
import pyodbc
import logging

def update_transactions(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

        # Database connection details
        username = 'dylanwalls'
        password = '950117Dy!'
        server = 'scorecard-server.database.windows.net'
        database = 'dashboard-new-server'
        driver = '{ODBC Driver 17 for SQL Server}'

        # Connect to the Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Log successful connection
        logging.info("Successfully connected to the Azure SQL Database")

        # Initialize lists for tracking unmatched and duplicate matches
        noMatchFound = []
        duplicateMatchFound = []

        cursor.execute("SELECT payout_reference FROM indluPayoutSchedule WHERE reconciled = 0")
        unreconciled_batches = cursor.fetchall()
        # Fetch externalReference values from unreconciled_batches list of tuples
        unreconciled_external_references = [batch[0] for batch in unreconciled_batches]
    

        # Fetch data from indluPayoutList
        cursor.execute("SELECT * FROM indluPayoutList")
        payoutList = cursor.fetchall()

        for payout in payoutList:
            internalReference = payout[3]
            externalReference = payout[7]
            amount = payout[6]

            if externalReference in unreconciled_external_references:
                continue

            # Search Transactions table for a matching rrn
            cursor.execute("SELECT * FROM Transactions WHERE rrn LIKE ?", (internalReference + '%',))
            matchingTransactions = cursor.fetchall()

            if len(matchingTransactions) == 0:
                noMatchFound.append(payout)
            elif len(matchingTransactions) > 1:
                total_transaction_amount = sum(transaction.amount for transaction in matchingTransactions)
                logging.info(f"Multiple transactions found for internalReference: {internalReference}, distributing proportionally.")

                for transaction in matchingTransactions:
                    proportion = transaction.amount / total_transaction_amount
                    proportional_payout = amount * proportion
                    fee = transaction.amount - proportional_payout
                    rrn = transaction.rrn
                    
                    cursor.execute("""
                        UPDATE Transactions
                        SET batch_payout_reference = ?, payout_amount = ?, fee = ? 
                        WHERE rrn = ?""", 
                        (externalReference, proportional_payout, fee, rrn))
                    logging.info(f"Updated transaction for internalReference: {rrn} with proportional payout: {proportional_payout} and fee: {fee}")
            else:
                # Exactly one match found, update Transactions
                transaction = matchingTransactions[0]
                fee = transaction.amount - amount
                cursor.execute("""
                    UPDATE Transactions
                    SET batch_payout_reference = ?, payout_amount = ?, fee = ? 
                    WHERE rrn = ?""", 
                    (externalReference, amount, fee, internalReference))
                logging.info(f"Updated transaction for internalReference: {internalReference} with fee: {fee}")


        # Commit changes if any updates were made
        conn.commit()

        # Close the connection
        conn.close()

        # Print unmatched and duplicate matches
        logging.info("No Match Found:")
        for item in noMatchFound:
            logging.info(item)

        logging.info("Duplicate Match Found:")
        for item in duplicateMatchFound:
            logging.info(item)

        return func.HttpResponse(f"Update process completed. No matches: {len(noMatchFound)}, Duplicates: {len(duplicateMatchFound)}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)

# Azure Functions entry point
def main(req: func.HttpRequest) -> func.HttpResponse:
    return update_transactions(req)
