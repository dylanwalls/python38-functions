import azure.functions as func
import pandas as pd
import os
import pyodbc  # Import the pyodbc library for connecting to Azure SQL Database
import logging
import tempfile

# Define your Azure SQL Database connection parameters
username = 'dylanwalls'  # It's not safe to hardcode credentials
password = '950117Dy!'   # Consider using Azure Key Vault or environment variables for secrets
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def process_excel_sheet():
    try:
        # Create a connection to Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        logging.info('CONNECTED')

        # Transaction Summary Query
        query = '''
        SELECT 
            t.batch_payout_reference,
            ROUND(SUM(t.amount), 2) AS total_amount,
            ROUND(SUM(t.fee), 2) AS total_fee,
            ROUND(SUM(t.deposit), 2) AS total_deposit,
            ROUND(SUM(t.rent), 2) AS total_rent,
            ROUND(SUM(t.payout_amount), 2) AS total_payout_amount,
            ROUND(SUM(t.rent) * 0.05, 2) AS [5_percent_fee],
            LEFT(MIN(ips.payout_date), 10) AS payout_date,
            DATENAME(month, CAST(MIN(ips.payout_date) AS date)) + ' ' + CAST(YEAR(CAST(MIN(ips.payout_date) AS date)) AS varchar) AS month_year
        FROM 
            Transactions t
        LEFT JOIN indluPayoutSchedule ips ON ips.payout_reference = t.batch_payout_reference
        WHERE 
            t.batch_payout_reference IS NOT NULL AND t.batch_payout_reference <> ''
        GROUP BY 
            t.batch_payout_reference;
        ;
        '''

        df = pd.read_sql_query(query, conn)

        logging.info('Data fetched successfully')

        # Define path for the Excel file in temporary storage
        temp_file = os.path.join(tempfile.gettempdir(), 'indluPayoutRecon.xlsx')

        # Export the DataFrame to an Excel file
        with pd.ExcelWriter(temp_file, engine='xlsxwriter') as writer:
            # Define a format for the bold style
            bold_format = writer.book.add_format({'bold': True})
            
            # Write the DataFrame to a new sheet
            df.to_excel(writer, sheet_name='Payout Summary', index=False)

        # Close the database connection
        conn.close()

        logging.info('Excel file created and connection closed')
        return "Payout Summary processed successfully.", temp_file

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return f"An error occurred: {str(e)}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        message, file_path = process_excel_sheet()
        
        with open(file_path, "rb") as f:
            return func.HttpResponse(f.read(),
                                        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        headers={
                                            'Content-Disposition': 'attachment;filename=transactionSummary.xlsx'
                                        })

    except Exception as e:
        import traceback
        logging.error(f'An error occurred: {str(e)}\n{traceback.format_exc()}')
        return func.HttpResponse(f"An error occurred: {str(e)}\n{traceback.format_exc()}", status_code=500)
