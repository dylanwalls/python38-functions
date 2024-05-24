import azure.functions as func
import pandas as pd
import os
import pyodbc  # Import the pyodbc library for connecting to Azure SQL Database
import logging
import tempfile

# Define your Azure SQL Database connection parameters
username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def process_excel_sheet():
    try:
        # Create a connection to Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        logging.info('CONNECTED')


        # DEPOSIT SCHEDULE

        # Define the columns and their order for export
        columns = ['property_id', 'property_ref', 'homeowner', 'latest_homeowner_total', 'latest_statement']

        # Fetch the data from the Invoices and RentalUnits tables with specific columns and join condition
        # query = f"SELECT {', '.join(columns)} FROM Invoices"
        query = '''SELECT
        Properties.property_id,
        Properties.property_ref,
        Properties.homeowner,
        Properties.latest_homeowner_total,
        Properties.latest_statement
        FROM Properties
        WHERE is_active = 1
        ORDER BY Properties.chron_order;'''

        df = pd.read_sql_query(query, conn)


        logging.info('Point 1')

        # Define path for the Excel file in temporary storage
        temp_file = os.path.join(tempfile.gettempdir(), 'payoutSchedule.xlsx')

        # Export the DataFrame to an Excel file
        with pd.ExcelWriter(temp_file, engine='xlsxwriter') as writer:
            # Define a format for the bold style
            bold_format = writer.book.add_format({'bold': True})
            
            # Write the 'Deposits' DataFrame to a new sheet named 'Deposits'
            df.to_excel(writer, sheet_name='Deposits', index=False)
            
        # Close the database connection
        conn.close()

        logging.info('Point 7 - connection closed')
        logging.info('Data processing completed successfully.')
        return "Deposit Schedule processed successfully.", temp_file

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
                                            'Content-Disposition': 'attachment;filename=depositSchedule.xlsx'
                                        })

    except Exception as e:
        import traceback
        logging.error(f'An error occurred: {str(e)}\n{traceback.format_exc()}')
        return func.HttpResponse(f"An error occurred: {str(e)}\n{traceback.format_exc()}", status_code=500)

