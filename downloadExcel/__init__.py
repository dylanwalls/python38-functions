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
        columns = ['unit_id', 'property_id', 'unit_ref', 'rent', 'deposit_balance', 'deposit_paid', 'status']

        # Fetch the data from the Invoices and RentalUnits tables with specific columns and join condition
        query = f"SELECT {', '.join(columns)} FROM ActiveRentalUnits"

        df = pd.read_sql_query(query, conn)

        # Fetch the data from the Invoices and RentalUnits tables where deposit payments are outstanding
        query = f"SELECT {', '.join(columns)} FROM ActiveRentalUnits WHERE status = 'Overdue'"


        overdue_list_df = pd.read_sql_query(query, conn)
        logging.info('Point 1')
        # TOTALS

        # Total flats
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM ActiveRentalUnits')
        total_flats = cursor.fetchone()[0]
        logging.info(f'Total flats: {total_flats}')
    
        # Total flats VACANT
        cursor.execute("SELECT COUNT(*) FROM ActiveRentalUnits WHERE vacant = 1")
        total_flats_vacant = cursor.fetchone()[0]
        logging.info(f'Total flats vacant: {total_flats_vacant}')

        # Total flats excl VACANT
        total_flats_occupied = total_flats - total_flats_vacant
        logging.info(f'Total flats occupied: {total_flats_occupied}')

        # Total value expected (excl vacant flats)
        cursor.execute("SELECT SUM(rent) FROM ActiveRentalUnits WHERE vacant = 0")
        total_value_due_excl_vacant = cursor.fetchone()[0]
        logging.info(f'Total value due excl vacancies: {total_value_due_excl_vacant}')

        logging.info('Point 2')
        # OVERDUE

        # Total number overdue
        cursor.execute("SELECT COUNT(*) FROM ActiveRentalUnits WHERE status = 'Overdue'")
        total_number_overdue = cursor.fetchone()[0]
        logging.info(f'Total number overdue: {total_number_overdue}')

        # Percentage overdue
        percentage_overdue = total_number_overdue / total_flats_occupied
        logging.info(f'Percentage overdue: {percentage_overdue}')

        # Rand value held by overdue invoices
        cursor.execute("SELECT SUM(deposit_paid) FROM ActiveRentalUnits WHERE vacant = 0 AND status = 'Overdue'")
        total_value_held_overdue_invoices = cursor.fetchone()[0]
        logging.info(f'Total value held in overdue invoices: {total_value_held_overdue_invoices}')

        logging.info('Point 3')

        # PARTIALLY PAID

        # Total number partially paid
        cursor.execute(f"SELECT COUNT(*) FROM ActiveRentalUnits WHERE vacant = 0 AND status = 'Partially Paid'")
        total_number_partially_paid = cursor.fetchone()[0]
        print(f'Total number partially paid: {total_number_partially_paid}')

        # Percentage partially paid
        percentage_partially_paid = total_number_partially_paid / total_flats_occupied
        print(f'Percentage partially paid: {percentage_partially_paid}')

        # Rand value partially paid
        cursor.execute("SELECT SUM(deposit_paid) FROM ActiveRentalUnits WHERE vacant = 0 AND status = 'Partially Paid'")
        total_value_partially_paid = cursor.fetchone()[0]
        print(f'Total value partially paid: {total_value_partially_paid}')

        logging.info('Point 4')

        # FULLY PAID

        # Total number fully paid
        total_fully_paid_conditions = ["status = 'Paid'", "status = 'Overpaid'"]
        cursor.execute(f"SELECT COUNT(*) FROM ActiveRentalUnits WHERE vacant = 0 AND {' OR '.join(total_fully_paid_conditions)}")
        total_number_fully_paid = cursor.fetchone()[0]
        print(f'Total number fully paid: {total_number_fully_paid}')

        # Percentage fully paid
        percentage_fully_paid = total_number_fully_paid / total_flats_occupied
        print(f'Percentage fully paid: {percentage_fully_paid}')

        # Rand value fully paid
        cursor.execute(f"SELECT SUM(deposit_paid) FROM ActiveRentalUnits WHERE vacant = 0 AND {' OR '.join(total_fully_paid_conditions)}")
        total_value_fully_paid = cursor.fetchone()[0]
        print(f'Total value fully paid: {total_value_fully_paid}')

        # SURPLUS AND DEFICIT

        # # Rand value deficit
        # cursor.execute("SELECT SUM(deficit) FROM Invoices WHERE invoice_type = 'deposit' AND deposit_status = 'OVERDUE'")
        # total_value_deficit = cursor.fetchone()[0]
        # print(f'Total value deficit: {total_value_deficit}')

        # # Rand value surplus
        # cursor.execute("SELECT SUM(surplus) FROM Invoices WHERE invoice_type = 'deposit' AND deposit_status = 'OVERPAID'")
        # total_value_surplus = cursor.fetchone()[0]
        # print(f'Total value surplus: {total_value_surplus}')

        logging.info('Point 5')
        
        # TOTALS
        totals_df = pd.DataFrame({
            'Label': ['Total flats'],
            'Number': [total_flats],
            'Percentage': [None],
            'Value': [None]
        })

        # VACANT
        vacant_df = pd.DataFrame({
            'Label': ['Total vacant'],
            'Number': [total_flats_vacant],
            'Percentage': [None],
            'Value': [None]
        })

        # Add a blank row to separate 'Totals' from the next section
        blank_row_df = pd.DataFrame({'Label': [''], 'Number': [None], 'Percentage': [None], 'Value': [None]})

        # OVERDUE
        overdue_df = pd.DataFrame({
            'Label': ['Overdue'],
            'Number': [total_number_overdue],
            'Percentage': [percentage_overdue],
            'Value': [total_value_held_overdue_invoices]
        })

        # PARTIALLY PAID
        partially_paid_df = pd.DataFrame({
            'Label': ['Partially Paid'],
            'Number': [total_number_partially_paid],
            'Percentage': [percentage_partially_paid],
            'Value': [total_value_partially_paid]
        })

        # FULLY PAID
        fully_paid_df = pd.DataFrame({
            'Label': ['Fully Paid'],
            'Number': [total_number_fully_paid],
            'Percentage': [percentage_fully_paid],
            'Value': [total_value_fully_paid]
        })

        # Concatenate DataFrames vertically (one below the other)
        calculated_df = pd.concat([totals_df, vacant_df, blank_row_df, overdue_df, partially_paid_df, fully_paid_df], axis=0)

        # Reset the index of the DataFrame
        calculated_df.reset_index(drop=True, inplace=True)

        # TOTAL ROW
        total_value_paid_excl_vacancies = total_value_held_overdue_invoices + total_value_partially_paid + total_value_fully_paid
        total_row_df = pd.DataFrame({
            'Label': ['Total'],
            'Number': [total_flats_occupied],
            'Percentage': ['1'],
            'Value': [total_value_paid_excl_vacancies]
        })

        logging.info('Point 6')

        # Concatenate 'Total' row to the main DataFrame
        calculated_df = pd.concat([calculated_df, total_row_df], ignore_index=True)

        # Define path for the Excel file in temporary storage
        temp_file = os.path.join(tempfile.gettempdir(), 'depositSchedule.xlsx')

        # Export the DataFrame to an Excel file
        with pd.ExcelWriter(temp_file, engine='xlsxwriter') as writer:
            # Define a format for the bold style
            bold_format = writer.book.add_format({'bold': True})
            
            # Write the 'Calculated' DataFrame to the 'Summary' sheet
            calculated_df.to_excel(writer, sheet_name='Summary', index=False, startrow=1, header=False)
            
            # Retrieve the 'Summary' sheet
            calculated_sheet = writer.sheets['Summary']
            
            # Apply bold to the column header
            for idx, col in enumerate(calculated_df.columns):
                calculated_sheet.write(0, idx, col, bold_format)
            
            # Apply bold to the 'Total' row
            calculated_sheet.conditional_format(1, 0, len(calculated_df), len(calculated_df.columns) - 1,
                                                {'type': 'formula', 'criteria': '=$A2="Total"',
                                                'format': bold_format})
            
            # Write the 'Deposits' DataFrame to a new sheet named 'Deposits'
            df.to_excel(writer, sheet_name='Deposits', index=False)

            # Write the 'Deposits' DataFrame to a new sheet named 'Deposits'
            overdue_list_df.to_excel(writer, sheet_name='Overdue', index=False)
            
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

