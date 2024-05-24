import os
import io
import time
import subprocess
import sys
import pandas as pd
from datetime import date, datetime
import sqlite3
import pdfkit
from jinja2 import Environment, FileSystemLoader
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
# import azure.functions as func
import pyodbc  # Import the pyodbc library for connecting to Azure SQL Database
import logging
logging.basicConfig(level=logging.CRITICAL)
import tempfile
import azure.functions as func
import requests
import json
import random
import string

# Define your Azure SQL Database connection parameters
username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

# # INPUTS
# global is_final, month, as_of, bulk_comments
# invoice_type = 'rent'

# # Function to generate a random string
# def generate_random_string(length):
#     characters = string.ascii_letters + string.digits
#     return ''.join(random.choice(characters) for _ in range(length))

# def convert_html_to_pdf_and_upload(html_content):
#     # Write HTML content to a temporary file
#     with tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.html') as temp_html:
#         temp_html.write(html_content)
#         temp_html_path = temp_html.name

#     # Run the Node.js script
#     result = subprocess.run(["node", "index.js", temp_html_path], capture_output=True)

#     # Delete the temporary HTML file
#     os.remove(temp_html_path)

#     if result.returncode != 0:
#         print("Error during PDF conversion:", result.stderr.decode())
#         return

#     pdf_bytes = result.stdout
#     pdf_file = io.BytesIO(pdf_bytes)
#     return pdf_file
#     # Now you can proceed to upload pdf_file to Azure Blob Storage
    
def zip_values_and_classes(values, classes):
    return zip(values, classes)

def generate_statement(property_data, is_final, month, as_of, bulk_comments):

    # Get the path of the "Homeowner Statements" folder
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    logging.info(os.getcwd())

    # Create a Jinja2 environment with the correct template directory
    env = Environment(loader=FileSystemLoader('templates'))

    # Create a Jinja environment with the template file location
    # env = Environment(loader=FileSystemLoader(os.path.dirname(template_path)), autoescape=True)
    env.globals['zip_values_and_classes'] = zip_values_and_classes

    template = env.get_template('statement_template.html')
    # # logo_path = os.path.join(base_path, 'Homeowner Statements/design/logo.png')
    # logo_path = "/Users/dylanwalls/Documents/Bitprop/Homeowner Statements/design/logo.png"
    # print(f'MY LOGO PATH: {logo_path}')

    # Prepare the data for the statement
    # Print property data for debugging
    logging.info(f'Property Data: {property_data}')
    data = {
        'title': 'Statement per Property',
        'heading': 'Final statement of {} for {} 2023'.format(invoice_type.capitalize(), str(month)),
        'content': '{}'.format(as_of),
        'details_list': [
            'Homeowner: {}'.format(property_data['homeowner']),
            'Street: {}'.format(property_data['street']),
            'Suburb: {}'.format(property_data['suburb']),
            'Property: {}'.format(property_data['property_ref'])
        ],
        'columns': ['Date Paid', 'Unit', 'Amount Due', 'Amount Paid'],
        'table_rows': [],
        'table_rows_future_month': [],
        'comments': property_data['comments'],
        # New table for water and electricity
        'third_table_columns': ['Description', 'Amount'],
        'third_table_rows': []
    }
    logging.info(f'Data: {data}')
    # Conditionally add month information to details_list
    if invoice_type == 'rent':
        data['details_list'].append('Month: {}'.format(str(month)))

    total_unpaid = 0
    total_partially_paid = 0
    logging.info(f'Total unpaid = {total_unpaid}')
    for invoice in invoices:
        # Calculate the sum of the amount column for the invoices
        if invoice[12] == 'Unpaid' and invoice[4] <= month:
            total_unpaid = total_unpaid + invoice[8]
        elif invoice[12] == 'Partially Paid' and invoice[4] <= month:
            logging.info("Here is a partially paid calculation")
            partially_paid_amounts = []
            for payment_id in invoice[14].split(','):
                logging.info(f'PAYMENT ID: {payment_id}')
                cursor.execute("SELECT * FROM InvoicePayments WHERE included_in_payout = 0 AND payment_id = ?", (payment_id,))
                partially_paid_amount = cursor.fetchall()
                for row in partially_paid_amount:
                    amount = row[4]
                    logging.info(f'NEW PARTIAL AMOUNT {amount}')
                    partially_paid_amounts.append(amount)
                logging.info(f'this is the list of partially paid amounts {partially_paid_amounts}')
            total_partially_paid = sum(amount for amount in partially_paid_amounts if amount is not None)
            logging.info(f'This is the sum of partially paid amounts {total_partially_paid}')
    total_amount = total_unpaid + total_partially_paid
    logging.info(f'MY TOTAL AMOUNT IS {total_amount}')

    total_css_classes = ['bold', '', '', '']  # CSS classes for total row

    # Populate the table rows from Invoices
    for invoice in invoices:
        if invoice[8] == 0:
            month_number = invoice[4]
            dt = datetime(year=2023, month=month_number, day=1)
            # logging.info('date...')
            # logging.info('percentage conversion')
            month_name = dt.strftime("%B")
            payment_date_str = f'UNPAID - {month_name}'
            css_class = 'unpaid' # CSS class for unpaid invoices
        else:
            payment_date = invoice[9]
            if invoice[4] == as_of:
                # logging.info('percentage conversion3')
                # logging.info(f'PAYMENT DATE: {payment_date}')
                payment_date_str = payment_date[:10]
            else:
                month_number = invoice[4]
                dt = datetime(year=2023, month=month_number, day=1)
                # logging.info('date2...')
                # logging.info('percentage conversion2')
                month_name = dt.strftime("%B")
                payment_date_str = f'{payment_date} - {month_name}'
                # logging.info(f'payment_date_str {payment_date_str}')
            css_class = '' # No CSS class for paid invoices
        unit_ref = invoice[3]
        unit_letter = unit_ref[-1].upper()
        row = {
            'values': [payment_date_str, unit_letter, invoice[7], invoice[8]],
            'css_classes': [css_class, css_class, css_class, css_class]  # Initialize CSS classes for each cell
        }
        if invoice[4] <= month:
            data['table_rows'].append(row)
        elif (invoice[4] > month and invoice[8] > 0):
            data['table_rows_future_month'].append(row)

    # logging.info('POINT 1')

    # Add a total row at the bottom of the table
    total_row = {
        'values': ['Total', '', '', round(total_amount, 2)],
        'css_classes': ['bold', '', '', 'bold']  # Apply 'bold' class to the first cell
    }
    data['table_rows'].append(total_row)
    data['total_css_classes'] = total_css_classes

    # logging.info('POINT 2')

    # Calculate the homeowner rent if it is a rent statement
    homeowner_rent = 0
    if invoice_type == 'rent':
        if property_data['property_ref'] == '26B':
            homeowner_rent = round(0.175 * total_amount, 2)
        elif property_data['property_ref'] == '59N':
            homeowner_rent = round(0.35 * total_amount, 2)
        else:
            homeowner_rent = round(0.15 * total_amount, 2)
        if property_data['homeowner'] in homeowner_totals_dict:
            logging.info("Homeowner already in homeowner payouts table")
        else:
            homeowner_totals_dict[property_data['homeowner']] = [property_data['homeowner'], property_data['property_ref'], homeowner_rent]

        # logging.info('POINT 3')
        cursor.execute('UPDATE Properties SET latest_homeowner_total = ? WHERE property_ref = ?', (homeowner_rent, property_data['property_ref']))
    # Add a homeowner rent row if applicable
    if homeowner_rent > 0:
        homeowner_rent_row = {
            'values': ['Homeowner Rent', '', '', homeowner_rent],
            'css_classes': ['bold', '', '', 'bold']  # Set CSS class 'bold' for the first cell
        }
        data['table_rows'].append(homeowner_rent_row)
        homeowner_rent_css_classes = ['bold', '', '', '']  # CSS classes for homeowner rent row
    else:
        homeowner_rent_css_classes = ['', '', '', '']  # Empty CSS classes for homeowner rent row

    # Add water and electricty table

    if property_data['elec']:
        elec_row = {
            'values': ['Electricity', round(property_data['elec'], 2)],
            'css_classes': ['bold', 'bold']
        }
        data['third_table_rows'].append(elec_row)
    if property_data['water']:
        water_row = {
            'values': ['Water', round(property_data['water'], 2)],
            'css_classes': ['bold', 'bold']
        }
        data['third_table_rows'].append(water_row)

        # logging.info('POINT 4')


    # Pass the CSS classes to the template
    data['homeowner_rent_css_classes'] = homeowner_rent_css_classes

    # Render the statement template with the data
    # data['logo_path'] = logo_path
    # Check if there are rows for future months
    if data['table_rows_future_month']:
        # Include the future months table
        statement = template.render(data)
        # logging.info(f'statement raw: {statement}')
    else:
        # Remove the future months table from the template
        data.pop('table_rows_future_month')
        statement = template.render(data)
        # logging.info(f'statement raw: {statement}')

    # logging.info('POINT 5')

    # Generate the file name
    file_name = '{} {} Final Statement {} 2023.pdf'.format(property_data['homeowner'], invoice_type.capitalize(), month)

    # logging.info('POINT 6')

    data = {
        "htmlContent": statement
    }
    json_data = json.dumps(data)

    # Use os.path.join to ensure the correct path formatting
    temp_pdf_path = os.path.join("/tmp", "output.pdf")

    # logging.info(f'JSON DUMPS DATA: {json_data}')
    response = requests.post("https://dashboard-function-app-1.azurewebsites.net/api/createPDF?code=Ixv6ZF_9XBhnHvNSsM64pHqD1xk7hVkc2WWqHTSvxXCZAzFuMB8kRA==", json={"htmlContent": statement})

    if response.status_code == 200:
        with open(temp_pdf_path, "wb") as f:
            f.write(response.content)
        print(f"PDF file created successfully at: {temp_pdf_path}")
    else:
        print("Failed to create PDF. Status code:", response.status_code)
        print("Response body:", response.text)
        
    pdf_file = response.content



    # Use a temporary directory to store the PDF file
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     # Construct the full file path within the temporary directory
    #     file_path = os.path.join(temp_dir, file_name)

    # # Construct the path to the statements_db subfolder
    # subfolder_path = os.path.join(base_path, '/generateStatements/statements')

    # # Create the subfolder if it doesn't exist
    # if not os.path.exists(subfolder_path):
    #     os.makedirs(subfolder_path)

    # # Construct the full file path within the subfolder
    # file_path = os.path.join(subfolder_path, file_name)

    # # # Convert the statement to PDF and save with the generated file path
    # # config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')
    # # pdfkit.from_string(statement, file_path, configuration=config, options={})

    # pdfkit.from_string(statement, file_path, options={'--debug-javascript': ''})

    # logging.info(file_path)

    # logging.info('POINT 7')
    upload_pdfs = 1
    if upload_pdfs == 1:
        # Upload the PDF file to Azure Blob Storage
        # logging.info('POINT 8')
        # logging.info(f"Connection String: {os.environ.get('DefaultEndpointsProtocol=https;AccountName=bitprop;AccountKey=o8sWbnjZ0AwN9CDtGTHZty9GB5CvjpDcWzR8cApW8r/tB8W4N7qVXn2gssTBy3povwxfUM3zanoa+AStW+vIIQ==;EndpointSuffix=core.windows.net')}")
        # logging.info('POINT 9')
        # connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
        connection_string = "DefaultEndpointsProtocol=https;AccountName=bitprop;AccountKey=o8sWbnjZ0AwN9CDtGTHZty9GB5CvjpDcWzR8cApW8r/tB8W4N7qVXn2gssTBy3povwxfUM3zanoa+AStW+vIIQ==;EndpointSuffix=core.windows.net"
        # logging.info('POINT 10')
        container_name = 'homeowner-statements'  # Replace with your container name
        blob_name = file_name  # Blob name in the container
        # logging.info('POINT 11')
        # logging.info(f"Connection String: {connection_string}")
        # logging.info('POINT 12')

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # logging.info(f'blob service client: {blob_service_client}')
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        # logging.info(f'blob client: {blob_client}')
        # logging.info('POINT 13')
        # Check if the blob already exists
        if blob_client.exists():
            # logging.info(f"Blob '{blob_name}' already exists. Deleting the existing blob...")
            blob_client.delete_blob()
            time.sleep(2)
        # logging.info('POINT 14')
        random_string = generate_random_string(8)  # Generate an 8-character random string
        blob_client.upload_blob(pdf_file, content_settings=ContentSettings(content_type='application/pdf'))
        # logging.info('POINT 15')
        # Generate the URL of the uploaded blob
        blob_url = f"{blob_client.url}?random={random_string}"

        # Print the URL for sharing
        # logging.info(f'Blob URL: {blob_url}')

        # Update the 'latest_statement' column in the Properties table
        update_query = "UPDATE Properties SET latest_statement = ? WHERE property_ref = ?"
        cursor.execute(update_query, (blob_url, property_data['property_ref']))
        conn.commit()


    invoice_str = ', '.join(str(invoice[0]) for invoice in invoices)
    invoice_payments_str = ', '.join(str(invoice[14]) for invoice in invoices)
    payout_date = date.today().strftime('%Y-%m-%d')
    # logging.info(f'INVOICE STR TEST {invoice_str}')

    payment_data = {
        'property_id': property_dict['property_id'],
        'homeowner': property_data['homeowner'],
        'month': month,
        'year': as_of[:4],
        'payout_date': payout_date,
        'amount': total_amount,
        'homeowner_amount': homeowner_rent,
        'invoices': invoice_str,
        'invoice_payments': invoice_payments_str,
        'property_ref': property_data['property_ref']
    }    

    if is_final == 1:
        logging.info('is final = 1')
        # Insert the HomeownerPayouts item into the database
        cursor.execute(
            "INSERT INTO HomeownerPayouts (property_id, homeowner, month, year, payout_date, amount, homeowner_amount, invoices, invoice_payments, property_ref) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (payment_data['property_id'], payment_data['homeowner'], payment_data['month'], payment_data['year'], payment_data['payout_date'], payment_data['amount'], payment_data['homeowner_amount'], payment_data['invoices'], payment_data['invoice_payments'], payment_data['property_ref'])
        )
        logging.info('homeownerpayouts row created')
        for invoice in invoices:
            amount_due = invoice[7]
            amount_paid = invoice[8]
            logging.info(f'Amount due: {amount_due} - Amount paid: {amount_paid}')
            # Update the payout status and payout date of the invoice based on the amount due and amount paid
            if amount_due == amount_paid:
                logging.info('amount_due == amount_paid')
                new_payout_status = 'Paid'
            elif amount_paid > 0:
                logging.info('amount_paid > 0')
                new_payout_status = 'Partially Paid'
            else:
                logging.info('default')
                new_payout_status = invoice[12]
            # logging.info(f'PAYOUT STATUS ISSSS: {new_payout_status}')
            # if invoice_dict['amount_due'] == invoice_dict['amount_paid']:
            #     payout_status = 'Paid'
            # elif invoice_dict['amount_due'] > 0:
            #     payout_status = 'Partially Paid'
            # else:
            #     payout_status = invoice_dict['payout_status']

            payout_date = date.today().strftime('%Y-%m-%d')
            
            if invoice[4] <= month:
                logging.info('invoice[4] <= month')
                # logging.info('Invoice month is less than or equal to my month')
            # Update the invoice in the database
                cursor.execute(
                    "UPDATE Invoices SET payout_status = ?, payout_date = ? WHERE invoice_id = ?",
                    (new_payout_status, payout_date, invoice[0]))
                logging.info('Invoice has been updated')
                # Update the included_in_payout and payout_date of the related InvoicePayments
                for payment_id in invoice[14].split(','):
                    logging.info(f'payment id: {payment_id}')
                    cursor.execute(
                        "UPDATE InvoicePayments SET included_in_payout = 1, payout_date = ? WHERE payment_id = ?",
                        (payout_date, payment_id))
                    logging.info('invoice payment has been updated')

    # Optionally, return the statement as an HTML string
    return blob_url

# # Establish a connection to the database
# conn = sqlite3.connect('property_management.db')
# cursor = conn.cursor()


def primary_function(is_final, month, as_of, bulk_comments, sql_input):
    blob_urls = {}
    try:

        # Create a connection to Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        global conn
        conn = pyodbc.connect(conn_str)
        global cursor
        cursor = conn.cursor()
        logging.info('CONNECTED')


        # INPUTS
        property_selected = sql_input
        logging.info(f'property id type: {property_selected}')
        cursor.execute('SELECT * FROM PROPERTIES WHERE property_id = ?', (property_selected,))
        # Fetch the properties
        properties = cursor.fetchall()

        global homeowner_totals_dict
        homeowner_totals_dict = {}
        global property_dict

        for property_data in properties:
            # Convert the tuple to a dictionary
            property_dict = {
                'property_id': property_data[0],
                'property_ref': property_data[1],
                'homeowner': property_data[2],
                'street': property_data[3],
                'suburb': property_data[4],
                'no_units': property_data[5],
                'rent': property_data[6],
                'manual_comments': property_data[7],
                'citiq_elec': property_data[8],
                'citiq_water': property_data[9]
            }

            # logging.info(f"PROPERTY DICT: {property_dict}")

            # Fetch the invoices for the current property and month
            cursor.execute("SELECT * FROM Invoices WHERE property_id = ? AND payout_status IN ('Unpaid', 'Partially Paid') AND invoice_type = ?", (property_dict['property_id'], invoice_type))

            # if invoice_type == 'rent':
            #     cursor.execute("SELECT * FROM Invoices WHERE property_id = ? AND payout_status IN ('Unpaid', 'Partially Paid') AND invoice_type = ?", (property_dict['property_id'], invoice_type))
            # else:
            #     cursor.execute("SELECT * FROM Invoices WHERE property_id = ? AND payout_status IN ('Unpaid', 'Partially Paid') AND invoice_type = ?", (property_dict['property_id'], invoice_type))
            global invoices
            invoices = cursor.fetchall()
            logging.info(f"MY INVOICES: {invoices}")

            # Prepare the invoice data for the property
            invoice_data = []
            for invoice in invoices:
                invoice_row = [
                    invoice[9],  # date_paid
                    invoice[3],  # unit_ref
                    invoice[7],  # amount_due
                    invoice[8],   # amount_paid
                    invoice[12]
                ]
                date_paid = invoice[9]
                unit_ref = invoice[3]
                amount_due = invoice[7]
                amount_paid = invoice[8]
                payout_status = invoice[12]
            # for invoice in invoices:
            #     invoice_dict = dict(zip([column[0] for column in cursor.description], invoice))
            #     invoice_row = [
            #         invoice_dict['date_paid'],
            #         invoice_dict['unit_ref'],
            #         invoice_dict['amount_due'],
            #         invoice_dict['amount_paid']

                invoice_data.append(invoice_row)
                logging.info(f'INVOICE ROW: {invoice_row}')
                    
            # Prepare the property data for statement generation  
            # property_data = {
            #     'homeowner': property_dict['homeowner'],
            #     'property_ref': property_dict['property_ref'],
            #     'street': property_dict['street'],
            #     'suburb': property_dict['suburb'],
            #     'invoices': invoice_data,
            #     'comments': property_dict['manual_comments'],
            #     'elec': property_dict['citiq_elec'],
            #     'water': property_dict['citiq_water']
            # }
            # logging.info(f'PROPERTY DATA: {property_data}')
            # Generate the statement for the current property



            logging.info('is final = 1')
            for invoice in invoices:
                amount_due = invoice[7]
                amount_paid = invoice[8]
                logging.info(f'Amount due: {amount_due} - Amount paid: {amount_paid}')
                # Update the payout status and payout date of the invoice based on the amount due and amount paid
                if amount_due == amount_paid:
                    logging.info('amount_due == amount_paid')
                    new_payout_status = 'Paid'
                elif amount_paid > 0:
                    logging.info('amount_paid > 0')
                    new_payout_status = 'Partially Paid'
                else:
                    logging.info('default')
                    new_payout_status = invoice[12]
                # logging.info(f'PAYOUT STATUS ISSSS: {new_payout_status}')
                # if invoice_dict['amount_due'] == invoice_dict['amount_paid']:
                #     payout_status = 'Paid'
                # elif invoice_dict['amount_due'] > 0:
                #     payout_status = 'Partially Paid'
                # else:
                #     payout_status = invoice_dict['payout_status']

                payout_date = date.today().strftime('%Y-%m-%d')
                
                if invoice[4] <= month:
                    logging.info('invoice[4] <= month')
                    # logging.info('Invoice month is less than or equal to my month')
                # Update the invoice in the database
                    cursor.execute(
                        "UPDATE Invoices SET payout_status = ?, payout_date = ? WHERE invoice_id = ?",
                        (new_payout_status, payout_date, invoice[0]))
                    logging.info('Invoice has been updated')
                    # Update the included_in_payout and payout_date of the related InvoicePayments
                    for payment_id in invoice[14].split(','):
                        logging.info(f'payment id: {payment_id}')
                        cursor.execute(
                            "UPDATE InvoicePayments SET included_in_payout = 1, payout_date = ? WHERE payment_id = ?",
                            (payout_date, payment_id))
                        logging.info('invoice payment has been updated')



            blob_url = generate_statement(property_data, is_final, month, as_of, bulk_comments)

        #     property_info = {
        #        'homeowner': property_data['homeowner'],
        #        'property_ref': property_data['property_ref'],
        #        'statement_url': blob_url
        #     }

        #     blob_urls[property_data['property_ref']] = property_info

        # logging.info(f'LIST OF PROPERTY INFO: {blob_urls}')

        logging.info("Homeowner Totals: ")
        total_payout = 0
        for homeowner, values in homeowner_totals_dict.items():
            homeowner_name, property_ref, homeowner_total = values
            total_payout += homeowner_total
            # logging.info("{} {}: {}".format(homeowner_name, property_ref, homeowner_total))
        # logging.info(f'total_payout: {total_payout}')

        # base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # logging.info('POINT 16')
        # # Construct the path to the template file
        # template_path = "/Users/dylanwalls/Documents/Bitprop/Homeowner Statements/templates/payout_template.html"

        # # Create a Jinja2 environment with the correct template directory
        # env = Environment(loader=FileSystemLoader('templates'))

        # # # Create a Jinja environment with the template file location
        # # env = Environment(loader=FileSystemLoader(os.path.dirname(template_path)), autoescape=True)
        # logging.info('POINT 17')
        # template = env.get_template('payout_template.html')

        # # Prepare the data for the payout table
        # data = {
        #     'title': 'Homeowner Payouts',
        #     'heading': 'Homeowner Payouts',
        #     'content': 'Payouts as of {}'.format(date.today().strftime('%Y-%m-%d')),
        #     'columns': ['Homeowner', 'Amount'],
        #     'table_rows': []
        # }
        # logging.info('POINT 18')
        # payout_page_summary = {}
        # Populate the table rows from the homeowner totals dictionary
        # for homeowner, values in homeowner_totals_dict.items():
        #     homeowner_name, property_ref, homeowner_total = values
        #     row = {
        #         'values': [homeowner_name, property_ref, homeowner_total],
        #         'css_classes': ['', '']  # No CSS classes for payout table cells
        #     }
        #     data['table_rows'].append(row)
        #     payout_page_summary[property_ref] = {
        #         'homeowner': homeowner_name,
        #         'homeowner_total': homeowner_total
        #     }
        # logging.info('POINT 19')




        # # Render the payout table template with the data
        # statement = template.render(data)
        # temp_pdf_path = os.path.join("/tmp", "output.pdf")
        # logging.info('POINT 20')
        # # Generate the file name
        # file_name = 'Homeowner Payouts {}.pdf'.format(date.today().strftime('%Y-%m-%d'))
        # # logging.info('POINT 20a')
        # response = requests.post("https://dashboard-function-app-1.azurewebsites.net/api/createPDF?code=Ixv6ZF_9XBhnHvNSsM64pHqD1xk7hVkc2WWqHTSvxXCZAzFuMB8kRA==", json={"htmlContent": statement})

        # if response.status_code == 200:
        #     with open(temp_pdf_path, "wb") as f:
        #         f.write(response.content)
        #     print(f"PDF file created successfully at: {temp_pdf_path}")
        # else:
        #     print("Failed to create PDF. Status code:", response.status_code)
        #     print("Response body:", response.text)
            
        # pdf_file = response.content
   
        # logging.info('POINT 21')

         # Upload the PDF file to Azure Blob Storage
        # logging.info('POINT 22')
        # logging.info(f"Connection String: {os.environ.get('DefaultEndpointsProtocol=https;AccountName=bitprop;AccountKey=o8sWbnjZ0AwN9CDtGTHZty9GB5CvjpDcWzR8cApW8r/tB8W4N7qVXn2gssTBy3povwxfUM3zanoa+AStW+vIIQ==;EndpointSuffix=core.windows.net')}")
        # logging.info('POINT 23')
        # connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
        # connection_string = "DefaultEndpointsProtocol=https;AccountName=bitprop;AccountKey=o8sWbnjZ0AwN9CDtGTHZty9GB5CvjpDcWzR8cApW8r/tB8W4N7qVXn2gssTBy3povwxfUM3zanoa+AStW+vIIQ==;EndpointSuffix=core.windows.net"
        # logging.info('POINT 24')
        # container_name = 'homeowner-statements'  # Replace with your container name
        # blob_name = file_name  # Blob name in the container
        # # logging.info('POINT 25')
        # logging.info(f"Connection String: {connection_string}")
        # logging.info('POINT 26')

        # blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # # logging.info(f'blob service client: {blob_service_client}')
        # blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        # # logging.info(f'blob client: {blob_client}')
        # # logging.info('POINT 27')
        # # Check if the blob already exists
        # if blob_client.exists():
        #     # logging.info(f"Blob '{blob_name}' already exists. Deleting the existing blob...")
        #     blob_client.delete_blob()
        #     time.sleep(2)
        # # logging.info('POINT 28')
        # blob_client.upload_blob(pdf_file, content_settings=ContentSettings(content_type='application/pdf', cache_control='no-store'))
        # # logging.info('POINT 29')
        # # Generate the URL of the uploaded blob
        # blob_url = blob_client.url

        # Print the URL for sharing
        # logging.info(f'Blob URL: {blob_url}')

        # current_datetime = datetime.now()

        # payout_info = {
        #     'as_of': as_of,
        #     'date_generated': current_datetime,
        #     'payout_data': payout_page_summary
        # }

        # blob_urls['Payout Info'] = payout_info


        # insert_query = '''
        #     INSERT INTO payoutStatements (as_of, date_generated, payout_data) 
        #     VALUES (?, ?, ?)
        # '''
        # cursor.execute(insert_query, (as_of, current_datetime, blob_url))
        # # logging.info(f'as of {as_of}')
        # # logging.info(f'date generate {current_datetime}')
        # # logging.info(f'payout datta {payout_info["payout_data"]}')
        # conn.commit()






        # # Convert the homeowner_totals_dict to a pandas DataFrame
        # homeowner_totals_list = [(values[0], values[1], values[2]) for homeowner, values in homeowner_totals_dict.items()]
        # df = pd.DataFrame(homeowner_totals_list, columns=['Homeowner', 'Property', 'Total'])

        # # Set the file path for the Excel file
        # excel_file_path = os.path.join(subfolder_path, 'Homeowner_Payouts_{}.xlsx'.format(date.today().strftime('%Y-%m-%d')))

        # # Write the DataFrame to an Excel file
        # df.to_excel(excel_file_path, index=False)

        # logging.info(file_path)

        # Commit and close the database connection
        conn.commit()
        conn.close()

        # Return an HTTP response with the blob URL
        return blob_urls

    except Exception as e:
        # logging.error(f'An error occurred: {str(e)}')
        return f"An error occurred: {str(e)}"


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        is_final = req_body.get('final', 0)
        month = req_body.get('month')
        as_of = req_body.get('statementDate')
        bulk_comments = req_body.get('bulkComments')
        sql_input = req_body.get('sqlInput')
        # logging.info(f'IS FINALLL {is_final}')
        # logging.info(f'month type: {type(month)}')
        logging.info(f'sql input: {sql_input}')
        logging.info(f'sql type: {type(sql_input)}')
        logging.info(f'is_final: {is_final} / month: {month} / as_of: {as_of} / bulk_comments: {bulk_comments} / sql_input: {sql_input}')

        statements = primary_function(is_final, month, as_of, bulk_comments, sql_input)
        # logging.info(f'FINAL STATEMENTS: {statements}')
        return 'Statement generation completed successfully'

    except Exception as e:
        import traceback
        logging.error(f'An error occurred: {str(e)}\n{traceback.format_exc()}')
        return func.HttpResponse(f"An error occurred: {str(e)}\n{traceback.format_exc()}", status_code=500)