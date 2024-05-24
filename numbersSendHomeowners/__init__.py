import json
import pyodbc
import logging
import azure.functions as func
import requests

# Define your Azure SQL Database connection parameters
username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Create a connection to Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        req_body = req.get_json()

        id_param = req_body.get('id')

        # Construct the SQL query based on the 'id' parameter or use the default query
        if id_param:
            query = f'SELECT * FROM Properties WHERE property_id = {id_param}'
        else:
            query = 'SELECT * FROM Properties WHERE property_id = 146'

        # Execute the SQL query
        cursor.execute(query)

        homeowners = cursor.fetchall()
        homeowner_dictionary = []

        # Prepare a list of dictionaries for sending WhatsApp messages
        for homeowner in homeowners:
            property_id = homeowner[0]
            property_ref = homeowner[1]
            name = homeowner[2]
            latest_statement = homeowner[10]
            phone = homeowner[11]

            if phone:
                homeowner_dictionary.append({
                    'name': name,
                    'latest_statement': latest_statement,
                    'phone': phone
                })

        # Logging the data to be sent
        logging.info(f'Sending homeowner data to WhatsApp function: {homeowner_dictionary}')

        # POST the entire homeowner_dictionary to the WhatsApp function in one go
        whatsapp_url = "https://dashboard-function-app-1.azurewebsites.net/api/whatsappHomeowners?code=Uk-G4FVd_gU4Gd5Hsl1OKL5FMFSz7i7-xdG9Zg3-P_XWAzFueMce9w=="
        headers = {'Content-Type': 'application/json'}
        response = requests.post(whatsapp_url, headers=headers, data=json.dumps(homeowner_dictionary))

        # Check response from WhatsApp function
        if response.status_code == 200:
            logging.info('Successfully triggered WhatsApp function.')
            return func.HttpResponse(f"Successfully triggered WhatsApp function with data: {homeowner_dictionary}", status_code=200)
        else:
            logging.error(f'Error while triggering WhatsApp function: {response.status_code} {response.text}')
            return func.HttpResponse(f"Error while triggering WhatsApp function: {response.status_code} {response.text}", status_code=response.status_code)

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
