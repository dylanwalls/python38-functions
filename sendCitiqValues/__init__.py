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
        # Parse the request body
        req_body = req.get_json()
        logging.info(f'req_body: {req_body}')
        property_ref = req_body.get('property_ref')
        send_to_all = req_body.get('send_to_all', False)

        # Create a connection to Azure SQL Database
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        if send_to_all:
            # If sending to all, select all properties directly
            query = 'SELECT phone, citiq_elec, citiq_water FROM Properties WHERE is_active = 1'
            cursor.execute(query)
        else:
            property_id = None
            if property_ref:
                # Retrieve property_id using property_ref
                property_id_query = 'SELECT property_id FROM Properties WHERE property_ref = ?'
                cursor.execute(property_id_query, (property_ref,))
                row = cursor.fetchone()
                if row:
                    property_id = row[0]

            if property_id:
                # Use the retrieved property_id to select properties
                query = 'SELECT phone, citiq_elec, citiq_water FROM Properties WHERE property_id = ?'
                cursor.execute(query, (property_id,))
            else:
                # Handle the case where property_ref is not provided or not found
                # This could be an error, or you might want to default to some behavior
                return func.HttpResponse("Property reference not provided or not found", status_code=400)

        properties = cursor.fetchall()
        recipients = []

        # Prepare the data structure for sending WhatsApp messages
        for property in properties:
            phone, citiq_elec, citiq_water = property

            # Handle NULL values from the database
            citiq_elec = 0 if citiq_elec is None else citiq_elec
            citiq_water = 0 if citiq_water is None else citiq_water

            if citiq_elec > 0 or citiq_water > 0:
                total = citiq_elec + citiq_water
                citiq_elec_formatted = "{:.2f}".format(citiq_elec)
                citiq_water_formatted = "{:.2f}".format(citiq_water)
                total_formatted = "{:.2f}".format(total)

                if phone:
                    recipient_data = {
                        'phone': phone.strip(),
                        'hsm_id': 156856,
                        'parameters': [
                            {'key': "{{1}}", 'value': str(total_formatted)},
                            {'key': "{{2}}", 'value': str(citiq_elec_formatted)},
                            {'key': "{{3}}", 'value': str(citiq_water_formatted)}
                        ]
                    }
                    recipients.append(recipient_data)

        messages_data = {'recipients': recipients}

        logging.info(f'Sending properties data to WhatsApp function: {json.dumps(messages_data)}')

        whatsapp_url = "https://dashboard-function-app-1.azurewebsites.net/api/sendWhatsappTemplate?code=HMT7Whg8vQmL9C_lOTjFZ0ILLhoLZORZXAd_myIXRdv1AzFuq4O4FQ=="
        headers = {'Content-Type': 'application/json'}
        response = requests.post(whatsapp_url, headers=headers, data=json.dumps(messages_data))

        if response.status_code == 200:
            logging.info('Successfully triggered WhatsApp function.')
            return func.HttpResponse(f"Successfully triggered WhatsApp function with data: {json.dumps(messages_data)}", status_code=200)
        else:
            logging.error(f'Error while triggering WhatsApp function: {response.status_code} {response.text}')
            return func.HttpResponse(f"Error while triggering WhatsApp function: {response.status_code} {response.text}", status_code=response.status_code)

    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
