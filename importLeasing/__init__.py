import azure.functions as func
import io
import openpyxl
import json
import logging
import os
import pyodbc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


username = 'dylanwalls'
password = '950117Dy!'
server = 'scorecard-server.database.windows.net'
database = 'dashboard-new-server'
driver = '{ODBC Driver 17 for SQL Server}'


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.basicConfig(level=logging.INFO)

        logging.info("Request received")

        if 'excelFile' not in req.files:
            logging.error("Missing 'excelFile' in request")
            return func.HttpResponse("Missing 'excelFile' in request.", status_code=400)

        # Get the uploaded file
        file = req.files['excelFile']
        logging.info("Excel file found in request")

        # Read the uploaded file content as bytes
        file_content = file.read()

        # Create a BytesIO object to load the content into openpyxl
        file_content_io = io.BytesIO(file_content)

        # Load the Excel file from BytesIO
        workbook = openpyxl.load_workbook(file_content_io)

        # Assuming the data is in the first sheet of the Excel file
        worksheet = workbook.active

        # Connect to your Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        logging.info("Connected to SQL Server")
        cursor = conn.cursor()

        # Fetch all existing lease IDs
        cursor.execute('SELECT leaseId FROM Leases')
        existing_leases = {row[0] for row in cursor.fetchall()}

        # Track leases marked as inactive for the response
        terminated_leases = []

        importTimestamp = datetime.now()
        
        # Iterate over the rows of the Excel sheet and insert each lease into the database
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            statusDateTime = datetime.strptime(row[9].split(".")[0], '%Y-%m-%dT%H:%M:%S')
            statusDate = statusDateTime.date()
            lastPaymentReceived = datetime.strptime(row[13].split(".")[0], '%Y-%m-%dT%H:%M:%S')
            logging.info("Parsed dates from Excel")

            # Insert lease with a timestamp
            insert_lease(cursor, row, statusDate, lastPaymentReceived, importTimestamp)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        response_body = {"message": "Processed successfully."}

        return func.HttpResponse(body=json.dumps(response_body), status_code=200, headers={"Content-Type": "application/json"})

    except Exception as e:
        logging.error('An error occurred: %s', str(e))
        response_body = {"message": f"Error: {str(e)}"}
        return func.HttpResponse(body=json.dumps(response_body), status_code=500, headers={"Content-Type": "application/json"})


def parse_date_from_isoformat(date_str):
    # Parse the ISO 8601 format datetime string if it contains time information
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        # If the string does not contain time information, parse it as date only
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.date()

def insert_lease(cursor, row, statusDate, lastPaymentReceived, importTimestamp):
    # Other columns assignment omitted for brevity
    buildingNo = row[0]
    logging.info(f"buildingNo: {buildingNo}")
    buildingName = row[1]
    logging.info(f"buildingName: {buildingName}")
    owner = row[2]
    logging.info(f"owner: {owner}")
    refNo = row[3]
    logging.info(f"refNo: {refNo}")
    unitNo = row[4]
    logging.info(f"unitNo: {unitNo}")
    currentRent = row[5]
    logging.info(f"currentRent: {currentRent}")
    deposit = row[6]
    logging.info(f"deposit: {deposit}")
    status = row[7]
    logging.info(f"status: {status}")
    statusDesc = row[8]
    logging.info(f"statusDesc: {statusDesc}")
    statusDate_str = str(statusDate)  # Convert statusDate to string
    logging.info(f"statusDate: {statusDate_str}")
    listedDays = row[10]
    logging.info(f"listedDays: {listedDays}")
    daysRemainingStatus = row[11]
    logging.info(f"daysRemainingStatus: {daysRemainingStatus}")
    paymentMethodDesc = row[12]
    logging.info(f"paymentMethodDesc: {paymentMethodDesc}")
    lastPaymentReceived_str = str(lastPaymentReceived)  # Convert lastPaymentReceived to string
    logging.info(f"lastPaymentReceived: {lastPaymentReceived_str}")
    credit = row[14]
    logging.info(f"credit: {credit}")
    daysRemaining = row[15]
    logging.info(f"daysRemaining: {daysRemaining}")
    tenantName = row[16]
    logging.info(f"tenantName: {tenantName}")
    tenantMobileNo = row[17]
    logging.info(f"tenantMobileNo: {tenantMobileNo}")
    tenantEmail = row[18]
    logging.info(f"tenantEmail: {tenantEmail}")
    tenantIdNo = row[19]
    logging.info(f"tenantIdNo: {tenantIdNo}")
    unitType = row[20]
    logging.info(f"unitType: {unitType}")
    architectureType = row[21]
    logging.info(f"architectureType: {architectureType}")
    thumbnail = row[22]
    logging.info(f"thumbnail: {thumbnail}")
    floorLevel = row[23]
    logging.info(f"floorLevel: {floorLevel}")
    createdOn = row[24]
    logging.info(f"createdOn: {createdOn}")
    isInvestorUnit = row[25]
    logging.info(f"isInvestorUnit: {isInvestorUnit}")
    qrCode = row[26]
    logging.info(f"qrCode: {qrCode}")
    leaseId = row[27]
    logging.info(f"leaseId: {leaseId}")
    isDeleted = row[28]
    logging.info(f"isDeleted: {isDeleted}")
    extRef = row[29]
    logging.info(f"extRef: {extRef}")

    # Convert createdOn to datetime object
    createdOn = datetime.strptime(row[24].split(".")[0], '%Y-%m-%dT%H:%M:%S')

    if statusDate.day < 15:
        firstMonthDate = statusDate.replace(day=1)
    else:
        firstMonthDate = statusDate.replace(day=1) + relativedelta(months=1)
    rentEscalationDate = firstMonthDate.replace(year=firstMonthDate.year + 1)
    logging.info(f"firstMonthDate: {firstMonthDate} // {type(firstMonthDate)}")
    logging.info(f"rentEscalationDate: {rentEscalationDate} // {type(rentEscalationDate)}")

    logging.info(f"statusDate type: {type(statusDate)}")
    logging.info(f"lastPaymentReceived type: {type(lastPaymentReceived)}")

    cursor.execute('''
        INSERT INTO Leases (buildingNo, buildingName, owner, refNo, unitNo, currentRent, deposit, status, statusDesc, statusDate, listedDays, daysRemainingStatus, paymentMethodDesc, lastPaymentReceived, credit, daysRemaining, tenantName, tenantMobileNo, tenantEmail, tenantIdNo, unitType, architectureType, thumbnail, floorLevel, createdOn, isInvestorUnit, qrCode, leaseId, isDeleted, extRef, firstMonthDate, rentEscalationDate, importTimestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (buildingNo, buildingName, owner, refNo, unitNo, currentRent, deposit, status, statusDesc, statusDate, listedDays, daysRemainingStatus, paymentMethodDesc, lastPaymentReceived, credit, daysRemaining, tenantName, tenantMobileNo, tenantEmail, tenantIdNo, unitType, architectureType, thumbnail, floorLevel, createdOn, isInvestorUnit, qrCode, leaseId, isDeleted, extRef, firstMonthDate, rentEscalationDate, importTimestamp))




# def update_lease(cursor, row, statusDate, lastPaymentReceived, leaseId):
#     # Other columns assignment omitted for brevity
#     buildingNo = row[0]
#     buildingName = row[1]
#     owner = row[2]
#     refNo = row[3]
#     unitNo = row[4]
#     currentRent = row[5]
#     deposit = row[6]
#     status = row[7]
#     statusDesc = row[8]
#     listedDays = row[10]
#     daysRemainingStatus = row[11]
#     paymentMethodDesc = row[12]
#     credit = row[14]
#     daysRemaining = row[15]
#     tenantName = row[16]
#     tenantMobileNo = row[17]
#     tenantEmail = row[18]
#     tenantIdNo = row[19]
#     unitType = row[20]
#     architectureType = row[21]
#     thumbnail = row[22]
#     floorLevel = row[23]
#     createdOn = row[24]
#     isInvestorUnit = row[25]
#     qrCode = row[26]
#     isDeleted = row[28]
#     extRef = row[29]

#     # Convert createdOn to datetime object
#     createdOn = datetime.strptime(row[24].split(".")[0], '%Y-%m-%dT%H:%M:%S')

#     if statusDate.day < 15:
#         firstMonthDate = statusDate.replace(day=1)
#     else:
#         firstMonthDate = statusDate.replace(day=1) + relativedelta(months=1)

#     rentEscalationDate = firstMonthDate.replace(year=firstMonthDate.year + 1)

#     cursor.execute('''
#         UPDATE Leases
#         SET buildingNo=?, buildingName=?, owner=?, refNo=?, unitNo=?, currentRent=?, deposit=?, status=?, statusDesc=?, statusDate=?, listedDays=?, daysRemainingStatus=?, paymentMethodDesc=?, lastPaymentReceived=?, credit=?, daysRemaining=?, tenantName=?, tenantMobileNo=?, tenantEmail=?, tenantIdNo=?, unitType=?, architectureType=?, thumbnail=?, floorLevel=?, createdOn=?, isInvestorUnit=?, qrCode=?, isDeleted=?, extRef=?, firstMonthDate=?, rentEscalationDate=?
#         WHERE leaseId=?
#     ''', (buildingNo, buildingName, owner, refNo, unitNo, currentRent, deposit, status, statusDesc, statusDate, listedDays, daysRemainingStatus, paymentMethodDesc, lastPaymentReceived, credit, daysRemaining, tenantName, tenantMobileNo, tenantEmail, tenantIdNo, unitType, architectureType, thumbnail, floorLevel, createdOn, isInvestorUnit, qrCode, isDeleted, extRef, firstMonthDate, rentEscalationDate, leaseId))
