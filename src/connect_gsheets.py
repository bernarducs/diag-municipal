from apiclient import discovery
from dotenv import dotenv_values
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

ENV = dotenv_values('.env')
SECRET_FILE = ENV['GTOKEN']
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
]


def _create_gdrive_service():
    credentials = Credentials.from_service_account_file(
        SECRET_FILE, 
        scopes=SCOPES
    )
    service = discovery.build('drive', 'v3', credentials=credentials)
    return service


def _create_sheets_service():
    credentials = Credentials.from_service_account_file(
        SECRET_FILE, scopes=SCOPES
    )
    service = discovery.build('sheets', 'v4', credentials=credentials)
    return service


def list_files(page_size):
    try:
        service = _create_gdrive_service()
        results = (
            service.files()
            .list(pageSize=page_size, fields='nextPageToken, files(id, name)')
            .execute()
        )
        items = results.get('files', [])

        if items:
            return {item['name']: item['id'] for item in items}
        else:
            print('No files found.')
            return {}
    except HttpError as error:
        print(f'An error occurred: {error}')


def export_dataset(gsheet_id, dataframe):
    service = _create_sheets_service()
    service_sheet = service.spreadsheets()

    values = dataframe.values.tolist()
    body = {'values': values}
    range_name = 'ExpectativasMercadoAnuais!A2'

    result = (
        service_sheet.values()
        .update(
            spreadsheetId=gsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body,
        )
        .execute()
    )
