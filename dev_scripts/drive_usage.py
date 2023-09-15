import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
import io
from googleapiclient.errors import HttpError

scope = ['https://www.googleapis.com/auth/drive']
service_account_json_key = '/home/gillian/dev/topo-satromo/secrets/geetest-credentials.secret'
credentials = service_account.Credentials.from_service_account_file(
                              filename=service_account_json_key,
                              scopes=scope)


service = build('drive', 'v3', credentials=credentials)
results = service.files().list(pageSize=1000, fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, trashed)").execute()

items = results.get('files', [])
"""
for item in items:
    service.files().delete(fileId=item['id']).execute()"""

print([x["trashed"] for x in items])
about = service.about().get(fields="*").execute()
result_storage = about.get("storageQuota", {})
usage_ratio = float(result_storage["usage"])/float(result_storage["limit"])
print(usage_ratio)