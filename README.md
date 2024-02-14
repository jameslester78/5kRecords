# 5kRecords
webscraping 5km course records, formating and uploading summary data to a google sheet

# Generate Sheets credentials

- https://console.developers.google.com/
- Create New project or add to existing project
- Add Google Sheets and Google Drive API
- Create service account
- Create json key for service account (will be auto downloaded)
- Grant permission on the spreadsheet to the service account that you just created (use email shown on account screen or in key file)

# Gmail credential pickle file

- https://console.developers.google.com/
- Add Gmail Api to project
- Create OAuth client ID
- Add "Send email on your behalf" scope
- Desktop App
- Add yourself as a test user
- pickle the file using the pickle.py file found here: https://github.com/jameslester78/SecretSanta/blob/main/picklingCode.py
