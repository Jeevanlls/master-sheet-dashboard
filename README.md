# Master Sheet Dashboard Streamlit App

This repository contains a Streamlit application for managing eBay and Prime transaction data. The app allows users to upload transaction files, process data into a master sheet, and view reports such as owner payout summaries and dispute tracking. It uses a local SQLite database (`master_sheet.db`) to store processed data.  The database file will be created automatically at runtime if it does not exist.

## Files Included

| File | Purpose |
| --- | --- |
| `app.py` | Main Streamlit app file. |
| `prime-dark.svg` | Logo displayed in the app. |
| `requirements.txt` | Python dependencies for deploying the app. |

## Deployment

This app is designed to be deployed on [Streamlit Community Cloud](https://streamlit.io/cloud). To deploy:

1. Create a new repository on GitHub and push the files in this repository to it.
2. Log into your Streamlit Community Cloud account and click **Deploy new app**.
3. Connect your GitHub account and select the repository containing these files.
4. Set **`app.py`** as the main file to run.
5. Click **Deploy**.

On first run the app will create an empty `master_sheet.db` SQLite database. You can then upload your transaction files and begin using the dashboard.