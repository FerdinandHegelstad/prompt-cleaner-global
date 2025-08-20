# Prompt Cleaner Global

A Streamlit application for cleaning and managing prompts with Google Cloud Storage backend, supporting multi-user collaboration.

## Features

- **Multi-user support**: Up to 5 users can work simultaneously
- **Google Cloud Storage**: Centralized data storage for prompts and user selections
- **Real-time collaboration**: Shared database with optimistic concurrency
- **Streamlit UI**: Clean, intuitive interface for prompt management
- **Cloud deployment**: Deployed on Streamlit Cloud for global access

## Local Development

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up Google Cloud credentials:
   - Create a service account in Google Cloud Console
   - Download the JSON key file as `APT.json`
   - Set environment variable: `export GCS_BUCKET=unfiltered_database`

4. Run locally:
   ```bash
   streamlit run ui.py
   ```

## Cloud Deployment

This app is configured for deployment on Streamlit Cloud:

1. **Secrets Configuration**: Add your Google Cloud credentials to Streamlit Cloud secrets
2. **Environment Variables**: Configure `GCS_BUCKET` in Streamlit Cloud settings
3. **Automatic Deployment**: Connect your GitHub repository to Streamlit Cloud

### Required Secrets (in Streamlit Cloud dashboard):

```toml
[google_cloud]
credentials = """
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "your-private-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "your-client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
}
"""

[gcs]
bucket_name = "unfiltered_database"
```

## Architecture

- **Frontend**: Streamlit web application
- **Backend**: Google Cloud Storage for data persistence
- **Authentication**: Service account-based GCS access
- **Concurrency**: Optimistic locking for multi-user support

## Data Structure

- `gs://unfiltered_database/DATABASE.json`: Global cleaned prompts database
- `gs://unfiltered_database/USER_SELECTION.json`: User selection queue
- `gs://unfiltered_database/raw_stripped.txt`: Raw prompts for processing

## Usage

1. **Global Database Tab**: View and manage all cleaned prompts
2. **User Selection Tab**: Review and approve/reject prompts for the global database
3. **Prompt Distribution Tab**: Analyze prompt length statistics

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## License

This project is private and proprietary.
