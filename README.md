# Fyl.la database manager

A Streamlit application for cleaning and managing prompts with Google Cloud Storage backend, supporting multi-user collaboration.

## Features

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

All data is stored in Google Cloud Storage as JSON/text files:

- `DATABASE.json`: Unified store for all prompts with parametric data
  - Format: `{prompt, occurrences, craziness?, isSexual?, filler?, madeFor?}`
  - `prompt` is the unique identifier (cleaned text)
  - Parametric fields are added by LLM parameterization
- `USER_SELECTION.json`: Queue of items awaiting human review
  - Format: `{prompt}`
- `DISCARDS.json`: Rejected prompts (kept to avoid re-processing)
  - Format: `{prompt, occurrences}`
- `raw_stripped.txt`: Raw unprocessed prompts
- `REMOVE_LINES.txt`: Filter list for content removal

## Usage

1. **Database Tab**: View and manage all prompts with occurrence counts
2. **Selection Tab**: Review and approve/reject prompts for the global database
3. **Input Tab**: Upload raw content and manage filter lists
4. **Parametrics Tab**: View all entries, run LLM parameterization, edit parametric fields

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## License

This project is private and proprietary.
