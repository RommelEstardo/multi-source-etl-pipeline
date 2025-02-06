# multi-source-etl-pipeline

# Multi-Source ETL Pipeline

ETL pipeline for automated data ingestion from S3, SFTP, local files, and URLs into SQL databases with secure credential management.

## Features

- Multiple data source support:
  - Amazon S3 buckets
  - SFTP servers
  - Local file system
  - URL endpoints
- File format support: CSV, JSON, TXT
- Secure credential management via AWS Parameter Store
- Multiple import methods: BCP and Pandas
- Automatic table and view creation
- Email notifications for process status
- Comprehensive logging
- ZIP file extraction
- Configurable via INI files

## Prerequisites

- Python 3.x
- SQL Server
- AWS account (for S3 and Parameter Store)
- Required Python packages:
  ```
  boto3
  pandas
  paramiko
  pyodbc
  requests
  sqlalchemy
  ```

## Configuration

Create configuration files for each data source type following these templates:
- `config_s3.ini` for Amazon S3
- `config_sftp.ini` for SFTP servers
- `config_local.ini` for local files
- `config_url.ini` for URL endpoints

### Setup AWS Parameter Store

1. Configure parameters in AWS Parameter Store:
```python
python setAWSparameter.py
```

2. Update parameters using the parameter store management script:
```python
python manageParameterStore.py
```

## Usage

Run the appropriate script based on your data source:

```bash
# For S3 source
python etlS3.py

# For SFTP source
python etlSFTP.py

# For local files
python etlLocal.py

# For URL endpoints
python etlURL.py
```

## Configuration Files

Each source type requires specific configuration in its INI file. Example structure:

```ini
[ETL]
data_source_type = sftp
database_type = mssql
file_type = csv
field_delimiter = ,
file_has_header = True

[IMPORT_METHOD]
bcp_import = True
pandas_import = False

[MSSQL]
server = your_server
database = your_database
user = your_user
table_name = your_table
```

## Security

- Credentials are managed through AWS Parameter Store
- SFTP and SQL passwords are stored securely
- Email credentials are protected
- Supports both SQL authentication and Windows authentication

## Error Handling

- Comprehensive logging
- Email notifications for success/failure
- Error recovery mechanisms
- Archive functionality for processed files
