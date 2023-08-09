#!/bin/sh

echo Setting up environment to be able to connect to your BigQuery instance or your Sqlite database.

# Export environment variables default values
DEFAULT_SQLITE_DATABASE_FILE_PATH="../data/sqlite.db"
DEFAULT_SQLITE_DATABASE_DIRECTORY_PATH="../data"
DEFAULT_GCP_PROJECT_ID="my-project-1234"
DEFAULT_DBT_SERVICE_ACCOUNT_KEYFILE="./service_account_key.json"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --sqlite-database-file-path)
      SQLITE_DATABASE_FILE_PATH=$2
      shift
      ;;
    --sqlite-database-directory-path)
      SQLITE_DATABASE_DIRECTORY_PATH=$2
      shift
      ;;
    --gcp-project-id)
      GCP_PROJECT_ID=$2
      shift
      ;;
    --dbt-service-account-keyfile)
      DBT_SERVICE_ACCOUNT_KEYFILE=$2
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
  shift
done

# Assign default values if not provided
SQLITE_DATABASE_FILE_PATH="${SQLITE_DATABASE_FILE_PATH:-$DEFAULT_SQLITE_DATABASE_FILE_PATH}"
SQLITE_DATABASE_DIRECTORY_PATH="${SQLITE_DATABASE_DIRECTORY_PATH:-$DEFAULT_SQLITE_DATABASE_DIRECTORY_PATH}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-$DEFAULT_GCP_PROJECT_ID}"
DBT_SERVICE_ACCOUNT_KEYFILE="${DBT_SERVICE_ACCOUNT_KEYFILE:-$DEFAULT_DBT_SERVICE_ACCOUNT_KEYFILE}"

# Export environment variables
export SQLITE_DATABASE_FILE_PATH
export SQLITE_DATABASE_DIRECTORY_PATH
export GCP_PROJECT_ID
export DBT_SERVICE_ACCOUNT_KEYFILE

# Make sure settigns are right
echo - Sqlite database file path: $SQLITE_DATABASE_FILE_PATH
echo - Sqlite database directory path: $SQLITE_DATABASE_DIRECTORY_PATH
echo - GCP project is: $GCP_PROJECT_ID
echo - Service account keyfile is at: $DBT_SERVICE_ACCOUNT_KEYFILE
