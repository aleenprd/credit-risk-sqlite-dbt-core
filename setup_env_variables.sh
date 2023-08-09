#!/bin/sh

echo Setting up environment to be able to connect to your BigQuery instance.

# Export environment variables default values
DEFAULT_GCP_PROJECT_ID="my-project-1234"
DEFAULT_DBT_SERVICE_ACCOUNT_KEYFILE="./service_account_key.json"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
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
GCP_PROJECT_ID="${GCP_PROJECT_ID:-$DEFAULT_GCP_PROJECT_ID}"
echo $GCP_PROJECT_ID
DBT_SERVICE_ACCOUNT_KEYFILE="${DBT_SERVICE_ACCOUNT_KEYFILE:-$DEFAULT_DBT_SERVICE_ACCOUNT_KEYFILE}"

# Export environment variables
export GCP_PROJECT_ID
export DBT_SERVICE_ACCOUNT_KEYFILE

# Make sure settigns are right
echo - GCP project is: $GCP_PROJECT_ID
echo - Service account keyfile is at: $DBT_SERVICE_ACCOUNT_KEYFILE
