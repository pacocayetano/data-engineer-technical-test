# Part 2 - Python + GCP + BigQuery

## Description
Python script that retrieves 100 records from a public API and loads them into Google BigQuery.

## API Used
https://jsonplaceholder.typicode.com/comments

## BigQuery Dataset
SANDBOX_api_test

## Table
api_comments

---

## Workflow

The script performs the following steps:

1. Connects to a public REST API  
2. Downloads 100 records in JSON format  
3. Uses a dedicated class for data extraction  
4. Uses a dedicated class for loading data into BigQuery  
5. Inserts the data into a table within the SANDBOX dataset  

---

## Requirements

Install dependencies:

pip install google-cloud-bigquery requests

---

## Configuration (Quick Guide - Windows PowerShell)

Before running the script, environment variables must be configured.

### 1. Configure GCP credentials

$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\credentials.json"

### 2. Configure project variables

$env:GCP_PROJECT_ID="your-project-id"  
$env:BQ_DATASET_ID="SANDBOX_api_test"  
$env:BQ_TABLE_ID="api_comments"  

---

## Execution

From the root of the project:

python parte2\main.py

---

## Expected Result

After execution:

- 100 records are retrieved from the API  
- Data is loaded into BigQuery  
- The table is automatically created (if it does not exist)  
- Data is inserted into:

SANDBOX_api_test.api_comments

---

## Verification

In BigQuery:

1. Open the dataset SANDBOX_api_test  
2. Open the table api_comments  
3. Check the data in the Preview tab  

---

## Important Notes

- Environment variables are only available during the current terminal session  
- Credentials (JSON files) must never be uploaded to the repository  
- Schema is automatically detected by BigQuery  
- Data is loaded using WRITE_APPEND mode  

---

## Architecture

- SANDBOX layer: stores raw, untransformed data  
- Data is loaded exactly as received from the API  
- Transformations will be applied in later stages  
