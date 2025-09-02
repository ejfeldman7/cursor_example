# Deployment Guide

This guide explains how to deploy the Loan Analytics Dashboard to your Databricks workspace.

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Databricks CLI** installed and configured
3. **SQL Warehouse** running and accessible
4. **Service Principal** with appropriate permissions
5. **Data Access** to the `efeld_cuj.loan_io` tables

## Quick Start

### 1. Clone and Configure

```bash
# Clone the repository
git clone <repo-url>
cd mcp_demo

# Configure your workspace details
vim databricks.yml  # Update workspace.host
vim app.yaml        # Update DATABRICKS_HOST and DATABRICKS_WAREHOUSE_ID
```

### 2. Set Up Authentication

Ensure your Databricks CLI is configured:

```bash
databricks configure
# or
databricks auth login
```

### 3. Deploy

```bash
# Deploy the bundle
databricks bundle deploy --target dev --profile DEFAULT

# Deploy the Streamlit app
databricks apps deploy loan-analytics-dashboard-v2 --source-code-path "/Workspace/Users/<your-email>/.bundle/loan-analytics-dashboard/dev/files" --profile DEFAULT
```

## Configuration Files

### `databricks.yml`
- Updates the `workspace.host` to your Databricks workspace URL
- Configures bundle name and deployment targets

### `app.yaml`
- Set `DATABRICKS_HOST` to your workspace URL
- Set `DATABRICKS_WAREHOUSE_ID` to your SQL warehouse ID
- Configure any additional environment variables

### `requirements.txt`
- Python dependencies for the Streamlit app
- Modify if you need additional packages

## Permissions Required

The service principal needs:
- **SQL Warehouse Access**: `CAN_USE` permissions on the target warehouse
- **Unity Catalog Access**: `SELECT` permissions on:
  - `efeld_cuj.loan_io.historical_loans`
  - `efeld_cuj.loan_io.raw_transactions`
  - `efeld_cuj.loan_io.ref_accounting`

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify your Databricks CLI profile is active
   - Check service principal permissions

2. **SQL Warehouse Not Found**
   - Verify the warehouse ID in `app.yaml`
   - Ensure the warehouse is running

3. **Table Access Denied**
   - Check Unity Catalog permissions
   - Verify table names and schema

4. **Bundle Deployment Fails**
   - Ensure workspace host is correct
   - Check if you have bundle deployment permissions

### Getting Help

- Check app logs in the Databricks workspace
- Use `databricks apps logs <app-name>` for debugging
- Verify permissions with your Databricks administrator

## Production Deployment

For production deployment:

1. **Use the `prod` target**: `--target prod`
2. **Set up proper RBAC**: Configure appropriate user access
3. **Monitor performance**: Set up alerting for app health
4. **Version control**: Tag releases and maintain deployment history

## Security Best Practices

- Use service principals instead of personal access tokens
- Limit app permissions to minimum required scope
- Regularly rotate service principal credentials
- Monitor app access and usage patterns
