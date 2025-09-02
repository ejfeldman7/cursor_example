"""
Database connection management for the Loan Analytics Dashboard.
"""

import os
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from typing import Optional


class DatabaseConnection:
    """Manages database connections and basic operations."""
    
    def __init__(self):
        self.warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
        self.config = Config()
        
    @st.cache_resource(ttl=600)  # Cache for 10 minutes
    def get_connection(_self):
        """
        Get cached database connection.
        
        Returns:
            SQL connection object
        """
        try:
            if not _self.warehouse_id:
                raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable not set")
                
            connection = sql.connect(
                server_hostname=_self.config.host,
                http_path=f"/sql/1.0/warehouses/{_self.warehouse_id}",
                credentials_provider=lambda: _self.config.authenticate
            )
            
            return connection
            
        except Exception as e:
            st.error(f"❌ Database connection failed: {str(e)}")
            st.cache_resource.clear()
            raise e
    
    def execute_query(self, query: str) -> Optional[dict]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Dictionary with 'data' (DataFrame) and 'metadata' (dict) or None if failed
        """
        try:
            connection = self.get_connection()
            
            with connection.cursor() as cursor:
                cursor.execute(query)
                result_df = cursor.fetchall_arrow().to_pandas()
                
                # Get basic metadata
                metadata = {
                    'row_count': len(result_df),
                    'column_count': len(result_df.columns) if not result_df.empty else 0,
                    'columns': list(result_df.columns) if not result_df.empty else []
                }
                
                return {
                    'data': result_df,
                    'metadata': metadata
                }
                
        except Exception as e:
            st.error(f"❌ Query execution failed: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test database connection with a simple query.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            result = self.execute_query("SELECT 1 as test")
            return result is not None and not result['data'].empty
        except Exception as e:
            st.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        Get basic information about a table.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            
        Returns:
            Dictionary with table information or None if failed
        """
        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.execute_query(query)
            
            if result and not result['data'].empty:
                return {
                    'table_name': table_name,
                    'columns': result['data'].to_dict('records'),
                    'column_count': len(result['data'])
                }
            return None
            
        except Exception as e:
            st.warning(f"Could not get table info for {table_name}: {str(e)}")
            return None
