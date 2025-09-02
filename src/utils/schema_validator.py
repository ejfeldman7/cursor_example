"""
Schema validation utilities to check actual table structures at runtime.
"""

import streamlit as st
from typing import Dict, List, Optional
from ..database_connection import DatabaseConnection


class SchemaValidator:
    """Validates actual table schemas against expected schemas."""
    
    def __init__(self):
        self.db = DatabaseConnection()
    
    def get_table_columns(self, table_name: str) -> Optional[List[str]]:
        """
        Get actual column names from a table.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            
        Returns:
            List of column names or None if failed
        """
        try:
            # Use DESCRIBE to get column info
            query = f"DESCRIBE {table_name}"
            result = self.db.execute_query(query)
            
            if result and not result['data'].empty:
                # Return column names from the describe result
                return result['data']['col_name'].tolist()
            return None
            
        except Exception as e:
            st.warning(f"Could not get columns for {table_name}: {str(e)}")
            return None
    
    def validate_column_exists(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column exists in a table.
        
        Args:
            table_name: Full table name
            column_name: Column name to check
            
        Returns:
            True if column exists, False otherwise
        """
        columns = self.get_table_columns(table_name)
        if columns:
            return column_name in columns
        return False
    
    def get_sample_data(self, table_name: str, limit: int = 1) -> Optional[dict]:
        """
        Get a sample row from the table to understand its structure.
        
        Args:
            table_name: Full table name
            limit: Number of rows to sample
            
        Returns:
            Sample data result or None if failed
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            result = self.db.execute_query(query)
            
            if result and not result['data'].empty:
                return {
                    'columns': list(result['data'].columns),
                    'sample_data': result['data'].to_dict('records')[0] if not result['data'].empty else {},
                    'row_count': len(result['data'])
                }
            return None
            
        except Exception as e:
            st.warning(f"Could not get sample data for {table_name}: {str(e)}")
            return None
    
    def validate_all_tables(self) -> Dict[str, Dict]:
        """
        Validate all tables in the loan_io schema.
        
        Returns:
            Dictionary with validation results for each table
        """
        tables = [
            "efeld_cuj.loan_io.historical_loans",
            "efeld_cuj.loan_io.raw_transactions", 
            "efeld_cuj.loan_io.ref_accounting"
        ]
        
        results = {}
        
        for table in tables:
            table_short_name = table.split('.')[-1]
            st.write(f"🔍 Validating {table_short_name}...")
            
            columns = self.get_table_columns(table)
            sample = self.get_sample_data(table)
            
            results[table_short_name] = {
                'full_name': table,
                'columns': columns,
                'column_count': len(columns) if columns else 0,
                'sample_data': sample,
                'has_loan_amnt': columns and 'loan_amnt' in columns if columns else False,
                'has_balance': columns and 'balance' in columns if columns else False,
            }
            
            # Show key findings
            if columns:
                st.success(f"✅ {table_short_name}: {len(columns)} columns found")
                
                # Show first 10 columns
                st.write(f"**First 10 columns**: {', '.join(columns[:10])}")
                
                # Check for key loan fields
                loan_fields = ['loan_amnt', 'member_id', 'loan_status', 'grade', 'purpose']
                loan_field_status = [f"✅ {field}" if field in columns else f"❌ {field}" 
                                   for field in loan_fields]
                st.write(f"**Loan fields**: {' | '.join(loan_field_status)}")
                
                # Check for key accounting fields  
                acct_fields = ['balance', 'base_rate', 'accrued_interest', 'type', 'status']
                acct_field_status = [f"✅ {field}" if field in columns else f"❌ {field}" 
                                   for field in acct_fields]
                st.write(f"**Accounting fields**: {' | '.join(acct_field_status)}")
                
            else:
                st.error(f"❌ {table_short_name}: Could not retrieve columns")
        
        return results
