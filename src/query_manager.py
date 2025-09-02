"""
Query management and execution for the Loan Analytics Dashboard.
"""

import pandas as pd
import streamlit as st
from typing import Optional, Dict, Any

from .database_connection import DatabaseConnection
from .queries.loan_queries import LoanQueries
from .queries.transaction_queries import TransactionQueries
from .queries.accounting_queries import AccountingQueries


class QueryManager:
    """Manages query execution and caching for the dashboard."""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.loan_queries = LoanQueries()
        self.transaction_queries = TransactionQueries()
        self.accounting_queries = AccountingQueries()
        
        # Define available query types
        self.query_registry = {
            # Loan analysis queries
            "loan_summary": self.loan_queries.get_loan_summary,
            "loan_status_distribution": self.loan_queries.get_loan_status_distribution,
            "grade_analysis": self.loan_queries.get_grade_analysis,
            "purpose_analysis": self.loan_queries.get_purpose_analysis,
            "state_distribution": self.loan_queries.get_state_distribution,
            "monthly_loan_trend": self.loan_queries.get_monthly_loan_trend,
            "risk_analysis": self.loan_queries.get_risk_analysis,
            "employment_analysis": self.loan_queries.get_employment_analysis,
            
            # Transaction analysis queries
            "transaction_summary": self.transaction_queries.get_transaction_summary,
            "transaction_by_status": self.transaction_queries.get_transaction_by_status,
            "transaction_by_purpose": self.transaction_queries.get_transaction_by_purpose,
            "payment_analysis": self.transaction_queries.get_payment_analysis,
            
            # Accounting queries
            "accounting_treatment_analysis": self.accounting_queries.get_accounting_treatment_analysis,
            "balance_analysis": self.accounting_queries.get_balance_analysis,
            "accounting_by_state": self.accounting_queries.get_accounting_by_state,
            "interest_analysis": self.accounting_queries.get_interest_analysis,
            
            # Legacy query names for backward compatibility
            "overview": self.loan_queries.get_loan_summary,
            "loan_status": self.loan_queries.get_loan_status_distribution,
            "monthly_trends": self.loan_queries.get_monthly_loan_trend,
            "transactions": self.transaction_queries.get_transaction_summary,
            "accounting": self.accounting_queries.get_accounting_treatment_analysis,
        }
    
    def execute_predefined_query(self, query_type: str) -> Optional[pd.DataFrame]:
        """
        Execute a predefined query by type.
        
        Args:
            query_type: Type of query to execute
            
        Returns:
            DataFrame with query results or None if failed
        """
        if query_type not in self.query_registry:
            st.error(f"❌ Unknown query type: {query_type}")
            st.info(f"Available query types: {', '.join(self.query_registry.keys())}")
            return None
        
        try:
            # Get the query function and execute it
            query_func = self.query_registry[query_type]
            query_sql = query_func()
            
            # Add some debugging info
            with st.expander(f"🔍 Query Details: {query_type}", expanded=False):
                st.code(query_sql, language="sql")
            
            # Execute the query with caching
            result = self._execute_query_cached(query_sql, query_type)
            
            if result and result['data'] is not None:
                df = result['data']
                
                # Show some metadata
                with st.expander(f"📊 Results Info: {query_type}", expanded=False):
                    st.write(f"**Rows**: {result['metadata']['row_count']}")
                    st.write(f"**Columns**: {result['metadata']['column_count']}")
                    if result['metadata']['columns']:
                        st.write(f"**Column Names**: {', '.join(result['metadata']['columns'])}")
                
                return df
            else:
                st.warning(f"⚠️ Query '{query_type}' returned no results")
                return None
                
        except Exception as e:
            st.error(f"❌ Failed to execute query '{query_type}': {str(e)}")
            return None
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes  
    def _execute_query_cached(_self, query_sql: str, query_type: str) -> Optional[dict]:
        """Execute a query with caching."""
        return _self.db.execute_query(query_sql)
    
    def execute_custom_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute a custom SQL query.
        
        Args:
            query: Custom SQL query string
            
        Returns:
            DataFrame with query results or None if failed
        """
        try:
            result = self.db.execute_query(query)
            
            if result and result['data'] is not None:
                return result['data']
            else:
                st.warning("⚠️ Custom query returned no results")
                return None
                
        except Exception as e:
            st.error(f"❌ Custom query failed: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        return self.db.test_connection()
    
    def get_available_queries(self) -> Dict[str, str]:
        """
        Get list of available predefined queries.
        
        Returns:
            Dictionary mapping query types to descriptions
        """
        descriptions = {
            # Loan queries (from historical_loans table)
            "loan_summary": "📊 Overall loan portfolio summary with key metrics",
            "loan_status_distribution": "📈 Distribution of loans by status",
            "grade_analysis": "🏆 Analysis by loan grade (A-G)",
            "purpose_analysis": "🎯 Analysis by loan purpose",
            "state_distribution": "🗺️ Geographic distribution by state",
            "monthly_loan_trend": "📅 Monthly loan origination trends",
            "risk_analysis": "⚠️ Risk analysis by grade and status",
            "employment_analysis": "👔 Analysis by employment length",
            
            # Accounting transaction queries (from raw_transactions table)
            "transaction_summary": "💰 Overall accounting transaction summary",
            "transaction_by_status": "📊 Accounting transactions by status",
            "transaction_by_purpose": "🎯 Accounting transactions by purpose", 
            "payment_analysis": "🏦 Accounting analysis by transaction type",
            
            # Accounting treatment queries (joins raw_transactions + ref_accounting)
            "accounting_treatment_analysis": "📋 Accounting treatment analysis with lookups",
            "balance_analysis": "💳 Balance analysis by type and status",
            "accounting_by_state": "🗺️ Accounting transactions by state",
            "interest_analysis": "📈 Accrued interest analysis",
        }
        
        return descriptions
