"""
Loan Analytics Dashboard - Database and Query Management

This module provides a well-structured approach to database connectivity
and query management for the loan analytics dashboard.

Main Components:
- DatabaseConnection: Handles Databricks SQL connections
- QueryManager: Manages predefined and custom queries  
- Table Schemas: Defines data structures and field mappings
- Query Classes: Organized SQL queries by domain (loans, transactions, accounting)
"""

import streamlit as st
from .query_manager import QueryManager
from .database_connection import DatabaseConnection

# Create a global query manager instance
_query_manager = None

def get_query_manager() -> QueryManager:
    """Get the global query manager instance."""
    global _query_manager
    if _query_manager is None:
        _query_manager = QueryManager()
    return _query_manager

# Convenience functions for backward compatibility
@st.cache_data(ttl=300)  # Cache for 5 minutes
def execute_predefined_query(query_type: str):
    """Execute a predefined query with caching at the function level."""
    # Get query manager and execute directly to avoid instance caching issues
    qm = get_query_manager()
    
    # Check if query type exists
    if query_type not in qm.query_registry:
        st.error(f"❌ Unknown query type: {query_type}")
        return None
    
    try:
        # Get the query function and execute it
        query_func = qm.query_registry[query_type]
        query_sql = query_func()
        
        # Execute the query directly
        result = qm.db.execute_query(query_sql)
        
        if result and result['data'] is not None:
            return result['data']
        else:
            st.warning(f"⚠️ Query '{query_type}' returned no results")
            return None
            
    except Exception as e:
        st.error(f"❌ Failed to execute query '{query_type}': {str(e)}")
        return None

def execute_custom_query(query: str):
    """Execute a custom query."""
    return get_query_manager().execute_custom_query(query)

def initialize_connection() -> bool:
    """Initialize and test database connection."""
    return get_query_manager().test_connection()

# Export key classes and functions
__all__ = [
    'QueryManager',
    'DatabaseConnection', 
    'get_query_manager',
    'execute_predefined_query',
    'execute_custom_query',
    'initialize_connection'
]