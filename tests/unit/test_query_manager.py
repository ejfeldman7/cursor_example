"""Unit tests for query manager functionality."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from query_manager import QueryManager


class TestQueryManager:
    """Test cases for QueryManager class."""
    
    def test_init(self) -> None:
        """Test QueryManager initialization."""
        qm = QueryManager()
        assert qm.db is not None
        assert qm.loan_queries is not None
        assert qm.transaction_queries is not None
        assert qm.accounting_queries is not None
        assert isinstance(qm.query_registry, dict)
        assert len(qm.query_registry) > 0
    
    def test_query_registry_contains_expected_queries(self) -> None:
        """Test that query registry contains expected query types."""
        qm = QueryManager()
        
        expected_queries = [
            'loan_summary',
            'loan_status_distribution',
            'grade_analysis',
            'transaction_summary',
            'accounting_treatment_analysis'
        ]
        
        for query_type in expected_queries:
            assert query_type in qm.query_registry
    
    def test_execute_predefined_query_success(
        self,
        mock_query_result: dict,
        mock_streamlit: Mock
    ) -> None:
        """Test successful predefined query execution."""
        qm = QueryManager()
        
        with patch.object(qm.db, 'execute_query', return_value=mock_query_result):
            result = qm.execute_predefined_query('loan_summary')
        
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
    
    def test_execute_predefined_query_unknown_type(
        self,
        mock_streamlit: Mock
    ) -> None:
        """Test execution with unknown query type."""
        qm = QueryManager()
        
        result = qm.execute_predefined_query('unknown_query')
        
        assert result is None
        mock_streamlit.error.assert_called_once()
    
    def test_execute_predefined_query_db_failure(
        self,
        mock_streamlit: Mock
    ) -> None:
        """Test query execution with database failure."""
        qm = QueryManager()
        
        with patch.object(qm.db, 'execute_query', return_value=None):
            result = qm.execute_predefined_query('loan_summary')
        
        assert result is None
        mock_streamlit.warning.assert_called_once()
    
    def test_execute_predefined_query_exception(
        self,
        mock_streamlit: Mock
    ) -> None:
        """Test query execution with exception."""
        qm = QueryManager()
        
        with patch.object(qm.db, 'execute_query', side_effect=Exception("DB Error")):
            result = qm.execute_predefined_query('loan_summary')
        
        assert result is None
        mock_streamlit.error.assert_called_once()
    
    def test_execute_custom_query_success(
        self,
        mock_query_result: dict
    ) -> None:
        """Test successful custom query execution."""
        qm = QueryManager()
        
        with patch.object(qm.db, 'execute_query', return_value=mock_query_result):
            result = qm.execute_custom_query('SELECT * FROM test_table')
        
        assert result is not None
        assert isinstance(result, pd.DataFrame)
    
    def test_execute_custom_query_failure(
        self,
        mock_streamlit: Mock
    ) -> None:
        """Test custom query execution failure."""
        qm = QueryManager()
        
        with patch.object(qm.db, 'execute_query', return_value=None):
            result = qm.execute_custom_query('SELECT * FROM test_table')
        
        assert result is None
        mock_streamlit.warning.assert_called_once()
    
    def test_test_connection(self) -> None:
        """Test connection testing."""
        qm = QueryManager()
        
        with patch.object(qm.db, 'test_connection', return_value=True):
            result = qm.test_connection()
        
        assert result is True
    
    def test_get_available_queries(self) -> None:
        """Test getting available queries."""
        qm = QueryManager()
        
        queries = qm.get_available_queries()
        
        assert isinstance(queries, dict)
        assert len(queries) > 0
        
        # Check that all query types have descriptions
        for query_type, description in queries.items():
            assert isinstance(description, str)
            assert len(description) > 0
            assert query_type in qm.query_registry
