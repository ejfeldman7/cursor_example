"""End-to-end integration tests."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from query_manager import QueryManager
from src import execute_predefined_query, get_query_manager


class TestEndToEndIntegration:
    """Integration tests for complete workflows."""
    
    @pytest.mark.integration
    def test_complete_query_workflow(
        self,
        mock_query_result: dict,
        mock_streamlit: Mock
    ) -> None:
        """Test complete query execution workflow."""
        # Test the complete flow from module-level function to database
        with patch('src.query_manager.QueryManager') as mock_qm_class:
            mock_qm = Mock()
            mock_qm.query_registry = {'loan_summary': Mock(return_value='SELECT 1')}
            mock_qm.db.execute_query.return_value = mock_query_result
            mock_qm_class.return_value = mock_qm
            
            result = execute_predefined_query('loan_summary')
            
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            mock_qm.db.execute_query.assert_called_once()
    
    @pytest.mark.integration
    def test_query_manager_integration(
        self,
        mock_query_result: dict
    ) -> None:
        """Test QueryManager integration with database layer."""
        qm = QueryManager()
        
        # Mock the database connection
        with patch.object(qm.db, 'execute_query', return_value=mock_query_result):
            result = qm.execute_predefined_query('loan_summary')
        
        assert result is not None
        assert isinstance(result, pd.DataFrame)
    
    @pytest.mark.integration
    def test_schema_and_query_integration(self) -> None:
        """Test that queries use proper schema definitions."""
        qm = QueryManager()
        
        # Test that loan queries contain proper field mappings
        loan_summary_query = qm.loan_queries.get_loan_summary()
        
        assert 'CAST(loan_amnt AS DOUBLE)' in loan_summary_query
        assert 'efeld_cuj.loan_io.historical_loans' in loan_summary_query
    
    @pytest.mark.integration
    def test_transaction_queries_use_correct_schema(self) -> None:
        """Test that transaction queries use the correct schema."""
        qm = QueryManager()
        
        transaction_summary_query = qm.transaction_queries.get_transaction_summary()
        
        assert 'CAST(balance AS DOUBLE)' in transaction_summary_query
        assert 'efeld_cuj.loan_io.raw_transactions' in transaction_summary_query
        assert 'accounting_treatment_id' in transaction_summary_query
    
    @pytest.mark.integration 
    def test_accounting_queries_with_joins(self) -> None:
        """Test that accounting queries properly join tables."""
        qm = QueryManager()
        
        accounting_query = qm.accounting_queries.get_accounting_treatment_analysis()
        
        assert 'JOIN' in accounting_query
        assert 'ref_accounting' in accounting_query
        assert 'raw_transactions' in accounting_query
    
    @pytest.mark.integration
    @pytest.mark.slow
    def test_all_predefined_queries_generate_valid_sql(self) -> None:
        """Test that all predefined queries generate valid SQL syntax."""
        qm = QueryManager()
        
        for query_type, query_func in qm.query_registry.items():
            if query_type in ['overview', 'transactions', 'accounting']:
                continue  # Skip legacy aliases
                
            query_sql = query_func()
            
            # Basic SQL validation
            assert isinstance(query_sql, str)
            assert len(query_sql.strip()) > 0
            assert 'SELECT' in query_sql.upper()
            assert 'FROM' in query_sql.upper()
            
            # Check for proper table references
            if 'loan' in query_type:
                assert 'efeld_cuj.loan_io.historical_loans' in query_sql
            elif 'transaction' in query_type:
                assert 'efeld_cuj.loan_io.raw_transactions' in query_sql
            elif 'accounting' in query_type:
                assert ('efeld_cuj.loan_io.ref_accounting' in query_sql or 
                       'efeld_cuj.loan_io.raw_transactions' in query_sql)
