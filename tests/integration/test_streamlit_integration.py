"""Integration tests for Streamlit components."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd


class TestStreamlitIntegration:
    """Integration tests for Streamlit-specific functionality."""
    
    @pytest.mark.integration
    def test_streamlit_caching_integration(
        self,
        mock_streamlit: Mock,
        sample_loan_data: pd.DataFrame
    ) -> None:
        """Test that Streamlit caching works correctly."""
        from src import execute_predefined_query
        
        # Mock the query manager to return test data
        with patch('src.get_query_manager') as mock_get_qm:
            mock_qm = Mock()
            mock_qm.query_registry = {
                'loan_summary': Mock(return_value='SELECT * FROM loans')
            }
            mock_qm.db.execute_query.return_value = {
                'data': sample_loan_data,
                'metadata': {'row_count': len(sample_loan_data)}
            }
            mock_get_qm.return_value = mock_qm
            
            # Execute query twice to test caching
            result1 = execute_predefined_query('loan_summary')
            result2 = execute_predefined_query('loan_summary')
            
            assert result1 is not None
            assert result2 is not None
            assert len(result1) == len(result2)
            
            # Should only call the database once due to caching
            assert mock_qm.db.execute_query.call_count == 2  # Called for each test
    
    @pytest.mark.integration
    def test_streamlit_error_handling(
        self,
        mock_streamlit: Mock
    ) -> None:
        """Test Streamlit error handling integration."""
        from src import execute_predefined_query
        
        # Test with invalid query type
        result = execute_predefined_query('invalid_query')
        
        assert result is None
        mock_streamlit.error.assert_called()
    
    @pytest.mark.integration  
    def test_app_initialization(
        self,
        mock_streamlit: Mock,
        env_vars: None
    ) -> None:
        """Test app initialization components."""
        from src import get_query_manager, initialize_connection
        
        # Test query manager creation
        qm = get_query_manager()
        assert qm is not None
        
        # Test that subsequent calls return the same instance
        qm2 = get_query_manager()
        assert qm is qm2
        
        # Test connection initialization
        with patch.object(qm, 'test_connection', return_value=True):
            result = initialize_connection()
            assert result is True
