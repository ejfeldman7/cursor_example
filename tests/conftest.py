"""Pytest configuration and shared fixtures."""

import os
import sys
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

# Add src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture
def mock_databricks_config() -> Generator[Mock, None, None]:
    """Mock Databricks configuration."""
    with pytest.MonkeyPatch.context() as m:
        mock_config = Mock()
        mock_config.host = "https://test-workspace.cloud.databricks.com"
        mock_config.authenticate = "mock_auth_token"
        
        m.setattr("databricks.sdk.core.Config", lambda: mock_config)
        yield mock_config


@pytest.fixture
def mock_streamlit() -> Generator[Mock, None, None]:
    """Mock Streamlit functions to avoid GUI dependencies in tests."""
    with pytest.MonkeyPatch.context() as m:
        mock_st = Mock()
        mock_st.cache_resource = lambda ttl=None: lambda func: func
        mock_st.cache_data = lambda ttl=None: lambda func: func
        mock_st.error = Mock()
        mock_st.warning = Mock()
        mock_st.success = Mock()
        mock_st.info = Mock()
        
        m.setattr("streamlit", mock_st)
        yield mock_st


@pytest.fixture
def sample_loan_data() -> pd.DataFrame:
    """Sample loan data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'member_id': [101, 102, 103, 104, 105],
        'loan_amnt': ['10000', '15000', '20000', '25000', '30000'],
        'funded_amnt': ['10000', '15000', '20000', '25000', '30000'],
        'int_rate': ['10.5%', '12.0%', '8.5%', '15.2%', '9.8%'],
        'grade': ['B', 'C', 'A', 'D', 'A'],
        'sub_grade': ['B2', 'C1', 'A3', 'D1', 'A2'],
        'loan_status': ['Fully Paid', 'Current', 'Fully Paid', 'Charged Off', 'Current'],
        'purpose': ['debt_consolidation', 'home_improvement', 'credit_card', 'car', 'vacation'],
        'addr_state': ['CA', 'NY', 'TX', 'FL', 'WA'],
        'annual_inc': [50000.0, 75000.0, 60000.0, 45000.0, 80000.0],
        'dti': [15.5, 20.2, 12.8, 25.3, 18.7],
        'issue_d': ['Jan-2023', 'Feb-2023', 'Mar-2023', 'Apr-2023', 'May-2023'],
        'total_pymnt': [10500.0, 5000.0, 20800.0, 8000.0, 12000.0],
        'installment': [300.0, 450.0, 600.0, 750.0, 900.0],
        'accounting_treatment_id': [1, 2, 1, 3, 2],
    })


@pytest.fixture
def sample_transaction_data() -> pd.DataFrame:
    """Sample transaction data for testing."""
    return pd.DataFrame({
        'id': ['T1', 'T2', 'T3', 'T4', 'T5'],
        'accounting_treatment_id': ['1', '2', '1', '3', '2'],
        'balance': ['1000.50', '2500.75', '1800.25', '3200.00', '950.33'],
        'accrued_interest': ['50.25', '125.30', '90.15', '160.00', '47.50'],
        'arrears_balance': ['0.00', '100.00', '0.00', '500.00', '0.00'],
        'date': ['2023-01-15', '2023-01-20', '2023-01-25', '2023-01-30', '2023-02-05'],
        'status': ['Active', 'Active', 'Closed', 'Overdue', 'Active'],
        'type': ['Interest', 'Principal', 'Fee', 'Principal', 'Interest'],
        'purpose': ['payment', 'payment', 'fee', 'payment', 'payment'],
        'state_code': ['CA', 'NY', 'TX', 'FL', 'WA'],
    })


@pytest.fixture
def sample_accounting_ref_data() -> pd.DataFrame:
    """Sample accounting reference data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'accounting_treatment': ['Standard', 'Premium', 'Special'],
    })


@pytest.fixture
def mock_sql_connection() -> Generator[Mock, None, None]:
    """Mock SQL connection for testing."""
    mock_connection = Mock()
    mock_cursor = Mock()
    
    # Mock cursor context manager
    mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = Mock(return_value=None)
    
    yield mock_connection


@pytest.fixture
def mock_query_result() -> Dict[str, Any]:
    """Mock query result structure."""
    return {
        'data': pd.DataFrame({
            'test_column': [1, 2, 3],
            'another_column': ['A', 'B', 'C']
        }),
        'metadata': {
            'row_count': 3,
            'column_count': 2,
            'columns': ['test_column', 'another_column']
        }
    }


@pytest.fixture
def env_vars() -> Generator[None, None, None]:
    """Set up test environment variables."""
    test_env = {
        'DATABRICKS_WAREHOUSE_ID': 'test_warehouse_id',
        'DATABRICKS_HOST': 'https://test-workspace.cloud.databricks.com',
    }
    
    # Store original values
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value
    
    yield
    
    # Restore original values
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value
