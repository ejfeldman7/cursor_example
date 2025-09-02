"""Unit tests for database connection functionality."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from database_connection import DatabaseConnection


class TestDatabaseConnection:
    """Test cases for DatabaseConnection class."""
    
    def test_init_with_env_vars(self, env_vars: None) -> None:
        """Test initialization with environment variables."""
        db = DatabaseConnection()
        assert db.warehouse_id == 'test_warehouse_id'
        assert db.config is not None
    
    def test_init_without_warehouse_id(self) -> None:
        """Test initialization without warehouse ID."""
        with patch.dict('os.environ', {}, clear=True):
            db = DatabaseConnection()
            assert db.warehouse_id is None
    
    @patch('database_connection.sql')
    def test_get_connection_success(
        self, 
        mock_sql: Mock, 
        mock_databricks_config: Mock,
        env_vars: None
    ) -> None:
        """Test successful database connection."""
        mock_connection = Mock()
        mock_sql.connect.return_value = mock_connection
        
        db = DatabaseConnection()
        connection = db.get_connection()
        
        assert connection == mock_connection
        mock_sql.connect.assert_called_once()
    
    @patch('database_connection.sql')
    def test_get_connection_no_warehouse_id(
        self, 
        mock_sql: Mock,
        mock_streamlit: Mock
    ) -> None:
        """Test connection failure when warehouse ID is missing."""
        with patch.dict('os.environ', {}, clear=True):
            db = DatabaseConnection()
            
            with pytest.raises(ValueError, match="DATABRICKS_WAREHOUSE_ID"):
                db.get_connection()
    
    @patch('database_connection.sql')
    def test_get_connection_failure(
        self, 
        mock_sql: Mock,
        mock_streamlit: Mock,
        env_vars: None
    ) -> None:
        """Test connection failure handling."""
        mock_sql.connect.side_effect = Exception("Connection failed")
        
        db = DatabaseConnection()
        
        with pytest.raises(Exception, match="Connection failed"):
            db.get_connection()
    
    def test_execute_query_success(
        self,
        mock_sql_connection: Mock,
        sample_loan_data: pd.DataFrame,
        env_vars: None
    ) -> None:
        """Test successful query execution."""
        # Setup mock cursor to return sample data
        mock_cursor = mock_sql_connection.cursor.return_value.__enter__.return_value
        mock_arrow_table = Mock()
        mock_arrow_table.to_pandas.return_value = sample_loan_data
        mock_cursor.fetchall_arrow.return_value = mock_arrow_table
        
        db = DatabaseConnection()
        
        # Mock the get_connection method
        with patch.object(db, 'get_connection', return_value=mock_sql_connection):
            result = db.execute_query("SELECT * FROM test_table")
        
        assert result is not None
        assert 'data' in result
        assert 'metadata' in result
        assert len(result['data']) == 5
        assert result['metadata']['row_count'] == 5
        assert result['metadata']['column_count'] == len(sample_loan_data.columns)
    
    def test_execute_query_failure(
        self,
        mock_sql_connection: Mock,
        mock_streamlit: Mock,
        env_vars: None
    ) -> None:
        """Test query execution failure handling."""
        mock_sql_connection.cursor.side_effect = Exception("Query failed")
        
        db = DatabaseConnection()
        
        with patch.object(db, 'get_connection', return_value=mock_sql_connection):
            result = db.execute_query("SELECT * FROM test_table")
        
        assert result is None
        mock_streamlit.error.assert_called_once()
    
    def test_test_connection_success(
        self,
        mock_sql_connection: Mock,
        env_vars: None
    ) -> None:
        """Test successful connection test."""
        # Mock successful query execution
        mock_cursor = mock_sql_connection.cursor.return_value.__enter__.return_value
        mock_arrow_table = Mock()
        test_df = pd.DataFrame({'test': [1]})
        mock_arrow_table.to_pandas.return_value = test_df
        mock_cursor.fetchall_arrow.return_value = mock_arrow_table
        
        db = DatabaseConnection()
        
        with patch.object(db, 'get_connection', return_value=mock_sql_connection):
            result = db.test_connection()
        
        assert result is True
    
    def test_test_connection_failure(
        self,
        mock_streamlit: Mock,
        env_vars: None
    ) -> None:
        """Test connection test failure."""
        db = DatabaseConnection()
        
        with patch.object(db, 'execute_query', return_value=None):
            result = db.test_connection()
        
        assert result is False
    
    def test_get_table_info_success(
        self,
        mock_sql_connection: Mock,
        env_vars: None
    ) -> None:
        """Test successful table info retrieval."""
        # Mock describe table result
        describe_df = pd.DataFrame({
            'col_name': ['id', 'name', 'value'],
            'data_type': ['LONG', 'STRING', 'DOUBLE'],
            'comment': [None, 'Name field', 'Value field']
        })
        
        mock_cursor = mock_sql_connection.cursor.return_value.__enter__.return_value
        mock_arrow_table = Mock()
        mock_arrow_table.to_pandas.return_value = describe_df
        mock_cursor.fetchall_arrow.return_value = mock_arrow_table
        
        db = DatabaseConnection()
        
        with patch.object(db, 'get_connection', return_value=mock_sql_connection):
            result = db.get_table_info("test.schema.table")
        
        assert result is not None
        assert result['table_name'] == "test.schema.table"
        assert result['column_count'] == 3
        assert len(result['columns']) == 3
    
    def test_get_table_info_failure(
        self,
        mock_streamlit: Mock,
        env_vars: None
    ) -> None:
        """Test table info retrieval failure."""
        db = DatabaseConnection()
        
        with patch.object(db, 'execute_query', return_value=None):
            result = db.get_table_info("test.schema.table")
        
        assert result is None
        mock_streamlit.warning.assert_called_once()
