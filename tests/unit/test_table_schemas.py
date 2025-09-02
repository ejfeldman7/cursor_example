"""Unit tests for table schema definitions."""

import pytest
from typing import List

from models.table_schemas import (
    ColumnInfo,
    TableSchema,
    LoanIOSchema,
    FIELD_MAPPINGS,
    COMMON_CONDITIONS,
)


class TestColumnInfo:
    """Test cases for ColumnInfo dataclass."""
    
    def test_column_info_creation(self) -> None:
        """Test creating a ColumnInfo instance."""
        col = ColumnInfo("test_col", "STRING", "Test column")
        
        assert col.name == "test_col"
        assert col.data_type == "STRING"
        assert col.comment == "Test column"
        assert col.nullable is True
    
    def test_column_info_defaults(self) -> None:
        """Test ColumnInfo with default values."""
        col = ColumnInfo("test_col", "LONG")
        
        assert col.name == "test_col"
        assert col.data_type == "LONG"
        assert col.comment is None
        assert col.nullable is True


class TestTableSchema:
    """Test cases for TableSchema dataclass."""
    
    def test_table_schema_creation(self) -> None:
        """Test creating a TableSchema instance."""
        columns = [
            ColumnInfo("id", "LONG", "Unique ID"),
            ColumnInfo("name", "STRING", "Name field"),
        ]
        schema = TableSchema("test_table", columns, "id", "Test table")
        
        assert schema.table_name == "test_table"
        assert len(schema.columns) == 2
        assert schema.primary_key == "id"
        assert schema.description == "Test table"
    
    def test_get_column_exists(self) -> None:
        """Test getting an existing column."""
        columns = [ColumnInfo("id", "LONG"), ColumnInfo("name", "STRING")]
        schema = TableSchema("test_table", columns)
        
        col = schema.get_column("name")
        assert col is not None
        assert col.name == "name"
        assert col.data_type == "STRING"
    
    def test_get_column_not_exists(self) -> None:
        """Test getting a non-existing column."""
        columns = [ColumnInfo("id", "LONG")]
        schema = TableSchema("test_table", columns)
        
        col = schema.get_column("nonexistent")
        assert col is None
    
    def test_get_numeric_columns(self) -> None:
        """Test getting numeric columns."""
        columns = [
            ColumnInfo("id", "LONG"),
            ColumnInfo("name", "STRING"),
            ColumnInfo("amount", "DOUBLE"),
            ColumnInfo("count", "INT"),
        ]
        schema = TableSchema("test_table", columns)
        
        numeric_cols = schema.get_numeric_columns()
        assert len(numeric_cols) == 3
        assert "id" in numeric_cols
        assert "amount" in numeric_cols
        assert "count" in numeric_cols
        assert "name" not in numeric_cols
    
    def test_get_string_columns(self) -> None:
        """Test getting string columns."""
        columns = [
            ColumnInfo("id", "LONG"),
            ColumnInfo("name", "STRING"),
            ColumnInfo("description", "STRING"),
        ]
        schema = TableSchema("test_table", columns)
        
        string_cols = schema.get_string_columns()
        assert len(string_cols) == 2
        assert "name" in string_cols
        assert "description" in string_cols
        assert "id" not in string_cols
    
    def test_get_date_columns(self) -> None:
        """Test getting date-like columns."""
        columns = [
            ColumnInfo("id", "LONG"),
            ColumnInfo("issue_d", "STRING"),
            ColumnInfo("created_date", "STRING"),
            ColumnInfo("name", "STRING"),
        ]
        schema = TableSchema("test_table", columns)
        
        date_cols = schema.get_date_columns()
        assert len(date_cols) == 2
        assert "issue_d" in date_cols
        assert "created_date" in date_cols
        assert "name" not in date_cols


class TestLoanIOSchema:
    """Test cases for LoanIOSchema class."""
    
    def test_get_historical_loans_schema(self) -> None:
        """Test getting historical loans schema."""
        schema = LoanIOSchema.get_historical_loans_schema()
        
        assert schema.table_name == "historical_loans"
        assert schema.primary_key == "id"
        assert len(schema.columns) > 10
        assert "Historical loan data" in schema.description
        
        # Check for key columns
        id_col = schema.get_column("id")
        assert id_col is not None
        assert id_col.data_type == "LONG"
        
        loan_amnt_col = schema.get_column("loan_amnt")
        assert loan_amnt_col is not None
        assert loan_amnt_col.data_type == "STRING"
    
    def test_get_raw_transactions_schema(self) -> None:
        """Test getting raw transactions schema."""
        schema = LoanIOSchema.get_raw_transactions_schema()
        
        assert schema.table_name == "raw_transactions"
        assert schema.primary_key == "id"
        assert len(schema.columns) > 5
        assert "Accounting and financial transaction data" in schema.description
        
        # Check for key columns
        balance_col = schema.get_column("balance")
        assert balance_col is not None
        assert balance_col.data_type == "STRING"
    
    def test_get_ref_accounting_schema(self) -> None:
        """Test getting ref accounting schema."""
        schema = LoanIOSchema.get_ref_accounting_schema()
        
        assert schema.table_name == "ref_accounting"
        assert schema.primary_key == "id"
        assert len(schema.columns) == 2
        assert "Reference lookup table" in schema.description
    
    def test_get_all_schemas(self) -> None:
        """Test getting all schemas."""
        schemas = LoanIOSchema.get_all_schemas()
        
        assert len(schemas) == 3
        assert "historical_loans" in schemas
        assert "raw_transactions" in schemas
        assert "ref_accounting" in schemas
        
        # Verify each schema is correct type
        for schema in schemas.values():
            assert isinstance(schema, TableSchema)


class TestFieldMappings:
    """Test cases for field mappings."""
    
    def test_field_mappings_exist(self) -> None:
        """Test that field mappings are defined."""
        assert isinstance(FIELD_MAPPINGS, dict)
        assert len(FIELD_MAPPINGS) > 0
    
    def test_loan_amount_mapping(self) -> None:
        """Test loan amount field mapping."""
        assert "loan_amount" in FIELD_MAPPINGS
        assert "CAST(loan_amnt AS DOUBLE)" in FIELD_MAPPINGS["loan_amount"]
    
    def test_transaction_balance_mapping(self) -> None:
        """Test transaction balance field mapping."""
        assert "transaction_balance" in FIELD_MAPPINGS
        assert "CAST(balance AS DOUBLE)" in FIELD_MAPPINGS["transaction_balance"]


class TestCommonConditions:
    """Test cases for common conditions."""
    
    def test_common_conditions_exist(self) -> None:
        """Test that common conditions are defined."""
        assert isinstance(COMMON_CONDITIONS, dict)
        assert len(COMMON_CONDITIONS) > 0
    
    def test_valid_loans_condition(self) -> None:
        """Test valid loans condition."""
        assert "valid_loans" in COMMON_CONDITIONS
        condition = COMMON_CONDITIONS["valid_loans"]
        assert "loan_amnt IS NOT NULL" in condition
        assert "CAST(loan_amnt AS DOUBLE) > 0" in condition
    
    def test_valid_status_condition(self) -> None:
        """Test valid status condition."""
        assert "valid_status" in COMMON_CONDITIONS
        condition = COMMON_CONDITIONS["valid_status"]
        assert "loan_status IS NOT NULL" in condition
