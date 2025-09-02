"""
Table schema definitions for loan_io dataset.
Defines the structure and key columns for each table.
"""

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class ColumnInfo:
    """Information about a table column."""
    name: str
    data_type: str
    comment: str = None
    nullable: bool = True
    

@dataclass
class TableSchema:
    """Schema definition for a table."""
    table_name: str
    columns: List[ColumnInfo]
    primary_key: str = None
    description: str = ""
    
    def get_column(self, name: str) -> ColumnInfo:
        """Get column info by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def get_numeric_columns(self) -> List[str]:
        """Get list of numeric column names."""
        numeric_types = ['LONG', 'INT', 'DOUBLE', 'FLOAT', 'DECIMAL']
        return [col.name for col in self.columns if col.data_type in numeric_types]
    
    def get_string_columns(self) -> List[str]:
        """Get list of string column names."""
        return [col.name for col in self.columns if col.data_type == 'STRING']
    
    def get_date_columns(self) -> List[str]:
        """Get list of date-like string columns."""
        date_indicators = ['_d', '_date', 'issue_d', 'earliest_cr_line']
        return [col.name for col in self.columns 
                if col.data_type == 'STRING' and 
                any(indicator in col.name for indicator in date_indicators)]


class LoanIOSchema:
    """Schema definitions for the loan_io dataset tables."""
    
    @staticmethod
    def get_historical_loans_schema() -> TableSchema:
        """Schema for historical_loans table."""
        key_columns = [
            ColumnInfo("id", "LONG", "Unique loan identifier"),
            ColumnInfo("member_id", "INT", "Member identifier"),
            ColumnInfo("loan_amnt", "STRING", "Loan amount (cast to numeric)"),
            ColumnInfo("funded_amnt", "STRING", "Funded amount (cast to numeric)"),
            ColumnInfo("term", "STRING", "Loan term"),
            ColumnInfo("int_rate", "STRING", "Interest rate (cast to numeric)"),
            ColumnInfo("grade", "STRING", "Loan grade (A-G)"),
            ColumnInfo("sub_grade", "STRING", "Loan sub-grade"),
            ColumnInfo("purpose", "STRING", "Loan purpose"),
            ColumnInfo("loan_status", "STRING", "Current loan status"),
            ColumnInfo("issue_d", "STRING", "Issue date"),
            ColumnInfo("addr_state", "STRING", "Borrower state"),
            ColumnInfo("annual_inc", "DOUBLE", "Annual income"),
            ColumnInfo("emp_length", "STRING", "Employment length"),
            ColumnInfo("home_ownership", "STRING", "Home ownership status"),
            ColumnInfo("verification_status", "STRING", "Income verification status"),
            ColumnInfo("total_pymnt", "DOUBLE", "Total payments received"),
            ColumnInfo("installment", "DOUBLE", "Monthly installment"),
            ColumnInfo("dti", "DOUBLE", "Debt-to-income ratio"),
            ColumnInfo("accounting_treatment_id", "INT", "Accounting treatment ID"),
        ]
        
        return TableSchema(
            table_name="historical_loans",
            columns=key_columns,
            primary_key="id",
            description="Historical loan data with borrower and loan characteristics"
        )
    
    @staticmethod
    def get_raw_transactions_schema() -> TableSchema:
        """Schema for raw_transactions table - accounting/financial transaction data."""
        key_columns = [
            ColumnInfo("id", "STRING", "Unique transaction identifier"),
            ColumnInfo("accounting_treatment_id", "STRING", "Accounting treatment ID"),
            ColumnInfo("balance", "STRING", "Account balance (cast to numeric)"),
            ColumnInfo("accrued_interest", "STRING", "Accrued interest (cast to numeric)"),
            ColumnInfo("arrears_balance", "STRING", "Arrears balance (cast to numeric)"),
            ColumnInfo("date", "STRING", "Transaction date"),
            ColumnInfo("status", "STRING", "Transaction status"),
            ColumnInfo("type", "STRING", "Transaction type"),
            ColumnInfo("purpose", "STRING", "Transaction purpose"),
            ColumnInfo("state_code", "STRING", "State code"),
            ColumnInfo("base_rate", "STRING", "Base rate"),
            ColumnInfo("cost_center_code", "STRING", "Cost center"),
            ColumnInfo("acc_fv_change_before_taxes", "STRING", "FV change before taxes (cast to numeric)"),
        ]
        
        return TableSchema(
            table_name="raw_transactions",
            columns=key_columns,
            primary_key="id",
            description="Accounting and financial transaction data"
        )
    
    @staticmethod
    def get_ref_accounting_schema() -> TableSchema:
        """Schema for ref_accounting table - simple lookup table."""
        key_columns = [
            ColumnInfo("id", "LONG", "Unique identifier"),
            ColumnInfo("accounting_treatment", "STRING", "Accounting treatment description"),
        ]
        
        return TableSchema(
            table_name="ref_accounting",
            columns=key_columns,
            primary_key="id", 
            description="Reference lookup table for accounting treatments"
        )
    
    @staticmethod
    def get_all_schemas() -> Dict[str, TableSchema]:
        """Get all table schemas."""
        return {
            "historical_loans": LoanIOSchema.get_historical_loans_schema(),
            "raw_transactions": LoanIOSchema.get_raw_transactions_schema(),
            "ref_accounting": LoanIOSchema.get_ref_accounting_schema(),
        }


# Common field mappings for queries
FIELD_MAPPINGS = {
    # Historical loans table mappings
    "loan_amount": "CAST(loan_amnt AS DOUBLE)",
    "interest_rate": "CAST(REPLACE(int_rate, '%', '') AS DOUBLE)",
    "funded_amount": "CAST(funded_amnt AS DOUBLE)", 
    "issue_date": "TO_DATE(issue_d, 'MMM-yyyy')",
    "borrower_id": "member_id",
    
    # Raw transactions table mappings (different schema!)
    "transaction_balance": "CAST(balance AS DOUBLE)",
    "transaction_interest": "CAST(accrued_interest AS DOUBLE)",
    "arrears_amount": "CAST(arrears_balance AS DOUBLE)",
    "fv_change": "CAST(acc_fv_change_before_taxes AS DOUBLE)",
    "transaction_date": "TO_DATE(date, 'yyyy-MM-dd')",
}

# Common filters and conditions
COMMON_CONDITIONS = {
    "valid_loans": "loan_amnt IS NOT NULL AND CAST(loan_amnt AS DOUBLE) > 0",
    "recent_loans": "issue_d IS NOT NULL",
    "valid_status": "loan_status IS NOT NULL",
    "valid_state": "addr_state IS NOT NULL AND addr_state != ''",
    "valid_grade": "grade IS NOT NULL AND grade != ''",
    "valid_purpose": "purpose IS NOT NULL AND purpose != ''",
}
