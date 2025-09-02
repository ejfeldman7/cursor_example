"""
Accounting transaction queries for the raw_transactions table.
Note: raw_transactions contains accounting data, not loan data!
"""

from ..models.table_schemas import FIELD_MAPPINGS, COMMON_CONDITIONS


class TransactionQueries:
    """Queries related to transaction analysis."""
    
    @staticmethod 
    def get_transaction_summary() -> str:
        """Get overall accounting transaction summary."""
        return f"""
            SELECT 
                COUNT(*) as total_transactions,
                COUNT(DISTINCT accounting_treatment_id) as unique_treatments,
                AVG({FIELD_MAPPINGS['transaction_balance']}) as avg_balance,
                SUM({FIELD_MAPPINGS['transaction_balance']}) as total_balance,
                COUNT(DISTINCT status) as unique_statuses,
                COUNT(DISTINCT type) as unique_types,
                COUNT(DISTINCT purpose) as unique_purposes
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE balance IS NOT NULL AND CAST(balance AS DOUBLE) != 0
        """
    
    @staticmethod
    def get_transaction_by_status() -> str:
        """Get accounting transaction analysis by status."""
        return f"""
            SELECT 
                COALESCE(status, 'Unknown') as transaction_status,
                COUNT(*) as transaction_count,
                AVG({FIELD_MAPPINGS['transaction_balance']}) as avg_balance,
                SUM({FIELD_MAPPINGS['transaction_balance']}) as total_balance,
                AVG({FIELD_MAPPINGS['transaction_interest']}) as avg_accrued_interest,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE balance IS NOT NULL AND CAST(balance AS DOUBLE) != 0
            GROUP BY status
            ORDER BY transaction_count DESC
        """
    
    @staticmethod
    def get_transaction_by_purpose() -> str:
        """Get accounting transaction analysis by purpose."""
        return f"""
            SELECT 
                COALESCE(purpose, 'Unknown') as transaction_purpose,
                COUNT(*) as transaction_count,
                AVG({FIELD_MAPPINGS['transaction_balance']}) as avg_balance,
                SUM({FIELD_MAPPINGS['transaction_balance']}) as total_balance,
                AVG({FIELD_MAPPINGS['transaction_interest']}) as avg_accrued_interest,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE balance IS NOT NULL AND CAST(balance AS DOUBLE) != 0 AND purpose IS NOT NULL
            GROUP BY purpose
            ORDER BY transaction_count DESC
        """
    
    @staticmethod
    def get_payment_analysis() -> str:
        """Get accounting analysis by transaction type."""
        return f"""
            SELECT 
                COALESCE(type, 'Unknown') as transaction_type,
                COUNT(*) as transaction_count,
                AVG({FIELD_MAPPINGS['transaction_balance']}) as avg_balance,
                SUM({FIELD_MAPPINGS['transaction_balance']}) as total_balance,
                AVG({FIELD_MAPPINGS['transaction_interest']}) as avg_accrued_interest,
                SUM({FIELD_MAPPINGS['transaction_interest']}) as total_accrued_interest,
                AVG({FIELD_MAPPINGS['arrears_amount']}) as avg_arrears,
                COUNT(DISTINCT accounting_treatment_id) as unique_treatments
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE balance IS NOT NULL AND CAST(balance AS DOUBLE) != 0
            GROUP BY type
            ORDER BY total_balance DESC
        """
