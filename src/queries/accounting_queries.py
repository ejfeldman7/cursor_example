"""
Accounting-specific queries for the ref_accounting table.
"""


class AccountingQueries:
    """Queries related to accounting analysis."""
    
    @staticmethod
    def get_accounting_treatment_analysis() -> str:
        """Get accounting treatment analysis using ref_accounting lookup."""
        return """
            SELECT 
                COALESCE(ra.accounting_treatment, 'Unknown') as treatment_type,
                COUNT(rt.id) as transaction_count,
                SUM(CAST(COALESCE(rt.balance, '0') AS DOUBLE)) as total_balance,
                AVG(CAST(COALESCE(rt.balance, '0') AS DOUBLE)) as avg_balance,
                SUM(CAST(COALESCE(rt.accrued_interest, '0') AS DOUBLE)) as total_accrued_interest,
                AVG(CAST(COALESCE(rt.accrued_interest, '0') AS DOUBLE)) as avg_accrued_interest
            FROM efeld_cuj.loan_io.raw_transactions rt
            LEFT JOIN efeld_cuj.loan_io.ref_accounting ra 
                ON CAST(rt.accounting_treatment_id AS LONG) = ra.id
            WHERE rt.balance IS NOT NULL AND rt.balance != ''
            GROUP BY ra.accounting_treatment
            ORDER BY total_balance DESC
        """
    
    @staticmethod
    def get_balance_analysis() -> str:
        """Get balance analysis by transaction type and status."""
        return """
            SELECT 
                COALESCE(type, 'Unknown') as transaction_type,
                COALESCE(status, 'Unknown') as transaction_status,
                COUNT(*) as transaction_count,
                SUM(CAST(COALESCE(balance, '0') AS DOUBLE)) as total_balance,
                AVG(CAST(COALESCE(balance, '0') AS DOUBLE)) as avg_balance,
                MIN(CAST(COALESCE(balance, '0') AS DOUBLE)) as min_balance,
                MAX(CAST(COALESCE(balance, '0') AS DOUBLE)) as max_balance
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE balance IS NOT NULL AND balance != ''
            GROUP BY type, status
            ORDER BY total_balance DESC
        """
    
    @staticmethod
    def get_accounting_by_state() -> str:
        """Get accounting transaction analysis by state."""
        return """
            SELECT 
                COALESCE(state_code, 'Unknown') as state_code,
                COUNT(*) as transaction_count,
                SUM(CAST(COALESCE(balance, '0') AS DOUBLE)) as total_balance,
                AVG(CAST(COALESCE(balance, '0') AS DOUBLE)) as avg_balance,
                SUM(CAST(COALESCE(accrued_interest, '0') AS DOUBLE)) as total_accrued_interest,
                COUNT(DISTINCT type) as transaction_types
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE balance IS NOT NULL AND balance != '' AND state_code IS NOT NULL
            GROUP BY state_code
            ORDER BY total_balance DESC
        """
    
    @staticmethod
    def get_interest_analysis() -> str:
        """Get accrued interest analysis from transactions."""
        return """
            SELECT 
                COALESCE(type, 'Unknown') as transaction_type,
                COUNT(*) as transaction_count,
                SUM(CAST(COALESCE(accrued_interest, '0') AS DOUBLE)) as total_accrued_interest,
                AVG(CAST(COALESCE(accrued_interest, '0') AS DOUBLE)) as avg_accrued_interest,
                SUM(CAST(COALESCE(balance, '0') AS DOUBLE)) as total_balance,
                CASE 
                    WHEN SUM(CAST(COALESCE(balance, '0') AS DOUBLE)) > 0 
                    THEN SUM(CAST(COALESCE(accrued_interest, '0') AS DOUBLE)) / SUM(CAST(COALESCE(balance, '0') AS DOUBLE)) * 100
                    ELSE 0
                END as interest_to_balance_ratio
            FROM efeld_cuj.loan_io.raw_transactions
            WHERE accrued_interest IS NOT NULL AND accrued_interest != '' 
                AND balance IS NOT NULL AND balance != ''
            GROUP BY type
            ORDER BY total_accrued_interest DESC
        """
