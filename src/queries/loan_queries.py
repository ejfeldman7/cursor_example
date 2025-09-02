"""
Loan-specific queries for the historical_loans table.
"""

from ..models.table_schemas import FIELD_MAPPINGS, COMMON_CONDITIONS


class LoanQueries:
    """Queries related to loan analysis."""

    @staticmethod
    def get_loan_summary() -> str:
        """Get overall loan portfolio summary."""
        return f"""
            SELECT 
                COUNT(*) as total_loans,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_loan_amount,
                SUM({FIELD_MAPPINGS["loan_amount"]}) as total_loan_amount,
                COUNT(DISTINCT member_id) as unique_borrowers,
                COUNT(DISTINCT grade) as unique_grades,
                MIN({FIELD_MAPPINGS["loan_amount"]}) as min_loan_amount,
                MAX({FIELD_MAPPINGS["loan_amount"]}) as max_loan_amount,
                STDDEV({FIELD_MAPPINGS["loan_amount"]}) as loan_amount_stddev
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]}
        """

    @staticmethod
    def get_loan_status_distribution() -> str:
        """Get loan status distribution."""
        return f"""
            SELECT 
                loan_status,
                COUNT(*) as count,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_amount,
                SUM({FIELD_MAPPINGS["loan_amount"]}) as total_amount,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]} AND {COMMON_CONDITIONS["valid_status"]}
            GROUP BY loan_status
            ORDER BY count DESC
        """

    @staticmethod
    def get_grade_analysis() -> str:
        """Get loan grade analysis."""
        return f"""
            SELECT 
                grade,
                COUNT(*) as loan_count,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_loan_amount,
                SUM({FIELD_MAPPINGS["loan_amount"]}) as total_amount,
                AVG({FIELD_MAPPINGS["interest_rate"]}) as avg_interest_rate,
                AVG(annual_inc) as avg_annual_income,
                AVG(dti) as avg_debt_to_income,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]} AND {COMMON_CONDITIONS["valid_grade"]}
            GROUP BY grade
            ORDER BY grade
        """

    @staticmethod
    def get_purpose_analysis() -> str:
        """Get loan purpose analysis."""
        return f"""
            SELECT 
                purpose,
                COUNT(*) as loan_count,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_loan_amount,
                SUM({FIELD_MAPPINGS["loan_amount"]}) as total_amount,
                AVG({FIELD_MAPPINGS["interest_rate"]}) as avg_interest_rate,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]} AND {COMMON_CONDITIONS["valid_purpose"]}
            GROUP BY purpose
            ORDER BY loan_count DESC
        """

    @staticmethod
    def get_state_distribution() -> str:
        """Get geographic distribution by state."""
        return f"""
            SELECT 
                addr_state,
                COUNT(*) as loan_count,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_loan_amount,
                SUM({FIELD_MAPPINGS["loan_amount"]}) as total_amount,
                AVG({FIELD_MAPPINGS["interest_rate"]}) as avg_interest_rate,
                AVG(annual_inc) as avg_annual_income
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]} AND {COMMON_CONDITIONS["valid_state"]}
            GROUP BY addr_state
            ORDER BY loan_count DESC
        """

    @staticmethod
    def get_monthly_loan_trend() -> str:
        """Get monthly loan origination trends."""
        return f"""
            SELECT 
                issue_d as month,
                COUNT(*) as loan_count,
                SUM({FIELD_MAPPINGS["loan_amount"]}) as total_amount,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_amount,
                AVG({FIELD_MAPPINGS["interest_rate"]}) as avg_interest_rate
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]} 
                AND {COMMON_CONDITIONS["recent_loans"]}
            GROUP BY issue_d
            ORDER BY issue_d
            LIMIT 50
        """

    @staticmethod
    def get_risk_analysis() -> str:
        """Get loan risk analysis by grade and status."""
        return f"""
            SELECT 
                grade,
                loan_status,
                COUNT(*) as loan_count,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_loan_amount,
                AVG({FIELD_MAPPINGS["interest_rate"]}) as avg_interest_rate,
                AVG(dti) as avg_debt_to_income,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY grade), 2) as status_percentage_in_grade
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]} 
                AND {COMMON_CONDITIONS["valid_grade"]}
                AND {COMMON_CONDITIONS["valid_status"]}
            GROUP BY grade, loan_status
            ORDER BY grade, loan_count DESC
        """

    @staticmethod
    def get_employment_analysis() -> str:
        """Get analysis by employment length."""
        return f"""
            SELECT 
                CASE 
                    WHEN emp_length = '< 1 year' THEN '< 1 year'
                    WHEN emp_length = '1 year' THEN '1 year'
                    WHEN emp_length IN ('2 years', '3 years', '4 years') THEN '2-4 years'
                    WHEN emp_length IN ('5 years', '6 years', '7 years', '8 years', '9 years') THEN '5-9 years'
                    WHEN emp_length = '10+ years' THEN '10+ years'
                    ELSE 'Unknown'
                END as emp_length_group,
                COUNT(*) as loan_count,
                AVG({FIELD_MAPPINGS["loan_amount"]}) as avg_loan_amount,
                AVG(annual_inc) as avg_annual_income,
                AVG({FIELD_MAPPINGS["interest_rate"]}) as avg_interest_rate,
                AVG(dti) as avg_debt_to_income
            FROM efeld_cuj.loan_io.historical_loans
            WHERE {COMMON_CONDITIONS["valid_loans"]}
            GROUP BY emp_length_group
            ORDER BY 
                CASE emp_length_group
                    WHEN '< 1 year' THEN 1
                    WHEN '1 year' THEN 2
                    WHEN '2-4 years' THEN 3
                    WHEN '5-9 years' THEN 4
                    WHEN '10+ years' THEN 5
                    ELSE 6
                END
        """
