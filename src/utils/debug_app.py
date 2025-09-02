"""
Debug utilities for the Loan Analytics Dashboard.
Quick debugging and schema validation app.
"""

import streamlit as st
from .schema_validator import SchemaValidator


def run_debug_app():
    """Run the debug application to validate schemas and test queries."""
    
    st.title("🔍 Loan Analytics Debug Tool")
    st.write("Use this tool to validate table schemas and debug query issues.")
    
    validator = SchemaValidator()
    
    # Schema validation section
    st.header("📊 Table Schema Validation")
    
    if st.button("🔍 Validate All Table Schemas"):
        with st.spinner("Validating table schemas..."):
            results = validator.validate_all_tables()
            
            # Summary
            st.subheader("📋 Validation Summary")
            for table_name, info in results.items():
                if info['columns']:
                    st.success(f"✅ **{table_name}**: {info['column_count']} columns")
                else:
                    st.error(f"❌ **{table_name}**: Schema validation failed")
    
    # Individual table testing
    st.header("🔧 Individual Table Testing")
    
    table_choice = st.selectbox(
        "Select table to test:",
        [
            "efeld_cuj.loan_io.historical_loans",
            "efeld_cuj.loan_io.raw_transactions", 
            "efeld_cuj.loan_io.ref_accounting"
        ]
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📝 Get Columns"):
            with st.spinner(f"Getting columns for {table_choice}..."):
                columns = validator.get_table_columns(table_choice)
                if columns:
                    st.success(f"Found {len(columns)} columns:")
                    st.json(columns)
                else:
                    st.error("Failed to get columns")
    
    with col2:
        if st.button("🔍 Get Sample Data"):
            with st.spinner(f"Getting sample data from {table_choice}..."):
                sample = validator.get_sample_data(table_choice)
                if sample:
                    st.success(f"Sample data retrieved:")
                    st.json(sample)
                else:
                    st.error("Failed to get sample data")
    
    with col3:
        if st.button("✅ Check Key Fields"):
            with st.spinner(f"Checking key fields in {table_choice}..."):
                columns = validator.get_table_columns(table_choice)
                if columns:
                    # Check different field types
                    loan_fields = ['loan_amnt', 'member_id', 'loan_status', 'grade', 'purpose', 'int_rate']
                    acct_fields = ['balance', 'base_rate', 'accrued_interest', 'type', 'status', 'date']
                    
                    st.write("**Loan Fields:**")
                    for field in loan_fields:
                        if field in columns:
                            st.write(f"✅ {field}")
                        else:
                            st.write(f"❌ {field}")
                    
                    st.write("**Accounting Fields:**")
                    for field in acct_fields:
                        if field in columns:
                            st.write(f"✅ {field}")
                        else:
                            st.write(f"❌ {field}")
                else:
                    st.error("Could not retrieve columns")
    
    # Custom query testing
    st.header("🔧 Custom Query Testing")
    
    custom_query = st.text_area(
        "Enter a test query:",
        value="SELECT COUNT(*) as row_count FROM efeld_cuj.loan_io.historical_loans",
        height=100
    )
    
    if st.button("▶️ Run Test Query"):
        if custom_query.strip():
            with st.spinner("Running test query..."):
                try:
                    result = validator.db.execute_query(custom_query)
                    if result and result['data'] is not None:
                        st.success("✅ Query executed successfully!")
                        st.write("**Results:**")
                        st.dataframe(result['data'])
                        st.write(f"**Metadata:** {result['metadata']}")
                    else:
                        st.error("❌ Query returned no results")
                except Exception as e:
                    st.error(f"❌ Query failed: {str(e)}")
        else:
            st.warning("Please enter a query to test")


if __name__ == "__main__":
    run_debug_app()
