"""
Streamlit Dashboard for Loan Analytics
Connects to Databricks to analyze historical_loans, raw_transactions, and ref_accounting tables
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from src import execute_predefined_query, execute_custom_query, initialize_connection


def configure_page():
    """Configure Streamlit page settings"""
    st.set_page_config(
        page_title="Loan Analytics Dashboard",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("💰 Loan Analytics Dashboard")
    st.markdown("---")
    
    # Test database connection
    if 'connection_tested' not in st.session_state:
        with st.spinner("Testing database connection..."):
            if initialize_connection():
                st.success("✅ Database connection established successfully")
                st.session_state.connection_tested = True
            else:
                st.error("❌ Database connection failed")
                st.session_state.connection_tested = False


def sidebar_filters():
    """Create sidebar filters and controls"""
    st.sidebar.header("📊 Dashboard Controls")
    
    # Authentication status
    st.sidebar.subheader("🔐 Authentication Status")
    st.sidebar.info("🔧 Service Principal")
    st.sidebar.caption("Secure app-based authentication")
    
    st.sidebar.markdown("---")
    
    # Analysis type selector
    analysis_type = st.sidebar.selectbox(
        "Select Analysis Type",
        ["Overview", "Loan Analysis", "Transaction Analysis", "Custom Query", "🔍 Debug Mode"],
        index=0
    )
    
    # Refresh data button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
        
    # Force cache clear on first load if needed
    if st.sidebar.button("🧹 Clear All Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Cache cleared!")
        st.rerun()
    
    return analysis_type


def display_overview():
    """Display overview dashboard with key metrics"""
    st.header("📈 Loan Portfolio Overview")
    
    # Get summary data
    loan_summary = execute_predefined_query("loan_summary")
    transaction_summary = execute_predefined_query("transaction_summary")
    
    if loan_summary is not None and not loan_summary.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Loans",
                f"{loan_summary['total_loans'].iloc[0]:,}",
                delta=None
            )
        
        with col2:
            avg_loan = loan_summary['avg_loan_amount'].iloc[0]
            st.metric(
                "Avg Loan Amount",
                f"${avg_loan:,.2f}" if pd.notna(avg_loan) else "N/A",
                delta=None
            )
        
        with col3:
            total_loan = loan_summary['total_loan_amount'].iloc[0]
            st.metric(
                "Total Loan Amount",
                f"${total_loan/1e6:.1f}M" if pd.notna(total_loan) else "N/A",
                delta=None
            )
        
        with col4:
            st.metric(
                "Unique Grades",
                f"{loan_summary['unique_grades'].iloc[0]}",
                delta=None
            )
    
    # Transaction summary
    if transaction_summary is not None and not transaction_summary.empty:
        st.subheader("💳 Transaction Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Transactions",
                f"{transaction_summary['total_transactions'].iloc[0]:,}",
                delta=None
            )
        
        with col2:
            st.metric(
                "Unique Treatments",
                f"{transaction_summary['unique_treatments'].iloc[0]:,}",
                delta=None
            )
        
        with col3:
            avg_balance = transaction_summary['avg_balance'].iloc[0]
            st.metric(
                "Avg Balance",
                f"${avg_balance:,.2f}" if pd.notna(avg_balance) else "N/A",
                delta=None
            )
        
        with col4:
            total_balance = transaction_summary['total_balance'].iloc[0]
            st.metric(
                "Total Balance",
                f"${total_balance/1e6:.1f}M" if pd.notna(total_balance) else "N/A",
                delta=None
            )


def display_loan_analysis():
    """Display detailed loan analysis"""
    st.header("🏦 Detailed Loan Analysis")
    
    # Loan Status Distribution
    st.subheader("📊 Loan Status Distribution")
    status_data = execute_predefined_query("loan_status_distribution")
    
    if status_data is not None and not status_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pie = px.pie(
                status_data, 
                values='count', 
                names='loan_status',
                title="Loans by Status"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            fig_bar = px.bar(
                status_data, 
                x='loan_status', 
                y='avg_amount',
                title="Average Loan Amount by Status"
            )
            fig_bar.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_bar, use_container_width=True)
    
    # Grade Analysis
    st.subheader("🎯 Grade Analysis")
    grade_data = execute_predefined_query("grade_analysis")
    
    if grade_data is not None and not grade_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_grade = px.bar(
                grade_data.groupby('grade').agg({
                    'loan_count': 'sum',
                    'avg_loan_amount': 'mean'
                }).reset_index(),
                x='grade',
                y='loan_count',
                title="Loan Count by Grade"
            )
            st.plotly_chart(fig_grade, use_container_width=True)
        
        with col2:
            fig_rate = px.scatter(
                grade_data,
                x='avg_loan_amount',
                y='avg_interest_rate',
                size='loan_count',
                color='grade',
                title="Loan Amount vs Interest Rate by Grade"
            )
            st.plotly_chart(fig_rate, use_container_width=True)
    
    # Purpose Analysis
    st.subheader("🎯 Loan Purpose Analysis")
    purpose_data = execute_predefined_query("purpose_analysis")
    
    if purpose_data is not None and not purpose_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_purpose = px.bar(
                purpose_data.head(10),
                x='loan_count',
                y='purpose',
                orientation='h',
                title="Top 10 Loan Purposes"
            )
            st.plotly_chart(fig_purpose, use_container_width=True)
        
        with col2:
            fig_amount = px.bar(
                purpose_data.head(10),
                x='avg_loan_amount',
                y='purpose',
                orientation='h',
                title="Average Loan Amount by Purpose"
            )
            st.plotly_chart(fig_amount, use_container_width=True)
    
    # State Distribution
    st.subheader("🗺️ Geographic Distribution")
    state_data = execute_predefined_query("state_distribution")
    
    if state_data is not None and not state_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_state = px.bar(
                state_data.head(15),
                x='addr_state',
                y='loan_count',
                title="Top 15 States by Loan Count"
            )
            st.plotly_chart(fig_state, use_container_width=True)
        
        with col2:
            fig_avg_state = px.bar(
                state_data.head(15),
                x='addr_state',
                y='avg_loan_amount',
                title="Average Loan Amount by State"
            )
            st.plotly_chart(fig_avg_state, use_container_width=True)


def display_transaction_analysis():
    """Display transaction analysis"""
    st.header("💳 Transaction Analysis")
    
    # Accounting Treatment Analysis
    st.subheader("📋 Accounting Treatment Analysis")
    accounting_data = execute_predefined_query("accounting_treatment_analysis")
    
    if accounting_data is not None and not accounting_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_treatment = px.bar(
                accounting_data.head(10),
                x='treatment_type',
                y='transaction_count',
                title="Transaction Count by Accounting Treatment"
            )
            st.plotly_chart(fig_treatment, use_container_width=True)
        
        with col2:
            fig_balance = px.bar(
                accounting_data.head(10),
                x='treatment_type',
                y='avg_balance',
                title="Average Balance by Accounting Treatment"
            )
            st.plotly_chart(fig_balance, use_container_width=True)
        
        # Data table
        st.subheader("📊 Detailed Accounting Treatment Data")
        st.dataframe(accounting_data.head(20), use_container_width=True)
    
    # Monthly Trend Analysis
    st.subheader("📈 Monthly Loan Trends")
    trend_data = execute_predefined_query("monthly_loan_trend")
    
    if trend_data is not None and not trend_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_trend = px.line(
                trend_data,
                x='month',
                y='loan_count',
                title="Monthly Loan Count Trend"
            )
            fig_trend.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_trend, use_container_width=True)
        
        with col2:
            fig_amount_trend = px.line(
                trend_data,
                x='month',
                y='total_amount',
                title="Monthly Total Loan Amount Trend"
            )
            fig_amount_trend.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_amount_trend, use_container_width=True)


def display_custom_query():
    """Display custom query interface"""
    st.header("🔍 Custom SQL Query")
    
    st.markdown("""
    **Available Tables:**
    - `efeld_cuj.loan_io.historical_loans`
    - `efeld_cuj.loan_io.raw_transactions`  
    - `efeld_cuj.loan_io.ref_accounting`
    """)
    
    # Query input
    query = st.text_area(
        "Enter your SQL query:",
        height=200,
        placeholder="SELECT * FROM efeld_cuj.loan_io.historical_loans LIMIT 10"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("🚀 Execute Query")
    
    if execute_button and query.strip():
        with st.spinner("Executing query..."):
            result = execute_custom_query(query)
            
            if result is not None and not result.empty:
                st.success(f"Query executed successfully! Retrieved {len(result)} rows.")
                
                # Display results
                st.subheader("📊 Query Results")
                st.dataframe(result, use_container_width=True)
                
                # Download option
                csv = result.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv"
                )
            else:
                st.warning("Query returned no results or failed to execute.")


def main():
    """Main application function"""
    configure_page()
    
    # Sidebar controls
    analysis_type = sidebar_filters()
    
    # Debug mode
    if analysis_type == "🔍 Debug Mode":
        from src.utils.debug_app import run_debug_app
        run_debug_app()
        return
    
    # Display appropriate analysis based on selection
    if analysis_type == "Overview":
        display_overview()
    elif analysis_type == "Loan Analysis":
        display_loan_analysis()
    elif analysis_type == "Transaction Analysis":
        display_transaction_analysis()
    elif analysis_type == "Custom Query":
        display_custom_query()
    
    # Footer
    st.markdown("---")
    st.markdown("💰 **Loan Analytics Dashboard** - Built with Streamlit and Databricks")


if __name__ == "__main__":
    main()
