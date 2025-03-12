import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import logging
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import Supabase connection
from database.connection import init_connection
from database.queries import (
    get_all_clients,
    get_top_products,
    get_top_filters_by_keyword,
    get_keywords_by_client,
    get_shipping_returns_for_product,
    get_merchant_distribution,
    get_position_trends_over_time
)

def render_header():
    """Render the app header."""
    st.title("🛒 Shopping Data Analytics Dashboard")
    st.write("Visualize shopping scraper data trends and insights.")

def render_client_selection(clients):
    """Render the client selection dropdown."""
    st.sidebar.header("Client Selection")
    selected_client = None
    
    if clients:
        client_names = [client["name"] for client in clients]
        selected_client = st.sidebar.selectbox("Select client:", client_names)
    else:
        st.sidebar.info("No clients found in the database.")
    
    return selected_client

def render_metric_selection():
    """Render the metrics selection sidebar."""
    st.sidebar.header("Metrics")
    metrics = st.sidebar.multiselect(
        "Select metrics to display:",
        [
            "Top Products",
            "Position Trends",
            "Filter Analysis",
            "Shipping & Returns",
            "Merchant Distribution"
        ],
        default=["Top Products", "Position Trends"]
    )
    
    return metrics

def render_date_filter():
    """Render date range filter."""
    st.sidebar.header("Date Range")
    date_range = st.sidebar.date_input(
        "Select date range:",
        value=(pd.Timestamp.now() - pd.Timedelta(days=30), pd.Timestamp.now()),
        max_value=pd.Timestamp.now()
    )
    
    return date_range

def render_top_products_chart(supabase, client_name, date_range, top_n=10):
    """Render chart showing top products by appearance in position 1 and top 5."""
    st.header("Top Products Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"Products in Position #1")
        top_1_products = get_top_products(supabase, client_name, 1, top_n, date_range)
        
        if not top_1_products.empty:
            fig = px.bar(
                top_1_products,
                x='count',
                y='title',
                orientation='h',
                labels={'count': 'Appearances', 'title': 'Product'},
                title=f'Top {top_n} Products in Position #1',
                color='count',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            # Add data table
            with st.expander("View Data"):
                st.dataframe(top_1_products)
        else:
            st.info("No data available for position #1.")
    
    with col2:
        st.subheader(f"Products in Top 5 Positions")
        top_5_products = get_top_products(supabase, client_name, 5, top_n, date_range)
        
        if not top_5_products.empty:
            fig = px.bar(
                top_5_products,
                x='count',
                y='title',
                orientation='h',
                labels={'count': 'Appearances', 'title': 'Product'},
                title=f'Top {top_n} Products in Top 5 Positions',
                color='count',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            # Add data table
            with st.expander("View Data"):
                st.dataframe(top_5_products)
        else:
            st.info("No data available for top 5 positions.")

def render_position_trends(supabase, client_name, date_range):
    """Render chart showing position trends over time."""
    st.header("Position Trends Over Time")
    
    # Get keywords for this client
    keywords = get_keywords_by_client(supabase, client_name)
    
    if not keywords:
        st.info("No keywords found for this client.")
        return
    
    # Let user select keywords to analyze
    selected_keywords = st.multiselect(
        "Select keywords to analyze:",
        [k["keyword"] for k in keywords],
        default=[keywords[0]["keyword"]] if keywords else []
    )
    
    if not selected_keywords:
        st.info("Please select at least one keyword to analyze.")
        return
    
    # Get position data for the selected keywords
    position_data = get_position_trends_over_time(supabase, client_name, selected_keywords, date_range)
    
    if not position_data.empty:
        # Create visualization
        fig = px.line(
            position_data,
            x='scrape_date',
            y='avg_position',
            color='keyword',
            labels={'scrape_date': 'Date', 'avg_position': 'Average Position', 'keyword': 'Keyword'},
            title='Average Position Trends Over Time',
            markers=True
        )
        # Invert y-axis since lower position numbers are better
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)
        
        # Add data table
        with st.expander("View Position Data"):
            st.dataframe(position_data)
    else:
        st.info("No position trend data available for the selected keywords.")

def render_filter_analysis(supabase, client_name, date_range):
    """Render analysis of filters used for keywords."""
    st.header("Filter Analysis")
    
    # Get keywords for this client
    keywords = get_keywords_by_client(supabase, client_name)
    
    if not keywords:
        st.info("No keywords found for this client.")
        return
    
    # Let user select a keyword to analyze
    selected_keyword = st.selectbox(
        "Select keyword to analyze filters:",
        [k["keyword"] for k in keywords]
    )
    
    if not selected_keyword:
        return
    
    # Get filter data
    filter_data = get_top_filters_by_keyword(supabase, client_name, selected_keyword, date_range)
    
    if not filter_data.empty:
        # Create visualization
        fig = px.pie(
            filter_data,
            values='count',
            names='filter',
            title=f'Most Common Filters for "{selected_keyword}"',
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Add data table
        with st.expander("View Filter Data"):
            st.dataframe(filter_data)
    else:
        st.info(f"No filter data available for the keyword '{selected_keyword}'.")

def render_shipping_returns(supabase, client_name, date_range):
    """Render shipping and returns info for top products."""
    st.header("Shipping & Returns Analysis")
    
    # Get top products for this client
    top_products = get_top_products(supabase, client_name, 5, 5, date_range)
    
    if top_products.empty:
        st.info("No top products found for this client.")
        return
    
    # Let user select a product to analyze
    product_titles = top_products['title'].tolist()
    product_ids = top_products['product_id'].tolist()
    
    selected_index = st.selectbox(
        "Select product to analyze shipping & returns:",
        range(len(product_titles)),
        format_func=lambda i: product_titles[i]
    )
    
    if selected_index is None:
        return
    
    selected_product_id = product_ids[selected_index]
    selected_product_title = product_titles[selected_index]
    
    # Get shipping and returns data
    shipping_returns_data = get_shipping_returns_for_product(supabase, selected_product_id)
    
    if shipping_returns_data:
        st.subheader(f"Shipping & Returns for: {selected_product_title}")
        
        # Display shipping info
        st.write("### Shipping Information")
        if shipping_returns_data.get('shipping_info'):
            st.info(shipping_returns_data['shipping_info'])
        else:
            st.info("No shipping information available.")
        
        # Display returns info
        st.write("### Returns Policy")
        if shipping_returns_data.get('returns_info'):
            st.info(shipping_returns_data['returns_info'])
        else:
            st.info("No returns information available.")
    else:
        st.info(f"No shipping and returns data available for '{selected_product_title}'.")

def render_merchant_distribution(supabase, client_name, date_range):
    """Render merchant distribution for top products."""
    st.header("Merchant Distribution")
    
    # Get merchant distribution data
    merchant_data = get_merchant_distribution(supabase, client_name, date_range)
    
    if not merchant_data.empty:
        # Create visualization
        fig = px.bar(
            merchant_data,
            x='merchant',
            y='count',
            labels={'merchant': 'Merchant', 'count': 'Product Count'},
            title='Product Distribution by Merchant',
            color='count',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Add data table
        with st.expander("View Merchant Data"):
            st.dataframe(merchant_data)
    else:
        st.info("No merchant distribution data available.")

def main():
    """Main application entry point."""
    st.set_page_config(
        page_title="Shopping Data Analytics",
        page_icon="📊",
        layout="wide"
    )
    
    # Render header
    render_header()
    
    # Initialize Supabase connection
    try:
        supabase = init_connection()
        
        # Success message but don't show to keep interface clean
        # st.success("Connected to Supabase database!")
    except Exception as e:
        st.error(f"Failed to connect to Supabase: {e}")
        st.stop()
    
    # Get all clients
    try:
        clients = get_all_clients(supabase)
    except Exception as e:
        st.error(f"Failed to retrieve clients: {e}")
        st.stop()
    
    # Handle client selection
    selected_client = render_client_selection(clients)
    
    if not selected_client:
        st.info("Please select a client to analyze data.")
        st.stop()
    
    # Set up the filters
    date_range = render_date_filter()
    selected_metrics = render_metric_selection()
    
    # Main content area
    try:
        if "Top Products" in selected_metrics:
            render_top_products_chart(supabase, selected_client, date_range)
        
        if "Position Trends" in selected_metrics:
            render_position_trends(supabase, selected_client, date_range)
        
        if "Filter Analysis" in selected_metrics:
            render_filter_analysis(supabase, selected_client, date_range)
        
        if "Shipping & Returns" in selected_metrics:
            render_shipping_returns(supabase, selected_client, date_range)
        
        if "Merchant Distribution" in selected_metrics:
            render_merchant_distribution(supabase, selected_client, date_range)
        
    except Exception as e:
        st.error(f"An error occurred while generating visualizations: {e}")
        logger.error(f"Visualization error: {e}", exc_info=True)
    
    # Footer
    st.markdown("---")
    st.markdown("© 2025 Calibre Nine | [GitHub Repository](https://github.com/chrisprideC9)")

if __name__ == "__main__":
    main()