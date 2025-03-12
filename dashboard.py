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
    get_shipping_returns_by_merchant,
    get_merchant_distribution,
    get_position_trends_over_time
)

def render_header():
    """Render the app header."""
    st.markdown(
        """
        <h1 style='display: flex; align-items: center; gap: 10px;'>
            <img src='https://calibrenine.com.au/wp-content/uploads/2022/02/Calibre9-Favicon.png' width='30'>
            Shopping Data Analytics Dashboard
        </h1>
        """,
        unsafe_allow_html=True
    )
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
            "Shipping & Returns by Merchant",
            "Merchant Distribution"
        ],
        default=["Top Products", "Filter Analysis"]
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
    """Render chart showing top products by appearance in position 1 and top 5 with keyword filtering."""
    st.header("Top Products Analysis")
    
    # Get keywords for this client
    keywords = get_keywords_by_client(supabase, client_name)
    keyword_options = ["All Keywords"] + [k["keyword"] for k in keywords]
    
    # Keyword filter
    selected_keyword = st.selectbox(
        "Filter by keyword:",
        keyword_options
    )
    
    # Modify the get_top_products function to include keyword filtering
    def get_filtered_top_products(position, keyword=None):
        # First get the client ID
        client_response = supabase.table("clients").select("id").eq("name", client_name).execute()
        
        if not client_response.data:
            return pd.DataFrame()
        
        client_id = client_response.data[0]["id"]
        
        # Construct the SQL query
        query = f"""
        SELECT 
            p.id as product_id,
            p.product_id as original_product_id,
            p.title,
            p.link,
            p.merchant,
            COUNT(ps.id) as count
        FROM 
            product_scrapes ps
        JOIN 
            products p ON ps.product_id = p.id
        JOIN 
            keywords k ON ps.keyword_id = k.id
        JOIN 
            scrape_dates sd ON ps.scrape_date_id = sd.id
        WHERE 
            k.client_id = {client_id}
            AND ps.position <= {position}
        """
        
        # Add keyword filter if specified
        if keyword and keyword != "All Keywords":
            query += f"""
            AND k.keyword = '{keyword}'
            """
        
        # Add date range filter if provided
        if date_range and len(date_range) == 2:
            start_date, end_date = date_range
            query += f"""
            AND sd.scrape_date >= '{start_date}'
            AND sd.scrape_date <= '{end_date}'
            """
        
        # Complete the query
        query += f"""
        GROUP BY 
            p.id, p.product_id, p.title, p.link, p.merchant
        ORDER BY 
            count DESC
        LIMIT {top_n}
        """
        
        # Execute the query
        try:
            response = supabase.rpc('run_query', {'query_text': query}).execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error running top products query: {str(e)}")
            return pd.DataFrame()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"Products in Position #1")
        
        # Get top products filtered by keyword if selected
        keyword_filter = None if selected_keyword == "All Keywords" else selected_keyword
        top_1_products = get_filtered_top_products(1, keyword_filter)
        
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
        
        # Get top 5 products filtered by keyword if selected
        top_5_products = get_filtered_top_products(5, keyword_filter)
        
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
    
    # Get filter data - now split by commas
    filter_data = get_top_filters_by_keyword(supabase, client_name, selected_keyword, date_range)
    
    if not filter_data.empty:
        st.subheader(f"Top 10 Most Common Individual Filters for \"{selected_keyword}\"")
        
        # Create bar chart visualization - better for comparing individual filters
        fig = px.bar(
            filter_data,
            x='count',
            y='filter',
            orientation='h',
            labels={'count': 'Appearances', 'filter': 'Filter Type'},
            title=f'Top 10 Individual Filters for "{selected_keyword}"',
            color='count',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Add data table
        with st.expander("View Filter Data"):
            st.dataframe(filter_data)
    else:
        st.info(f"No filter data available for the keyword '{selected_keyword}'.")

def render_shipping_returns(supabase, client_name, date_range):
    """Render shipping and returns info by merchant."""
    st.header("Shipping & Returns Analysis by Merchant")
    
    # Get merchant data for this client
    merchant_data = get_merchant_distribution(supabase, client_name, date_range)
    
    if merchant_data.empty:
        st.info("No merchant data found for this client.")
        return
    
    # Let user select a merchant to analyze
    merchant_names = merchant_data['merchant'].tolist()
    
    selected_merchant = st.selectbox(
        "Select merchant to view shipping & returns policies:",
        merchant_names
    )
    
    if not selected_merchant:
        return
    
    # Get shipping and returns data by merchant
    shipping_returns_data = get_shipping_returns_by_merchant(supabase, selected_merchant)
    
    if shipping_returns_data:
        st.subheader(f"Shipping & Returns Policies for: {selected_merchant}")
        
        col1, col2 = st.columns(2)
        
        # Display shipping info
        with col1:
            st.write("### Shipping Information")
            if shipping_returns_data.get('shipping_info'):
                st.info(shipping_returns_data['shipping_info'])
            else:
                st.info("No shipping information available.")
        
        # Display returns info
        with col2:
            st.write("### Returns Policy")
            if shipping_returns_data.get('returns_info'):
                st.info(shipping_returns_data['returns_info'])
            else:
                st.info("No returns information available.")
        
        # Add products from this merchant
        st.write("### Products from this Merchant")
        
        # Query to get products from this merchant
        query = f"""
        SELECT 
            p.title, 
            p.rating, 
            p.price,
            COUNT(ps.id) as appearance_count
        FROM 
            products p
        JOIN 
            product_scrapes ps ON p.id = ps.product_id
        JOIN 
            keywords k ON ps.keyword_id = k.id
        JOIN 
            clients c ON k.client_id = c.id
        WHERE 
            c.name = '{client_name}'
            AND p.merchant = '{selected_merchant}'
        GROUP BY 
            p.title, p.rating, p.price
        ORDER BY 
            appearance_count DESC
        LIMIT 10
        """
        
        try:
            response = supabase.rpc('run_query', {'query_text': query}).execute()
            if response.data:
                products_df = pd.DataFrame(response.data)
                st.dataframe(products_df)
            else:
                st.info(f"No products found for {selected_merchant}")
        except Exception as e:
            st.error(f"Error fetching products: {e}")
    else:
        st.info(f"No shipping and returns data available for '{selected_merchant}'.")

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
        
        if "Shipping & Returns by Merchant" in selected_metrics:
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