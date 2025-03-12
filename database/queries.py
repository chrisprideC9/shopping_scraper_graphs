import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Tuple
from supabase import Client
from datetime import datetime, date

logger = logging.getLogger(__name__)

def get_all_clients(supabase: Client) -> List[Dict[str, Any]]:
    """Get all clients from the database."""
    response = supabase.table("clients").select("id, name").order("name").execute()
    return response.data

def get_keywords_by_client(supabase: Client, client_name: str) -> List[Dict[str, Any]]:
    """Get all keywords for a specific client."""
    # First get the client ID
    client_response = supabase.table("clients").select("id").eq("name", client_name).execute()
    
    if not client_response.data:
        return []
    
    client_id = client_response.data[0]["id"]
    
    # Then get all keywords for this client
    response = supabase.table("keywords").select("id, keyword").eq("client_id", client_id).execute()
    return response.data

def get_top_products(
    supabase: Client, 
    client_name: str, 
    max_position: int = 5, 
    limit: int = 10,
    date_range: Optional[Tuple[date, date]] = None
) -> pd.DataFrame:
    """
    Get top products by appearance frequency in specific positions.
    
    Args:
        supabase: Supabase client
        client_name: Name of the client
        max_position: Maximum position to include (e.g., 1 for only first position, 5 for top 5)
        limit: Number of products to return
        date_range: Optional tuple of (start_date, end_date)
        
    Returns:
        DataFrame with product details and count
    """
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
        AND ps.position <= {max_position}
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
    LIMIT {limit}
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

def get_top_filters_by_keyword(
    supabase: Client,
    client_name: str,
    keyword: str,
    date_range: Optional[Tuple[date, date]] = None,
    limit: int = 10
) -> pd.DataFrame:
    """
    Get the most frequently used filters for a specific keyword.
    
    Args:
        supabase: Supabase client
        client_name: Name of the client
        keyword: The keyword to analyze
        date_range: Optional tuple of (start_date, end_date)
        limit: Number of filters to return
        
    Returns:
        DataFrame with filter details and count
    """
    # First get the client ID
    client_response = supabase.table("clients").select("id").eq("name", client_name).execute()
    
    if not client_response.data:
        return pd.DataFrame()
    
    client_id = client_response.data[0]["id"]
    
    # Get the keyword ID
    keyword_response = supabase.table("keywords").select("id").eq("keyword", keyword).eq("client_id", client_id).execute()
    
    if not keyword_response.data:
        return pd.DataFrame()
    
    keyword_id = keyword_response.data[0]["id"]
    
    # Construct the SQL query
    query = f"""
    SELECT 
        ps.filters as filter,
        COUNT(ps.id) as count
    FROM 
        product_scrapes ps
    JOIN 
        scrape_dates sd ON ps.scrape_date_id = sd.id
    WHERE 
        ps.keyword_id = {keyword_id}
        AND ps.filters IS NOT NULL
        AND ps.filters != ''
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
        ps.filters
    ORDER BY 
        count DESC
    LIMIT {limit}
    """
    
    # Execute the query
    try:
        response = supabase.rpc('run_query', {'query_text': query}).execute()
        
        if response.data:
            return pd.DataFrame(response.data)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error running top filters query: {str(e)}")
        return pd.DataFrame()

def get_shipping_returns_for_product(supabase: Client, product_id: int) -> Dict[str, Any]:
    """
    Get shipping and returns information for a specific product.
    
    Note: This is a mockup function since your database schema doesn't have
    shipping and returns information. In a real implementation, you would
    need to modify the database schema to include this data.
    
    Args:
        supabase: Supabase client
        product_id: ID of the product
        
    Returns:
        Dictionary with shipping and returns info
    """
    # This is a mockup function. In a real implementation, you would query the database.
    # For now, we'll return some placeholder data
    try:
        # Get the product details
        product_response = supabase.table("products").select("*").eq("id", product_id).execute()
        
        if not product_response.data:
            return {}
        
        # In a real implementation, you would have shipping and returns fields
        # Since we don't have real data, we'll create some placeholder text
        return {
            'shipping_info': "Free shipping on orders over $25. Standard delivery in 3-5 business days.",
            'returns_info': "Free returns within 30 days of delivery. Must be in original packaging."
        }
    except Exception as e:
        logger.error(f"Error getting shipping and returns data: {str(e)}")
        return {}

def get_merchant_distribution(
    supabase: Client,
    client_name: str,
    date_range: Optional[Tuple[date, date]] = None,
    limit: int = 10
) -> pd.DataFrame:
    """
    Get distribution of products by merchant.
    
    Args:
        supabase: Supabase client
        client_name: Name of the client
        date_range: Optional tuple of (start_date, end_date)
        limit: Number of merchants to return
        
    Returns:
        DataFrame with merchant details and count
    """
    # First get the client ID
    client_response = supabase.table("clients").select("id").eq("name", client_name).execute()
    
    if not client_response.data:
        return pd.DataFrame()
    
    client_id = client_response.data[0]["id"]
    
    # Construct the SQL query
    query = f"""
    SELECT 
        COALESCE(p.merchant, 'Unknown') as merchant,
        COUNT(DISTINCT p.id) as count
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
        AND p.merchant IS NOT NULL
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
        p.merchant
    ORDER BY 
        count DESC
    LIMIT {limit}
    """
    
    # Execute the query
    try:
        response = supabase.rpc('run_query', {'query_text': query}).execute()
        
        if response.data:
            return pd.DataFrame(response.data)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error running merchant distribution query: {str(e)}")
        return pd.DataFrame()

def get_position_trends_over_time(
    supabase: Client,
    client_name: str,
    keywords: List[str],
    date_range: Optional[Tuple[date, date]] = None
) -> pd.DataFrame:
    """
    Get position trends over time for specific keywords.
    
    Args:
        supabase: Supabase client
        client_name: Name of the client
        keywords: List of keywords to include
        date_range: Optional tuple of (start_date, end_date)
        
    Returns:
        DataFrame with date, keyword, and average position
    """
    if not keywords:
        return pd.DataFrame()
    
    # First get the client ID
    client_response = supabase.table("clients").select("id").eq("name", client_name).execute()
    
    if not client_response.data:
        return pd.DataFrame()
    
    client_id = client_response.data[0]["id"]
    
    # Get the keyword IDs
    keyword_ids = []
    for kw in keywords:
        response = supabase.table("keywords").select("id").eq("keyword", kw).eq("client_id", client_id).execute()
        if response.data:
            keyword_ids.append(response.data[0]["id"])
    
    if not keyword_ids:
        return pd.DataFrame()
    
    # Create a comma-separated list of keyword IDs
    keyword_ids_str = ", ".join(str(kid) for kid in keyword_ids)
    
    # Construct the SQL query
    query = f"""
    SELECT 
        sd.scrape_date,
        k.keyword,
        AVG(ps.position) as avg_position
    FROM 
        product_scrapes ps
    JOIN 
        keywords k ON ps.keyword_id = k.id
    JOIN 
        scrape_dates sd ON ps.scrape_date_id = sd.id
    WHERE 
        k.id IN ({keyword_ids_str})
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
        sd.scrape_date, k.keyword
    ORDER BY 
        sd.scrape_date, k.keyword
    """
    
    # Execute the query
    try:
        response = supabase.rpc('run_query', {'query_text': query}).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            # Convert scrape_date to datetime
            df['scrape_date'] = pd.to_datetime(df['scrape_date'])
            return df
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error running position trends query: {str(e)}")
        return pd.DataFrame()