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
    Split the filters by comma and count each individual filter.
    
    Args:
        supabase: Supabase client
        client_name: Name of the client
        keyword: The keyword to analyze
        date_range: Optional tuple of (start_date, end_date)
        limit: Number of filters to return
        
    Returns:
        DataFrame with individual filter details and count
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
    
    # Construct the SQL query to get all filters
    query = f"""
    SELECT 
        ps.filters as filter_string
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
    
    # Execute the query
    try:
        response = supabase.rpc('run_query', {'query_text': query}).execute()
        
        if not response.data:
            return pd.DataFrame()
            
        # Process the data to split filters
        all_filters = []
        for row in response.data:
            filter_string = row.get('filter_string', '')
            if filter_string:
                # Split by comma and strip whitespace
                individual_filters = [f.strip() for f in filter_string.split(',')]
                all_filters.extend(individual_filters)
        
        # Count frequencies
        from collections import Counter
        filter_counts = Counter(all_filters)
        
        # Convert to DataFrame and sort
        df = pd.DataFrame([
            {'filter': filter_name, 'count': count}
            for filter_name, count in filter_counts.items()
        ])
        
        if df.empty:
            return pd.DataFrame()
            
        df = df.sort_values('count', ascending=False).head(limit).reset_index(drop=True)
        return df
        
    except Exception as e:
        logger.error(f"Error processing filters: {str(e)}")
        return pd.DataFrame()

def get_shipping_returns_by_merchant(supabase: Client, merchant_name: str) -> Dict[str, Any]:
    """
    Get shipping and returns information for a specific merchant.
    
    Note: This is a mockup function that returns information based on the merchant name
    rather than a specific product. In a real implementation, you would query the database
    for actual shipping and returns policies by merchant.
    
    Args:
        supabase: Supabase client
        merchant_name: Name of the merchant
        
    Returns:
        Dictionary with shipping and returns info
    """
    # Default shipping and returns info
    default_info = {
        'shipping_info': "Standard shipping policies apply. Check merchant website for details.",
        'returns_info': "Standard return policies apply. Check merchant website for details."
    }
    
    # Mapping of merchants to their shipping and returns policies
    merchant_policies = {
        'Walmart': {
            'shipping_info': "Free shipping on orders over $35. Standard delivery in 2-5 business days. NextDay delivery available in select areas.",
            'returns_info': "Free returns within 90 days of purchase. Some items have a 30-day return window. Some items cannot be returned (check listing)."
        },
        'Target': {
            'shipping_info': "Free shipping on orders over $35 or with RedCard. Standard delivery in 2-5 business days. Same-day delivery through Shipt.",
            'returns_info': "Most unopened items can be returned within 90 days. RedCard holders get 120 days. Some items have modified return policies."
        },
        'Amazon': {
            'shipping_info': "Free standard shipping for Prime members. Non-Prime: Free shipping on orders over $25. Standard delivery in 4-5 business days.",
            'returns_info': "Most items can be returned within 30 days of receipt. Some items have different policies. Amazon Prime members get free returns."
        },
        'Best Buy': {
            'shipping_info': "Free standard shipping on orders over $35. Standard delivery in 1-3 business days. Store pickup available.",
            'returns_info': "Returns accepted within 15 days for most products. Elite and Elite Plus members get extended return periods."
        },
        'CVS Pharmacy': {
            'shipping_info': "Free shipping on orders over $35. Standard delivery in 1-4 business days.",
            'returns_info': "Returns accepted within 60 days of purchase with receipt. Some items cannot be returned for health reasons."
        },
        'Walgreens.com': {
            'shipping_info': "Free shipping on orders over $35. Standard delivery in 1-3 business days.",
            'returns_info': "Most items can be returned within 30 days. Health and personal care items may have restrictions."
        }
    }
    
    # Look up the merchant in our mapping, return default if not found
    if merchant_name in merchant_policies:
        return merchant_policies[merchant_name]
    else:
        return default_info

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