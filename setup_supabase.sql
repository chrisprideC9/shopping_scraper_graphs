-- Add this SQL function to your Supabase instance to enable running arbitrary queries
-- NOTE: This is a convenience function that allows running SQL directly.
-- Make sure you have proper security rules in place to restrict access.

-- Function to run a query as text and return the results
CREATE OR REPLACE FUNCTION run_query(query_text TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    results JSONB;
BEGIN
    EXECUTE 'SELECT jsonb_agg(t) FROM (' || query_text || ') t' INTO results;
    RETURN COALESCE(results, '[]'::JSONB);
END;
$$;

-- Additional views and functions you might want to add:

-- View for top products
CREATE OR REPLACE VIEW top_products_view AS
SELECT 
    p.id as product_id,
    p.product_id as original_product_id,
    p.title,
    p.link,
    p.merchant,
    k.keyword,
    k.client_id,
    c.name as client_name,
    ps.position,
    sd.scrape_date
FROM 
    product_scrapes ps
JOIN 
    products p ON ps.product_id = p.id
JOIN 
    keywords k ON ps.keyword_id = k.id
JOIN 
    scrape_dates sd ON ps.scrape_date_id = sd.id
JOIN
    clients c ON k.client_id = c.id
ORDER BY
    c.name, k.keyword, sd.scrape_date DESC, ps.position;

-- View for filter analysis
CREATE OR REPLACE VIEW filter_analysis_view AS
SELECT 
    ps.filters,
    k.keyword,
    k.client_id,
    c.name as client_name,
    COUNT(ps.id) as count
FROM 
    product_scrapes ps
JOIN 
    keywords k ON ps.keyword_id = k.id
JOIN
    clients c ON k.client_id = c.id
WHERE
    ps.filters IS NOT NULL
    AND ps.filters != ''
GROUP BY
    ps.filters, k.keyword, k.client_id, c.name
ORDER BY
    c.name, k.keyword, count DESC;

-- Function to get position trends for a specific client and keyword
CREATE OR REPLACE FUNCTION get_position_trends(
    client_name TEXT,
    keyword_name TEXT,
    start_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL
)
RETURNS TABLE (
    scrape_date TIMESTAMP WITH TIME ZONE,
    avg_position FLOAT
)
LANGUAGE SQL
AS $$
    SELECT 
        sd.scrape_date,
        AVG(ps.position) as avg_position
    FROM 
        product_scrapes ps
    JOIN 
        keywords k ON ps.keyword_id = k.id
    JOIN 
        scrape_dates sd ON ps.scrape_date_id = sd.id
    JOIN
        clients c ON k.client_id = c.id
    WHERE 
        c.name = client_name
        AND k.keyword = keyword_name
        AND (start_date IS NULL OR sd.scrape_date >= start_date)
        AND (end_date IS NULL OR sd.scrape_date <= end_date)
    GROUP BY 
        sd.scrape_date
    ORDER BY 
        sd.scrape_date;
$$;

-- Add a shipping_returns table (this would need actual data)
-- Uncomment and execute this to add shipping and returns functionality
/*
CREATE TABLE shipping_returns (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    shipping_info TEXT,
    returns_info TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for better performance
CREATE INDEX idx_shipping_returns_product ON shipping_returns(product_id);
*/