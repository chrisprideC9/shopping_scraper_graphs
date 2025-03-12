# Shopping Data Analytics Dashboard

A Streamlit-based dashboard for visualizing shopping scraper data trends and insights from Supabase.

## Features

- **Top Products Analysis**: Visualize the most frequent products in position #1 and top 5 positions
- **Position Trends**: Track how product positions change over time for specific keywords
- **Filter Analysis**: Identify the most common filters used for specific keywords
- **Shipping & Returns**: View shipping and returns information for top products
- **Merchant Distribution**: Analyze the distribution of products by merchant

## Installation

1. Clone the repository
   ```bash
   git clone https://github.com/yourusername/shopping-analytics-dashboard.git
   cd shopping-analytics-dashboard
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables
   Create a `.env` file in the project root with the following variables:
   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_API_KEY=your_supabase_api_key
   ```

   Alternatively, you can set up these variables in Streamlit secrets:
   ```
   # .streamlit/secrets.toml
   SUPABASE_URL = "your_supabase_url"
   SUPABASE_API_KEY = "your_supabase_api_key"
   ```

## Usage

1. Run the Streamlit app
   ```bash
   streamlit run dashboard.py
   ```

## Database Setup

This dashboard connects to a Supabase database with the following schema:

- `clients`: Client information
- `keywords`: Keywords associated with clients
- `scrape_dates`: Dates when data was scraped
- `products`: Product information
- `product_scrapes`: Position data for products

The dashboard assumes this schema is already set up and populated with data from the shopping scraper import tool.

## Adding Custom Metrics

To add new metrics or visualizations:

1. Create a new query function in `database/queries.py`
2. Add a new render function in `dashboard.py`
3. Update the metrics selection in the sidebar

## Development

### Project Structure

```
.
├── .env                      # Environment variables
├── .gitignore                # Git ignore file
├── README.md                 # Project documentation
├── dashboard.py              # Main application file
├── requirements.txt          # Project dependencies
└── database/                 # Database related code
    ├── __init__.py           # Package initialization
    ├── connection.py         # Database connection functions
    └── queries.py            # Database query functions
```

### Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some feature'`)
5. Push to the branch (`git push origin feature/your-feature`)
6. Open a Pull Request

## License

MIT