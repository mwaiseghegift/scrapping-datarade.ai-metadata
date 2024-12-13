import requests
from bs4 import BeautifulSoup
import time  # Import time module for adding waits

# Base URL for the search
base_url = "https://datarade.ai/search/products?category%5B%5D=financial-market-data"

# List to hold the metadata for each dataset
data = []

# Add headers to simulate a real browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

# Start with the first page and loop through the pages
page_number = 1



while True:
    # Construct the URL for the current page
    url = f"{base_url}&page={page_number}"
    
    # Send a GET request to fetch the page content with headers
    response = requests.get(url, headers=headers)
    time.sleep(4)  # Wait for 2 seconds before making the next request
    
    # Check if the request was successful (status code 200)
    if response.status_code != 200:
        print(f"Failed to retrieve page {page_number}. Status code: {response.status_code}")
        break
    
    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all product cards
    product_cards = soup.find_all('a', class_='product-card--horizontal')
    
    # If no products are found, retry up to 3 times before exiting the loop (end of pages)
    retries = 3
    while not product_cards and retries > 0:
        print(f"Failed to retrieve page {page_number}. Retrying... ({3 - retries + 1}/3)")
        response = requests.get(url, headers=headers)
        time.sleep(4)  # Wait for 4 seconds before making the next request
        soup = BeautifulSoup(response.content, 'html.parser')
        product_cards = soup.find_all('a', class_='product-card--horizontal')
        retries -= 1
    if not product_cards:
        print(f"No more products found after {3} retries. Total pages: {page_number - 1}")
        break

    # Iterate through each product card and extract relevant information
    for card in product_cards:
        # enter the product page
        product_url = card['href']
        if not product_url.startswith('http'):
            product_url = f"https://datarade.ai{product_url}"
        product_response = requests.get(product_url, headers=headers)
        time.sleep(2)  
        product_soup = BeautifulSoup(product_response.content, 'html.parser')

        title = product_soup.find('h1', class_='product-hero__header-content-title-name').text.strip()
        
        # Find and extract the values
        volume = product_soup.find('div', class_='dataset__fact-name', string=lambda text: text and text.strip() == 'Volume')
        volume = volume.find_next('div', class_='dataset__fact-value').text.strip() if volume else 'N/A'

        data_quality = product_soup.find('div', class_='dataset__fact-name', string=lambda text: text and text.strip() == 'Data Quality')
        data_quality = data_quality.find_next('div', class_='dataset__fact-value').text.strip() if data_quality else 'N/A'

        avail_formats = product_soup.find('div', class_='dataset__fact-name', string=lambda text: text and text.strip() == 'Avail. Formats')
        avail_formats = avail_formats.find_next('div', class_='dataset__fact-value').text.strip() if avail_formats else 'N/A'

        coverage = product_soup.find('div', class_='dataset__fact-name', string=lambda text: text and text.strip() == 'Coverage')
        coverage = coverage.find_next('div', class_='dataset__fact-value').text.strip() if coverage else 'N/A'

        history = product_soup.find('div', class_='dataset__fact-name', string=lambda text: text and text.strip() == 'History')
        history = history.find_next('div', class_='dataset__fact-value').text.strip() if history else 'N/A'
        
        pricing = product_soup.find('div', class_='pricing-plan__quote')
        pricing = pricing.text.strip() if pricing else 'N/A'

        # Append the metadata to the list
        data.append({
            'Title': title,
            'Volume': volume,
            'Data Quality': data_quality,
            'Available Formats': avail_formats,
            'Coverage': coverage,
            'History': history,
            'Pricing': pricing
        })
        print(f"Processed: {title}")

    # Move to the next page
    page_number += 1
    print(f"Page {page_number - 1} done.")

    """
    <div class="dtrd-menu pagination"><a class="icon item disabled"><i class="feather icon-chevron-left"></i>Previous</a> <a class="item active" href="/search/products?category%5B%5D=financial-market-data">1</a> <a class="item" rel="next" href="/search/products?category%5B%5D=financial-market-data&amp;page=2">2</a> <a class="item" href="/search/products?category%5B%5D=financial-market-data&amp;page=3">3</a> <a class="item" href="/search/products?category%5B%5D=financial-market-data&amp;page=4">4</a> <a class="item" href="/search/products?category%5B%5D=financial-market-data&amp;page=5">5</a> <div class="disabled item">…</div> <a class="item" href="/search/products?category%5B%5D=financial-market-data&amp;page=9">9</a> <a class="icon item " rel="next" href="/search/products?category%5B%5D=financial-market-data&amp;page=2">Next<i class="feather icon-chevron-right"></i></a></div>
    """
    # find max page number from pagination
    max_number = soup.find('div', class_='dtrd-menu pagination').find_all('a')[-2].text
    if page_number > int(max_number):
        print(f"End of pages. Total pages: {page_number - 1}")
        break

import pandas as pd
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file (optional)
df.to_csv('datasets_metadata_paginated.csv', index=False)

# Display the DataFrame
print(df)
