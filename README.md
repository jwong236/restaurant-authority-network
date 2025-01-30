# TravelQuest ETL

## Overview

The `travelquest-etl` repository is responsible for **scraping, transforming, and storing** travel and restaurant-related data for the TravelQuest application. It implements an **ETL (Extract, Transform, Load) pipeline** that collects data from various sources, processes it, and loads it into a structured database for efficient retrieval by the TravelQuest search engine.

## Features

- **Web Crawling & Scraping**: Extracts restaurant and travel recommendations from popular sources.
- **Multi-Source Data Collection**:
  - Common blogs and recommendation sites (e.g., Michelin Guide, food blogs).
  - Video content from platforms like **YouTube** and **TikTok**, where restaurants and travel spots are frequently recommended.
- **Data Transformation**:
  - Extracts relevant metadata from blogs and videos (e.g., restaurant name, location, rating, review summary, influencer mentions).
  - Processes **YouTube and TikTok videos** by analyzing titles, descriptions, and comments to extract recommendations.
- **Data Storage**:
  - Loads structured data into a **database** for later retrieval by the TravelQuest search engine.

## Tech Stack

- **Python** (main scraping and processing logic)

## Architecture

1. **Extract**

   - Crawl food blogs and travel guides for curated recommendations.
   - Scrape YouTube/TikTok for videos recommending restaurants and places of interest.
   - Use APIs where available to gather structured data.

2. **Transform**

   - Clean and normalize text data (e.g., restaurant names, location data, cuisine type).
   - Extract key information from video descriptions and comments.
   - Assign credibility scores based on source popularity (e.g., Michelin Guide > food blog > social media mention).

3. **Load**
   - Store processed data in a **PostgreSQL** or **MongoDB** database.
   - Ensure efficient indexing for search engine queries.

This repository is part of the **TravelQuest** ecosystem and serves as the **data collection and ingestion pipeline** for the search engine.
