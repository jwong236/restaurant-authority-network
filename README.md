# TravelQuest ETL

## Overview

The `travelquest-etl` repository is responsible for **scraping, transforming, and storing** travel and restaurant-related data for the TravelQuest application. It implements an **ETL (Extract, Transform, Load) pipeline** that collects data from various sources, processes it, and loads it into a structured database for efficient retrieval by the TravelQuest search engine.

## Features

- **Web Crawling & Scraping**: Extracts restaurant and travel recommendations from popular sources.
- **Multi-Source Data Collection**:
  - Common blogs and recommendation sites (e.g., Michelin Guide, food blogs).
- **Data Transformation**:
  - Extracts relevant metadata from blogs and videos (e.g., restaurant name, location, rating, review summary, influencer mentions).
  - Processes **YouTube and TikTok videos** by analyzing titles, descriptions, and comments to extract recommendations.
- **Data Storage**:
  - Loads structured data into a **database** for later retrieval by the TravelQuest search engine.
- **Parallelism**:
  - Multithreaded architecture with fair scheduling techniques ensures efficient CPU usage per unit of time

## Tech Stack

- **Python** (Main scraping and processing logic)
- **Postgres** (Data storage and database operations)

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
   - Store processed data in a **PostgreSQL** database.
   - Ensure efficient indexing for search engine queries.

## Why I am not using an ORM

1. **Orm Hate by Martin Fowler**

   - https://martinfowler.com/bliki/OrmHate.html

2. **Golang, Orms, and why I am still not using one by Eric Urban**

   - https://www.hydrogen18.com/blog/golang-orms-and-why-im-still-not-using-one.html

3. **The Vietnam of Computer Science by Ted Neward**

   - https://www.odbms.org/wp-content/uploads/2013/11/031.01-Neward-The-Vietnam-of-Computer-Science-June-2006.pdf

##

This repository is part of the **TravelQuest** ecosystem and serves as the **data collection and ingestion pipeline** for the search engine.
