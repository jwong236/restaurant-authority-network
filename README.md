# TravelQuest ETL

## Overview

`travelquest-etl` is an autonomous crawling, extraction, and storage pipeline designed for the TravelQuest search engine. It continuously explores and expands its dataset, using machine learning models to estimate the relevance and priority of discovered resources. By analyzing backlinks and citations, the system calculates domain authority scores for restaurants and blogs, similar to Google's PageRank algorithm.

## **Features**

### **🔍 Intelligent Web Crawling & Source Discovery**

- Identifies and prioritizes **high-quality restaurant sources** (e.g., Michelin Guide, reputable food blogs).
- Extracts **backlinks and citations** to determine the credibility of restaurant mentions.
- Dynamically **adjusts crawling strategy** based on discovered domains and relevance scores.

### **🛠 Multi-Source Data Extraction & Processing**

- Scrapes **food blogs, review sites, and curated lists** for restaurant recommendations.
- Analyzes **structured and unstructured data**, including raw text from blogs, tables, and metadata.
- **Extracts restaurant attributes** such as name, location, cuisine type, and influencer mentions.

### **⚡ Priority-Based Processing & ML-Driven Ranking**

- Implements **intelligent task queueing** to prioritize **authoritative sources first**.
- Uses **machine learning models** to assess **content relevance and domain credibility**.
- Dynamically **adjusts priority scores** based on restaurant mentions, backlinks, and trustworthiness.

### **🔎 Data Transformation & Entity Recognition**

- Detects and verifies **restaurant names, locations, and metadata** from diverse sources.
- Uses **fuzzy matching** to associate restaurant mentions across different websites.
- Filters out irrelevant or low-confidence matches to **improve data accuracy**.

### **📦 Efficient Data Storage & Retrieval**

- Stores extracted and processed data into a **PostgreSQL** database.
- Implements **indexing and caching strategies** for faster search queries.
- Ensures **structured data integrity** for seamless integration with TravelQuest’s search engine.

### **⚙️ Scalable & Modular Architecture**

- Uses **linked-list based ETL phase control** for modular and maintainable execution.
- Supports **parallelized task execution** for large-scale crawling and processing.
- Implements **dynamic lazy loading** to efficiently fetch and extract content as needed.

## Tech Stack

- **Python** → Web crawling, data extraction, and ETL processing.
- **PostgreSQL** → Data storage, indexing, and retrieval.
- **BeautifulSoup & Requests** → HTML parsing and web requests.
- **Multithreading & Priority Queues** → Efficiently handling large-scale data extraction.

---

## Why I Am Not Using an ORM

- [**ORM Hate by Martin Fowler**](https://martinfowler.com/bliki/OrmHate.html)
- [**Golang, ORMs, and Why I’m Still Not Using One**](https://www.hydrogen18.com/blog/golang-orms-and-why-im-still-not-using-one.html)
- [**The Vietnam of Computer Science**](https://www.odbms.org/wp-content/uploads/2013/11/031.01-Neward-The-Vietnam-of-Computer-Science-June-2006.pdf)

---

## About This Repository

This repository is part of the **TravelQuest** ecosystem and serves as the **data ingestion pipeline** for the search engine, ensuring that relevant restaurant data is efficiently collected, structured, and stored.
