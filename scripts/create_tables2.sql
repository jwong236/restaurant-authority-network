-- Drop tables in order to handle dependencies
DROP TABLE IF EXISTS restaurant_references;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS source_types;
DROP TABLE IF EXISTS restaurants;
DROP TABLE IF EXISTS priority_queue;

-- Create restaurants table with geolocation
CREATE TABLE restaurants (
    restaurant_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    backlink_count INTEGER NOT NULL DEFAULT 0,
    is_michelin BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (name, address)
);

-- Source types lookup table (now required)
CREATE TABLE source_types (
    type_name VARCHAR(10) PRIMARY KEY CHECK (type_name IN ('blog', 'video', 'social'))
);

-- Enhanced sources table with credibility tracking
CREATE TABLE sources (
    source_id SERIAL PRIMARY KEY,
    source_type VARCHAR(10) NOT NULL REFERENCES source_types(type_name),
    domain VARCHAR(255) NOT NULL,
    credibility_score FLOAT DEFAULT 0.5 
        CHECK (credibility_score BETWEEN 0 AND 1),
    last_crawled_at TIMESTAMPTZ,
    crawl_interval INTERVAL DEFAULT '7 days',
    UNIQUE (source_type, domain)
);

-- Optimized restaurant_references table with hash tracking
CREATE TABLE restaurant_references (
    reference_id SERIAL PRIMARY KEY,
    restaurant_id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    reference_url TEXT,
    sentiment_score FLOAT NOT NULL 
        CHECK (sentiment_score >= -1 AND sentiment_score <= 1),
    external_id VARCHAR(255),
    url_hash BYTEA GENERATED ALWAYS AS (
        CASE 
            WHEN reference_url IS NOT NULL 
            THEN DECODE(MD5(reference_url), 'hex') 
            ELSE NULL 
        END
    ) STORED,
    content_hash BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id) 
        ON DELETE CASCADE,
    FOREIGN KEY (source_id) REFERENCES sources(source_id) 
        ON DELETE CASCADE,
    CHECK (reference_url IS NOT NULL OR external_id IS NOT NULL)
);

-- Hash-based unique constraints for restaurant_references
ALTER TABLE restaurant_references
  ADD CONSTRAINT unq_ref_url_hash 
  UNIQUE (restaurant_id, url_hash);

ALTER TABLE restaurant_references
  ADD CONSTRAINT unq_external_id 
  UNIQUE (restaurant_id, source_id, external_id);

-- Optimized indexes for restaurant_references and sources
CREATE INDEX idx_ref_url_hash ON restaurant_references USING HASH (url_hash);
CREATE INDEX idx_content_hash ON restaurant_references (content_hash);
CREATE INDEX idx_sentiment ON restaurant_references (sentiment_score);
CREATE INDEX idx_source_credibility ON sources (credibility_score);

-- Add geography index for restaurants (requires the Earthdistance and cube extensions)
CREATE INDEX idx_restaurant_geo 
    ON restaurants USING GIST (ll_to_earth(latitude, longitude));

-- New Priority Queue Table for URL management
CREATE TABLE priority_queue (
    url TEXT PRIMARY KEY,  -- Ensure each URL is unique
    priority INTEGER NOT NULL CHECK (priority BETWEEN 1 AND 100), 
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'processing', 'completed')),
    source_type VARCHAR(20) NOT NULL CHECK (source_type IN ('michelin', 'discovered')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt TIMESTAMPTZ,
    date_entered TIMESTAMPTZ DEFAULT NOW()
);

-- Index for efficient querying of the priority queue
CREATE INDEX idx_priority_queue_status 
    ON priority_queue (status, priority DESC, date_entered ASC);
