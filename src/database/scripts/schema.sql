-- Drop tables in the correct order to avoid foreign key constraint issues
DROP TABLE IF EXISTS priority_queue;
DROP TABLE IF EXISTS reference;
DROP TABLE IF EXISTS url;
DROP TABLE IF EXISTS restaurant;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS domain;

-- Recreate tables
CREATE TABLE domain (
    id SERIAL PRIMARY KEY,
    domain_name TEXT UNIQUE NOT NULL,
	visit_count INT DEFAULT 0
);

CREATE TABLE source (
    id SERIAL PRIMARY KEY,
    domain_id INT REFERENCES domain(id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    credibility_score FLOAT CHECK (credibility_score >= 0 AND credibility_score <= 1),
    UNIQUE(domain_id, source_type)
);

CREATE TABLE restaurant (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT,
    UNIQUE(name, address)
);

CREATE TABLE url (
    id SERIAL PRIMARY KEY,
    source_id INT REFERENCES source(id) ON DELETE CASCADE,
    full_url TEXT NOT NULL,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_crawled TIMESTAMP,
    UNIQUE(full_url)
);

CREATE TABLE reference (
    id SERIAL PRIMARY KEY,
    restaurant_id INT REFERENCES restaurant(id) ON DELETE CASCADE,
    url_id INT REFERENCES url(id) ON DELETE CASCADE,
    relevance_score FLOAT CHECK (relevance_score >= 0 AND relevance_score <= 1),
    discovered_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE priority_queue (
    url_id INT PRIMARY KEY REFERENCES url(id) ON DELETE CASCADE,
    priority INT DEFAULT 1,
    queued_at TIMESTAMP DEFAULT NOW()
);
