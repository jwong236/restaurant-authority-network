-- Drop tables if they exist (adjust order for dependencies)
DROP TABLE IF EXISTS restaurant_references;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS source_types;
DROP TABLE IF EXISTS restaurants;

-- Create restaurants table
CREATE TABLE restaurants (
    restaurant_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    is_michelin BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (name, address)
);

-- Optional: Source types lookup table
CREATE TABLE source_types (
    type_name VARCHAR(10) PRIMARY KEY CHECK (type_name IN ('blog', 'video', 'social'))
);

-- Create sources table
CREATE TABLE sources (
    source_id SERIAL PRIMARY KEY,
    source_type VARCHAR(10) REFERENCES source_types(type_name),
    domain VARCHAR(255) NOT NULL,
    UNIQUE (source_type, domain)
);

-- Create restaurant_references table
CREATE TABLE restaurant_references (
    reference_id SERIAL PRIMARY KEY,
    restaurant_id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    reference_url TEXT,
    external_id VARCHAR(255),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id) ON DELETE CASCADE,
    FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE,
    CHECK (reference_url IS NOT NULL OR external_id IS NOT NULL)
);

-- Drop existing indexes
DROP INDEX IF EXISTS unq_ref_url;
DROP INDEX IF EXISTS unq_external_id;

-- Add proper unique constraints
ALTER TABLE restaurant_references
  ADD CONSTRAINT unq_ref_url
  UNIQUE (restaurant_id, source_id, reference_url);

ALTER TABLE restaurant_references
  ADD CONSTRAINT unq_external_id
  UNIQUE (restaurant_id, source_id, external_id);