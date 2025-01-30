-- Videos Table
CREATE TABLE videos (
    id INT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key
    url TEXT NOT NULL UNIQUE,           -- Unique URL of the video
    sentiment TEXT,                     -- Sentiment of the video
    source TEXT,                        -- Source platform (e.g., YouTube)
    crawl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,  -- When the video was crawled
    processed_timestamp DATETIME        -- When the video was processed
);

-- Locations Table
CREATE TABLE locations (
    id INT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key
    video_id INT NOT NULL,              -- Foreign key to the videos table
    address TEXT NOT NULL,              -- Location mentioned in the video
    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE
);

-- Staging Table
CREATE TABLE staging (
    id INT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key
    url TEXT NOT NULL UNIQUE,           -- URL of the video
    crawl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,  -- When the URL was crawled
    status TEXT DEFAULT 'pending'       -- Status of the URL (e.g., 'pending', 'processed')
);