
-- Create schema
CREATE SCHEMA IF NOT EXISTS audio_analytics;
SET search_path TO audio_analytics;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- dim_date: Calendar dimension for time-based analysis
-- Distribution: ALL (small table, replicated to all nodes)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT             NOT NULL    ENCODE az64,
    full_date       DATE            NOT NULL    ENCODE az64,
    day_of_week     SMALLINT        NOT NULL    ENCODE az64,
    day_name        VARCHAR(10)     NOT NULL    ENCODE lzo,
    month           SMALLINT        NOT NULL    ENCODE az64,
    month_name      VARCHAR(10)     NOT NULL    ENCODE lzo,
    quarter         SMALLINT        NOT NULL    ENCODE az64,
    year            SMALLINT        NOT NULL    ENCODE az64,
    is_weekend      BOOLEAN         NOT NULL    ENCODE raw,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);


-- dim_tracks: Track/song dimension
-- Distribution: ALL (relatively small, frequently joined)
CREATE TABLE IF NOT EXISTS dim_tracks (
    track_key       BIGINT          NOT NULL    ENCODE az64,
    track_id        VARCHAR(50)     NOT NULL    ENCODE lzo,
    title           VARCHAR(255)    NOT NULL    ENCODE lzo,
    artist          VARCHAR(255)    NOT NULL    ENCODE lzo,
    album           VARCHAR(255)                ENCODE lzo,
    genre           VARCHAR(100)                ENCODE lzo,
    duration_ms     INT                         ENCODE az64,
    release_date    DATE                        ENCODE az64,
    created_at      TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,
    updated_at      TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,
    PRIMARY KEY (track_key)
)
DISTSTYLE ALL
SORTKEY (artist, title);

