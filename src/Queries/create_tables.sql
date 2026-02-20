
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


-- dim_users: User dimension
-- Distribution: ALL (for fast joins with fact table)
CREATE TABLE IF NOT EXISTS dim_users (
    user_key        BIGINT          NOT NULL    ENCODE az64,
    user_id         VARCHAR(50)     NOT NULL    ENCODE lzo,
    subscription_tier VARCHAR(20)   DEFAULT 'FREE'      ENCODE lzo,
    signup_date     DATE                        ENCODE az64,
    region          VARCHAR(50)                 ENCODE lzo,
    first_seen      TIMESTAMP                   ENCODE az64,
    last_seen       TIMESTAMP                   ENCODE az64,
    primary_device  VARCHAR(50)                 ENCODE lzo,
    created_at      TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,
    updated_at      TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,
    PRIMARY KEY (user_key)
)
DISTSTYLE ALL
SORTKEY (user_id);


-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- fact_listens: Core fact table for listening events
-- Distribution: KEY on user_key (enables efficient user-centric queries)
-- Sort: Compound sort on event_timestamp for time-range queries
CREATE TABLE IF NOT EXISTS fact_listens (
    listen_key      BIGINT          NOT NULL    ENCODE az64,
    user_key        BIGINT          NOT NULL    ENCODE az64,
    track_key       BIGINT          NOT NULL    ENCODE az64,
    date_key        INT             NOT NULL    ENCODE az64,
    session_id      VARCHAR(50)     NOT NULL    ENCODE lzo,
    action          VARCHAR(20)     NOT NULL    ENCODE lzo,  -- PLAY, PAUSE, SKIP, COMPLETE
    duration_ms     INT             NOT NULL    ENCODE az64,
    device_type     VARCHAR(30)     NOT NULL    ENCODE lzo,
    event_timestamp TIMESTAMP       NOT NULL    ENCODE az64,
    loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,
    PRIMARY KEY (listen_key)
)
DISTSTYLE KEY
DISTKEY (user_key)
COMPOUND SORTKEY (event_timestamp, user_key, action);


-- ============================================================================
-- FOREIGN KEY CONSTRAINTS (for documentation, not enforced)
-- ============================================================================

ALTER TABLE fact_listens ADD CONSTRAINT fk_user 
    FOREIGN KEY (user_key) REFERENCES dim_users(user_key);

ALTER TABLE fact_listens ADD CONSTRAINT fk_track 
    FOREIGN KEY (track_key) REFERENCES dim_tracks(track_key);

ALTER TABLE fact_listens ADD CONSTRAINT fk_date 
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key);


-- ============================================================================
-- STAGING TABLES (for upsert operations)
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_users_staging (LIKE dim_users);
CREATE TABLE IF NOT EXISTS dim_tracks_staging (LIKE dim_tracks);


-- ============================================================================
-- MATERIALIZED VIEWS (for common aggregations)
-- ============================================================================

-- Daily listening summary by user
CREATE MATERIALIZED VIEW mv_daily_user_summary AS
SELECT 
    d.full_date,
    u.user_id,
    u.subscription_tier,
    COUNT(*) AS total_events,
    SUM(CASE WHEN f.action = 'PLAY' THEN 1 ELSE 0 END) AS plays,
    SUM(CASE WHEN f.action = 'SKIP' THEN 1 ELSE 0 END) AS skips,
    SUM(CASE WHEN f.action = 'COMPLETE' THEN 1 ELSE 0 END) AS completes,
    SUM(f.duration_ms) / 1000.0 / 60.0 AS total_minutes,
    COUNT(DISTINCT f.track_key) AS unique_tracks,
    COUNT(DISTINCT f.session_id) AS sessions
FROM fact_listens f
JOIN dim_users u ON f.user_key = u.user_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, u.user_id, u.subscription_tier;


-- Track performance summary
CREATE MATERIALIZED VIEW mv_track_performance AS
SELECT 
    t.track_id,
    t.title,
    t.artist,
    COUNT(*) AS total_events,
    SUM(CASE WHEN f.action = 'COMPLETE' THEN 1 ELSE 0 END) AS completions,
    SUM(CASE WHEN f.action = 'SKIP' THEN 1 ELSE 0 END) AS skips,
    ROUND(
        100.0 * SUM(CASE WHEN f.action = 'SKIP' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS skip_rate,
    AVG(f.duration_ms) / 1000.0 AS avg_listen_seconds,
    COUNT(DISTINCT f.user_key) AS unique_listeners
FROM fact_listens f
JOIN dim_tracks t ON f.track_key = t.track_key
GROUP BY t.track_id, t.title, t.artist;


-- ============================================================================
-- GRANTS
-- ============================================================================

-- Read access for BI tools
GRANT USAGE ON SCHEMA audio_analytics TO GROUP bi_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA audio_analytics TO GROUP bi_analysts;

-- Write access for ETL
GRANT ALL ON SCHEMA audio_analytics TO GROUP etl_jobs;
GRANT ALL ON ALL TABLES IN SCHEMA audio_analytics TO GROUP etl_jobs;
