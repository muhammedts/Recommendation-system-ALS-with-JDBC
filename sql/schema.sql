-- Tables the job reads and writes. The companion Laravel app creates these through its
-- migrations; use this file only when running the job against your own database.

CREATE TABLE IF NOT EXISTS ratings (
    user_id    BIGINT UNSIGNED  NOT NULL,
    book_id    BIGINT UNSIGNED  NOT NULL,
    rating     TINYINT UNSIGNED NOT NULL CHECK (rating BETWEEN 1 AND 5),
    PRIMARY KEY (user_id, book_id)
);

CREATE TABLE IF NOT EXISTS recommendations (
    user_id      BIGINT UNSIGNED  NOT NULL,
    book_id      BIGINT UNSIGNED  NOT NULL,
    `rank`       TINYINT UNSIGNED NOT NULL,
    score        DOUBLE           NOT NULL,
    generated_at TIMESTAMP        NOT NULL,
    PRIMARY KEY (user_id, `rank`)
);
