CREATE EXTENSION btree_gist;

CREATE TYPE rental_status AS ENUM ('in_progress', 'completed');

CREATE SEQUENCE authors_seq START 1;
CREATE SEQUENCE unused_seq START 25;

CREATE TABLE authors (
    author_id INT PRIMARY KEY DEFAULT nextval('authors_seq'),
    name VARCHAR(255) NOT NULL,
    birthdate DATE
);

CREATE TABLE book_series (
    series_id SERIAL PRIMARY KEY,
    series_name VARCHAR(255) NOT NULL
);

CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    published_date DATE,
    author_id INT,
    series_id INT,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
CREATE INDEX idx_books_title ON books(title);

CREATE TABLE readers (
    reader_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    membership_date DATE
); -- PARTITION BY RANGE (membership_date);

-- Создание партиций
-- CREATE TABLE Readers_2020 PARTITION OF readers
-- FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

-- CREATE TABLE Readers_2019 PARTITION OF readers
-- FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

-- CREATE TABLE Readers_2021 PARTITION OF readers
-- FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE rentals (
    book_id INT,
    reader_id INT,
    rental_daterange DATERANGE,
    PRIMARY KEY (book_id, reader_id),
    FOREIGN KEY (book_id) REFERENCES books(book_id),
    FOREIGN KEY (reader_id) REFERENCES readers(reader_id),
    status rental_status
);
CREATE INDEX ON rentals USING gist (rental_daterange);

CREATE TABLE author_details (
    author_id INT PRIMARY KEY,
    biography TEXT,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

ALTER TABLE books
ADD CONSTRAINT FK_series_book FOREIGN KEY (series_id) REFERENCES book_series(series_id);

SELECT setval('unused_seq', 100);