INSERT INTO authors (name, birthdate) VALUES
('Джоан Роулинг', '1965-07-31'),
('Джордж Мартин', '1948-09-20');

INSERT INTO book_series (series_name) VALUES
('Гарри Поттер'),
('Песнь Льда и Огня');

INSERT INTO books (title, published_date, author_id, series_id) VALUES
('Гарри Поттер и философский камень', '1997-06-26', 1, 1),
('Гарри Поттер и Тайная комната', '1998-07-02', 1, 1),
('Игра престолов', '1996-08-06', 2, 2),
('Битва королей', '1998-11-16', 2, 2);

INSERT INTO readers (reader_id, name, membership_date) VALUES
(1, 'Иван Иванов', '2020-01-15'),
(2, 'Мария Петрова', '2019-04-23'),
(3, 'Ольга Смирнова', '2021-03-11');

INSERT INTO rentals (book_id, reader_id, rental_daterange, status) VALUES
(1, 1, '[2021-01-15, 2021-02-10]', 'completed'),
(2, 1, '[2021-02-15, 2021-03-10]', 'completed'),
(3, 2, '[2021-01-20, 2021-02-20]', 'in_progress'),
(4, 2, '[2021-03-01, 2021-04-01]', 'completed');

INSERT INTO author_details (author_id, biography) VALUES
(1, 'Джоан Роулинг - британская писательница, автор серии книг о Гарри Поттере.'),
(2, 'Джордж Мартин - американский писатель, автор фэнтезийного цикла "Песнь Льда и Огня".');

UPDATE books SET series_id = 1 WHERE book_id IN (1, 2);
UPDATE books SET series_id = 2 WHERE book_id IN (3, 4);
