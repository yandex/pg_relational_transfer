# pg-relational-transfer

Инструмент для переноса взаимосвязанных данных из одной базы данных Postgresql в другую.

Предназначен для переноса части данных, которые связаны между собой через Foreign Key.

Также имеет функциональность работы со схемой базы данных: перенос схемы, её удаление и генерация PlantUML-диаграммы.

## Установка
Требуется Python 3.13 или новее.

Установка [uv](https://docs.astral.sh/uv):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Из корня репозитория установите зависимости:
```bash
uv sync
```

## Использование
```
uv run -m src [OPTIONS] COMMAND [ARGS]...
```

### Вывод справки
Для просмотра справки по доступным командам:
```
uv run -m src --help
```
Для просмотра справки по конкретной команде:
```
uv run -m src COMMAND --help
```

### Работа со схемой
#### Клонирование схемы
Переносит схему таблиц, включая пользовательские типы, последовательности (sequences) и расширения (extensions).

По умолчанию переносится схема `public SCHEMA` -- регулируется переменной окружения `SOURCE_SCHEMA`.

##### Примеры
```
uv run -m src clone-schema --source-db postgresql://postgres@localhost:5432/source --target-db postgresql://postgres@localhost:5433/target
```

#### Очистка схемы
Очищает схему базы (таблицы), но не удаляет саму схему.

Если не указать параметр `--schema SCHEMA`, будет очищена схема `public`.

##### Примеры
Очистка схемы `my_schema`:
```
uv run -m src clear-schema --db postgresql://postgres@localhost:5432/db --schema my_schema
```

Очистка схемы по умолчанию (`public`):
```
uv run -m src clear-schema --db postgresql://postgres@localhost:5432/db
```

#### Генерация PlantUML-диаграммы
Генерирует PlantUML-диаграмму схемы таблиц.

Есть возможность указать названия таблиц. Тогда сгенерируются диаграмма для этих и всех связанных таблиц.

Можно указать параметр `--output FILENAME`, тогда диаграмма будет сохранена в файл. По умолчанию выводится в стандартный поток вывода.

##### Примеры
Генерация диаграммы всех связанных таблиц с таблицами `my_table1`, `my_table2`, и сохранение в файл `my_schema.puml`:
```
uv run -m src print-schema --db postgresql://postgres@localhost:5432/db --table my_table1 --table my_table2 --output my_schema.puml
```

Генерация диаграммы всех таблиц в схеме, и вывод в стандартный поток вывода:
```
uv run -m src print-schema --db postgresql://postgres@localhost:5432/db
```

### Работа с данными
#### Перенос данных
Переносит взаимосвязанные данные из одной базы в другую.
Алгоритм переноса оперирует двумя основными понятиями:
- Граф таблиц -- это граф, вершины которого однозначно соответствуют таблицам базы, а дуги (ребра) -- связям между таблицами (по умолчанию связи формируются по Foreign Key).
- Граф данных -- это граф, вершины которого однозначно соответствуют конкретным данным, а дуги (ребра) -- связям между данными.

По умолчанию используется алгоритм BFS по неориентированному графу данных.

Помимо параметров `--source-db DSN` и `--target-db DSN`, имеет обязательный параметр `--rule-path PATH`, который указывает на путь к файлу с правилами переноса данных.

##### Правила переноса данных
На верхнем уровне правила можно разделить на два типа:
- `source rules` -- правила, описывающие стартовые вершины графа (таблиц/данных)
- `traversal rules` -- правила, описывающие изменения графа (таблиц/данных)

В общем виде правила описываются с помощью json-файла:
```
{
    "source_rules": [rule1, rule2, ...],
    "traversal_rules": [rule1, rule2, ...]
}
```

###### source_rules
Каждый элемент `source_rules` имеет вид:
```
{"table": "table_name", "where": "condition"}
```
где:
 - `table_name` -- имя стартовой вершины графа таблиц. Имена таблиц в `source_rules` не должны повторяться.
 - `condition` -- условие на языке SQL, которое будет использоваться для выборки стартовых вершин графа данных.

###### traversal_rules
Каждый элемент `traversal_rules` имеет вид:
```
{"type": "rule_type", "values": [value1, value2, ...]}
```
где `rule_type` -- тип правила, `values` -- список значений для правила.

Поддерживаются следующие типы правил:
- `no_exit` -- не выходить из указанных вершин графа таблиц/данных, т.е. можно сказать, удалять дуги, выходящие из указанных вершин. Элемент списка `values` имеет вид: `{"table":  "table_name", "where":  "condition"}`, где `table_name` -- имя вершины графа таблиц, `condition` -- условие на языке SQL, которое будет использоваться для выборки вершин графа данных. Если `values` не указать, то выбираются все вершины таблицы, т.е. ограничение идет на уровне графа таблиц.
- `no_enter` -- не входить в указанные вершины графа таблиц/данных, т.е. можно сказать, удалять дуги, входящие в указанные вершины. Формат элемента `values` аналогичен формату правила `no_exit`.
- `limit_distance` -- ограничить путь, начиная с определенных вершин графа таблиц. Элемент списка `values` имеет вид: `{"table": "table_name", "max_distance": number}`, где `table_name` -- имя вершины графа таблиц, `number` -- максимальная длина пути от вершины `table_name`.

##### Примеры
Перенос данных со следующими правилами: начало обхода графа данных с вершин таблицы `readers`, где `readers_id=2`; не выходить из вершин таблицы `rentals`, где `status='completed'`; не входить в вершины таблицы `authors`.
```
uv run -m src clone-data --source-db postgresql://postgres@localhost:5432/source --target-db postgresql://postgres@localhost:5433/target --rule-path "rules_1.json"
```
`rules_1.json`:
```json
{
  "source_rules": [
    {
      "table": "readers",
      "where": "reader_id=2"
    }
  ],
  "traversal_rules": [
    {
      "type": "no_exit",
      "values": [
        {
          "table": "rentals",
          "where": "status='completed'"
        }
      ]
    },
    {
      "type": "no_enter",
      "values": [
        {
          "table": "authors"
        }
      ]
    }
  ]
}
```

Перенос данных со следующими правилами: начало обхода графа данных с вершин таблицы `readers`, где `readers_id=2`, и с вершин таблицы `author_details`; ограничить путь, начиная с вершины `author_details`, до 1 вершины, а путь, начиная с вершины `readers`, до 2 вершин.
```
uv run -m src clone-data --source-db postgresql://postgres@localhost:5432/source --target-db postgresql://postgres@localhost:5433/target --rule-path "rules_2.json"
```
`rules_2.json`:
```json
{
  "source_rules": [
    {
      "table": "readers",
      "where": "reader_id=2"
    },
    {
      "table": "author_details",
      "where": "author_id=2"
    }
  ],
  "traversal_rules": [
    {
      "type": "limit_distance",
      "values": [
        {
          "table": "author_details",
          "max_distance": 1
        },
        {
          "table": "readers",
          "max_distance": 2
        }
      ]
    }
  ]
}
```

#### Удаление данных
Удаляет все данные в указанной базе данных.

##### Примеры
```
uv run -m src clear-data --db postgresql://postgres@localhost:5432/db
```

#### Перенос по дереву таблиц

Для явно заданного пути используется пара `--walker table_tree_walker --writer via_fdw_batch`.
В файле дерева отступы задают вложенность, а условие в двойных кавычках фильтрует строки таблицы:

```text
authors "authors.author_id = 1"
  (author_id,) -> books.(author_id,) "books.book_id = 1"
    (book_id,) -> rentals.(book_id,)
      (reader_id,) -> readers.(reader_id,)
```

```bash
uv run -m src clone-data --source-db postgresql://postgres@localhost:5432/source --target-db postgresql://postgres@localhost:5433/target --rule-path tests/rules/tree_authors_with_where.tree --walker table_tree_walker --writer via_fdw_batch
```

Для обхода графа с JSON-правилами по умолчанию используются `data_graph_walker` и `via_fdw_single`.

#### Построение дерева обхода

Команда `build-path` сохраняет дерево связей таблиц в текстовый файл. Для существующих правил
`no_enter`, `no_exit` и `limit_distance` укажите `--rule-loader-type excluding`:

```bash
uv run -m src build-path --db postgresql://postgres@localhost:5432/source --rule-path tests/rules/source_authors_1.json --rule-loader-type excluding --max-depth 3 --output-path path.tree
```

Режим `including` (по умолчанию) строит граф из разрешённых связей: `add_node` добавляет исходящие связи
таблицы (`table`), `add_edges` — связи из `from_table` в `to_tables`, а `add_edge` — одну существующую связь
по `from_table`, `to_table`, `from_key` и `to_key`. Правила задаются в `traversal_rules` того же JSON-файла.
Условия `where` из `source_rules` не переносятся в сгенерированное дерево; нужные фильтры добавляются в него вручную.

#### Переменные окружения

- SOURCE_SCHEMA - название схемы исходной базы данных. По умолчанию `public`.
- TARGET_SCHEMA - название схемы целевой базы данных. По умолчанию `public`.
- REMOTE_SCHEMA - название схемы для FWD-сервера в целевой базе данных. По умолчанию `remote`.
- CONNECTION_POOL_SIZE - размер пула подключений. По умолчанию `5`.
- STREAM_LOG_LEVEL - уровень логирования. По умолчанию `INFO`.
- QUERIES_LOG_FILENAME - названия файла для логирования запросов. По умолчанию `queries_log.txt`.
- OVERRIDE_REMOTE_HOST - хост исходной базы данных для подключения через FDW из целевой базы. По умолчанию берётся из DSN исходной базы. Работает для команды `clone-data`.
- OVERRIDE_REMOTE_PORT - порт исходной базы данных для подключения через FDW из целевой базы. По умолчанию берётся из DSN исходной базы. Работает для команды `clone-data`.
- SOURCE_DB_PASSWORD - пароль к исходной базе данных. Работает для команд, требующих две базы данных.
- TARGET_DB_PASSWORD - пароль к целевой базе данных. Работает для команд, требующих две базы данных.
- DB_PASSWORD - пароль к базе данных. Работает для команд, требующих одну базу данных.

Если пароль отсутствует в DSN, команды клонирования, очистки и `print-schema` используют соответствующую
переменную окружения или запрашивают пароль интерактивно. `build-path` получает параметры подключения из DSN.

## Разработка и тестирование

`uv sync` устанавливает также группу `dev` с pytest и Ruff. Команды ниже выполняются из корня репозитория.

Для локальных баз можно скопировать `.env.sample` в `.env` и выполнить `make db`.
Тесты используют отдельный PostgreSQL 16 на `127.0.0.1:55432`; для запуска через Compose нужны Docker и Docker Compose:

```bash
cp .env.sample .env
make test-db
make test
docker compose --profile test stop test-db
```

Вместо `make test` можно выполнить `uv run python -m pytest`. Тесты создают и удаляют базы
`pg_relational_transfer_test_source` и `pg_relational_transfer_test_target`.
Для отдельного тестового сервера доступны `TEST_DB_HOST`, `TEST_DB_PORT`, `TEST_DB_USER` и `TEST_DB_PASSWORD`;
по умолчанию используются `127.0.0.1`, `55432`, `postgres` и `password`. Пользователю PostgreSQL нужны права
на создание и удаление баз, установку расширений `btree_gist`, `postgres_fdw` и настройку FDW.
Тестовые базы не должны содержать нужные данные.

Проверка стиля: `make lint`. Форматирование: `make format`.
