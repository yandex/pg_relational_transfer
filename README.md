# pg-relational-transfer

Инструмент для переноса взаимосвязанных данных из одной базы данных Postgresql в другую.

Предназначен для переноса части данных, которые связаны между собой через Foreign Key.

Также имеет функциональность работы со схемой базы данных: перенос схемы, её удаление и генерация PlantUML-диаграммы.

## Установка
Установка [uv](https://docs.astral.sh/uv):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
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
uv run -m src clone-schema --source-db postgresql://postgres:XXX@localhost:5432/source --target-db postgresql://postgres:XXX@localhost:5433/target
```

#### Очистка схемы
Очищает схему базы (таблицы), но не удаляет саму схему.

Если не указать параметр `--schema SCHEMA`, будет очищена схема `public`.

##### Примеры
Очистка схемы `my_schema`:
```
uv run -m src clear-schema --db postgresql://postgres:XXX@localhost:5432/db --schema my_schema
```

Очистка дефолтной схемы (`public`):
```
uv run -m src clear-schema --db postgresql://postgres:XXX@localhost:5432/db
```

#### Генерация PlantUML-диаграммы
Генерирует PlantUML-диаграмму схемы таблиц.

Есть возможность указать названия таблиц. Тогда сгенерируются диаграмма для этих и всех связанных таблиц.

Можно указать параметр `--output FILENAME`, тогда диаграмма будет сохранена в файл. По умолчанию выводится в стандартный поток вывода.

##### Примеры
Генерация диаграммы всех связанных таблиц с таблицами `my_table1`, `my_table2`, и сохранение в файл `my_schema.puml`:
```
uv run -m src print-schema --db postgresql://postgres:XXX@localhost:5432/db --table my_table1 --table my_table2 --output my_schema.puml
```

Генерация диаграммы всех таблиц в схеме, и вывод в стандартный поток вывода:
```
uv run -m src print-schema --db postgresql://postgres:XXX@localhost:5432/db
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
uv run -m src clone-data --source-db postgresql://postgres:XXX@localhost:5432/source --target-db postgresql://postgres:XXX@localhost:5433/target --rule-path "rules_1.json"
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
uv run -m src clone-data --source-db postgresql://postgres:XXX@localhost:5432/source --target-db postgresql://postgres:XXX@localhost:5433/target --rule-path "rules_2.json"
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
uv run -m src clear-data --db postgresql://postgres:XXX@localhost:5432/db
```
