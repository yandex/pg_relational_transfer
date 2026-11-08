ifeq ($(shell test -e '.env' && echo -n yes),yes)
	include .env
endif

CODE = src tests

args := $(wordlist 2, 100, $(MAKECMDGOALS))
ifndef args
MESSAGE = "No such command (or you pass two or many targets to ). List of possible commands: make help"
else
MESSAGE = "Done"
endif

HELP_FUN = \
	%help; while(<>){push@{$$help{$$2//'options'}},[$$1,$$3] \
	if/^([\w-_]+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/}; \
    print"$$_:\n", map"  $$_->[0]".(" "x(20-length($$_->[0])))."$$_->[1]\n",\
    @{$$help{$$_}},"\n" for keys %help; \

# Commands
help: ##@Help Show this help
	@echo -e "Usage: make [target] ...\n"
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)

run:  ##@Application Run application
	uv run -m src $(args)

install:  ##@Install Install dependencies
	uv sync --locked --group dev

env:  ##@Environment Create .env file with variables
	@$(eval SHELL:=/bin/bash)
	@cp .env.sample .env

db:  ##@Database Create database with docker-compose
	docker compose -f docker-compose.yml up -d

source-db:  ##@Database Create source database with docker-compose
	docker compose -f docker-compose.yml up source-db -d

target-db:  ##@Database Create target database with docker-compose
	docker compose -f docker-compose.yml up target-db -d

test-db:  ##@Database Start isolated PostgreSQL for tests
	docker compose --profile test up -d --wait test-db

test:  ##@Code Run tests (requires make test-db)
	uv run python -m pytest

open-source:  ##@Database Open source database
	PGPASSWORD=${DB_SOURCE_PASSWORD} psql -U $(DB_SOURCE_USERNAME) -h localhost -p $(DB_SOURCE_PORT) ${DB_SOURCE_NAME}

open-target:  ##@Database Open target database
	PGPASSWORD=${DB_TARGET_PASSWORD} psql -U $(DB_TARGET_USERNAME) -h localhost -p $(DB_TARGET_PORT) ${DB_TARGET_NAME}

lint:  ##@Code Check code with ruff
	uv run ruff check --config pyproject.toml $(CODE)

format: ##@Code Reformat code with ruff
	uv run ruff format --config pyproject.toml $(CODE)
	uv run ruff check --fix --config pyproject.toml $(CODE)

%::
	@if [ "$(words $(MAKECMDGOALS))" -eq 1 ]; then \
		echo $(MESSAGE); \
	fi
