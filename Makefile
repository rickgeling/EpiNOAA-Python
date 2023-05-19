MAKEFLAGS += -j6

default: help

## Install Python dependencies
install: 
	poetry install

## Update Python dependencies
update:
	poetry update

## Commit using best practices for git
commit:
	poetry run cz commit

## Launc MKDocs development server
serve:
	cd docs; \
	poetry run mkdocs serve

## Make NClimGrid Visualizations
visualizations:
	time poetry run python nclimgrid_importer/harris_county.py
	time poetry run python nclimgrid_importer/temp_diff.py

## Build Documentation Site
build:
	cd docs; \
	poetry run mkdocs build


# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)


TARGET_MAX_CHAR_NUM=24
## Show help
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)
