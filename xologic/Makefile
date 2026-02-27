BASEDIR = .
PROJECT = bcimport-xologic

export LOGDIR = $(BASEDIR)/logs
OUTPUT_DIR = $(BASEDIR)/output

SAMPLE_FEED = $(BASEDIR)/samplefeed/lutron-data-highlighted.xlsx
INPUT_DIR = $(BASEDIR)/input

CREATE_DIRS = \
	$(LOGDIR) \
	$(OUTPUT_DIR)

ENV_DIR = $(BASEDIR)/env

ENV_FILES = \
	$(ENV_DIR)/.env.dev \
	$(ENV_DIR)/.env.prod \
	$(ENV_DIR)/.env.stage

LINTDIRS = \
	$(BASEDIR)

default: venv

all: venv lint test

test: venv
	$(printTarget)
	. venv/bin/activate && python -m pytest tests/ -v

lint: venv
	$(printTarget)
	. venv/bin/activate && pylint $(LINTDIRS)/*.py

format: black isort

black: venv
	$(printTarget)
	. venv/bin/activate && black .

isort: venv
	$(printTarget)
	. venv/bin/activate && isort .

run-sample:
	$(MAKE) run-sample-dev
run-sample-%: venv envfiles | $(OUTPUT_DIR)
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python processor.py --feed $(SAMPLE_FEED)

run:
	$(MAKE) run-dev
run-%: venv envfiles | $(OUTPUT_DIR)
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python processor.py --feed-dir $(INPUT_DIR)

prime-categories:
	$(MAKE) prime-categories-dev
prime-categories-%: venv envfiles
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/prime_categories.py

activate-categories:
	$(MAKE) activate-categories-dev
activate-categories-%: venv envfiles
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/activate_categories.py

clean:
	$(printTarget)
	-rm -rf $(LOGDIR) .pytest_cache .mypy_cache
	-find . -type d -name '__pycache__' -exec rm -rf {} +
	-find . -type f -name '*.pyc' -delete

clean-env:
	$(printTarget)
	-rm -rf venv

clean-all: clean clean-env

venv: venv/touch

venv/touch: requirements.txt
	$(printTarget)
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
	touch $@

envfiles: $(ENV_FILES)

$(ENV_FILES):
	$(printTarget)
	@mkdir -p $(ENV_DIR)
	aws s3 cp "s3://grandbrass-secrets-development/bcimport/$(@F)" $@

$(CREATE_DIRS):
	$(printTarget)
	@mkdir -p $(@)

#Helper function to pretty print targets as they execute
TARGET_COLOR := \033[0;32m
NO_COLOR := \033[m
CURRENT_TARGET = $(@)

define printTarget
	@printf "%b" "\n$(TARGET_COLOR)$(CURRENT_TARGET):$(NO_COLOR) $^\n";
endef

.PHONY: all test lint format black isort \
	run-sample run \
	prime-categories activate-categories \
	envfiles clean clean-env clean-all venv
