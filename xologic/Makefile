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

envfile-%: $(ENV_DIR)/.env.%
	@:

default: venv

all: venv lint test

test: venv
	$(printTarget)
	. venv/bin/activate && python -m pytest tests/ -v

lint: venv
	$(printTarget)
	. venv/bin/activate && pylint --fail-under=9.0 $(LINTDIRS)/*.py

format: black isort

black: venv
	$(printTarget)
	. venv/bin/activate && black .

isort: venv
	$(printTarget)
	. venv/bin/activate && isort .

run-sample:
	$(MAKE) run-sample-dev
run-sample-%: venv envfile-% | $(OUTPUT_DIR)
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python processor.py --feed $(SAMPLE_FEED) --vendor lutron --sku CAR2S-20-STR-BL

run:
	$(MAKE) run-dev
run-%: venv envfile-% | $(OUTPUT_DIR)
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python processor.py --feed-dir $(INPUT_DIR) --vendor lutron

prime-lutron-categories:
	$(MAKE) prime-lutron-categories-dev
prime-lutron-categories-%: venv envfile-%
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/prime_categories.py --vendor lutron

prime-teardown-lutron-categories:
	$(MAKE) prime-teardown-lutron-categories-dev
prime-teardown-lutron-categories-%: venv envfile-%
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/prime_categories.py --vendor lutron --teardown

activate-lutron-categories:
	$(MAKE) activate-lutron-categories-dev
activate-lutron-categories-%: venv envfile-%
	$(printTarget)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/activate_categories.py --vendor lutron

poc-metafield:
	$(MAKE) poc-metafield-dev
poc-metafield-%: venv envfile-%
	$(printTarget)
	@test -n "$(PRODUCT_ID)" || (echo "Usage: make poc-metafield PRODUCT_ID=<bc_product_id>" && exit 1)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/poc_metafield_search.py --product-id $(PRODUCT_ID)

activate-products:
	$(MAKE) activate-products-dev
activate-products-%: venv envfile-%
	$(printTarget)
	@test -n "$(RUN_ID)" || (echo "Usage: make activate-products RUN_ID=<run-id>  (run ID is logged at the start of each import run)" && exit 1)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/activate_products.py --run-id $(RUN_ID)

delete-run-products:
	$(MAKE) delete-run-products-dev
delete-run-products-%: venv envfile-%
	$(printTarget)
	@test -n "$(RUN_ID)" || (echo "Usage: make delete-run-products RUN_ID=<run-id>" && exit 1)
	set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python tools/delete_run_products.py --run-id $(RUN_ID) --confirm --papply

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

push-config:
	$(MAKE) push-config-dev

push-config-%: envfile-%
	$(printTarget)
	@test -f $(ENV_DIR)/.env.$* || \
		(echo "ERROR: env/.env.$* not found — copy from env/.env.example and fill in values." && exit 1)
	set -a && . $(ENV_DIR)/.env.$* && set +a && \
		aws s3 cp $(ENV_DIR)/.env.$* \
		s3://$$S3_SECRETS_BUCKET/.env && \
		echo "Pushed: env/.env.$* → s3://$$S3_SECRETS_BUCKET/.env"

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
	prime-lutron-categories prime-teardown-lutron-categories activate-lutron-categories poc-metafield \
	activate-products delete-run-products \
	envfiles clean clean-env clean-all venv \
	push-config
