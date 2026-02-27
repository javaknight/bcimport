# Copilot Instructions for bcimport

## Project Overview
BigCommerce product import tool. Currently one input system: **xologic** (XOlogic XLSX feed → BigCommerce).
All code lives under `xologic/`. Future input systems would get their own sibling directories.

## Architecture Decisions

### General
- Simple stateless pipeline: read feed → transform → upsert BC → produce reports
- No queues, no state management — if a run fails, re-feed the whole file
- Feed format: XLSX, read with **pandas** (not openpyxl directly)
- Libraries: `bigc` for BigCommerce API, `boto3` for AWS

### BigCommerce API
- **POST** `/v3/catalog/products`: one product at a time (no batch create endpoint)
- **PUT** `/v3/catalog/products`: batch of max **10** products, requires BC `id` field
- To use batch PUT, a prior SKU-lookup phase is needed to resolve BC `id`s
- Status codes: 200 success, 207 partial success (warning), 409/422 error — no retries
- Rate limiting must be built in from the start (~150 req/30s on standard plans)

### Error Reporting
- Produce `output/error_items.xlsx` for failed rows (409/422): preserve all original columns,
  append `bc_status_code` and `bc_error_message` columns so the file can be fixed and re-fed directly
- Produce `output/warnings.xlsx` (or flag in error file) for 207 responses — do NOT re-feed these,
  the product landed in BC but something partial failed; a human should inspect in BC control panel
- Run summary log line at end: `Processed X/Y: N success, W warnings (207), E errors. Elapsed: Xm Ys`

### Category Management
- A one-shot `tools/prime_categories.py` script creates/verifies the BC category tree with `is_visible: false`
  and writes `category_map.json` which the main processor loads at runtime
- `tools/activate_categories.py` reads `category_map.json` and sets `is_visible: true` on all entries —
  run this when you're ready to make the categories live on the storefront
- `category_map.json` is saved at `xologic/category_map.json`
- Main import never creates categories mid-run

### Field Mapping (XOlogic → BigCommerce)
- Filter: only process rows where `VendorID == 4460` and `Product Type` in `[0, 1, 4]`
- SKU: `LU-{Item Number}`
- GTIN: → `gtin`
- Item Name: → `name`
- Short Description: → `description` (base; links appended below)
- Width/Height: → `width` / `height`
- Extra-Length: → custom field `Length`
- Standard-Finish / Variant-Finish: → custom field `Finish`
- Standard-Style: → custom field `Style`
- Extra-Weight: → `weight`
- Image Path: → `images` with `is_thumbnail: true`
- Categories: Lutron parent + subcategory from feed, resolved via `category_map.json`
- Description HTML additions (appended as separate lines):
  - Extra-Installation Link, Extra-Line Drawing Link, Extra-Spec Sheet,
    Extra-Tech Drawing Link, Extra-Warranty Link, Extra-Video Clip → hyperlinks
  - Extra-UNSPSC → plain text line
  - Extra-Brochure → link

### Logging
- Standard Python `logging` module, INFO level default
- Format includes timestamp (`%(asctime)s`)
- Start/end timestamps, elapsed time in summary line

---

## Makefile Conventions
All Python projects follow this structure. Reference: `xologic/Makefile`.

### venv
- Lives inside the project subdirectory (e.g. `xologic/venv`), shared by all scripts in that dir
- Built via touchfile pattern: `venv/touch: requirements.txt`
- `python3 -m venv venv && pip install --upgrade pip && pip install -r requirements.txt`

### Standard targets
| Target | Purpose |
|---|---|
| `default` | `venv` |
| `all` | `venv lint test` |
| `test` | pytest tests/ -v |
| `lint` | pylint on all .py files |
| `format` | `black isort` |
| `black` | black . |
| `isort` | isort . |
| `run-sample` | run against `samplefeed/` with dev env |
| `run` | run against `input/` with dev env |
| `prime-categories` | one-shot category setup tool (creates categories with `is_visible: false`) |
| `activate-categories` | sets `is_visible: true` on all categories in `category_map.json` |
| `envfiles` | validate env files exist, error with helpful message if missing |
| `clean` | remove logs, pycache, pytest artifacts |
| `clean-env` | remove venv |
| `clean-all` | clean + clean-env |

### ENV files
- Live in `env/.env.dev`, `env/.env.prod`, `env/.env.stage`
- Fetched from S3: `aws s3 cp "s3://grandbrass-secrets-development/bcimport/$(@F)" $@`
- `envfiles` target triggers the S3 fetch for all three

### Dynamic environment target pattern (`$*` stem)
All runnable targets use the `%` pattern rule so any env suffix works automatically:
```makefile
run: run-dev
run-%: venv envfiles | $(OUTPUT_DIR)
    set -a && . $(ENV_DIR)/.env.$* && set +a && venv/bin/python processor.py --feed-dir $(INPUT_DIR)
```
- `make run` → dev (default)
- `make run-stage`, `make run-prod` — no extra targets needed
- Same pattern applies to `run-sample-%`, `prime-categories-%`, and any future runnable
- `.PHONY` must list the concrete variants explicitly (e.g. `run-dev run-stage run-prod`)

### CREATE_DIRS pattern
Directories declared in `CREATE_DIRS` variable, created via:
```makefile
$(CREATE_DIRS):
	$(printTarget)
	@mkdir -p $(@)
```

### printTarget helper
Always included at the bottom, green color output:
```makefile
TARGET_COLOR := \033[0;32m
NO_COLOR := \033[m
CURRENT_TARGET = $(@)

define printTarget
	@printf "%b" "\n$(TARGET_COLOR)$(CURRENT_TARGET):$(NO_COLOR) $^\n";
endef
```

### .PHONY
Declared at the bottom of the file listing all non-file targets.

---

## Directory Structure
```
bcimport/
├── .github/
│   └── copilot-instructions.md
├── notes/
│   └── fieldmapping.md
└── xologic/
    ├── Makefile
    ├── requirements.txt
    ├── processor.py          # Main orchestrator
    ├── readers/
    │   └── xlsx_reader.py    # pandas read_excel, filter by Vendor ID / Product Type
    ├── mappers/
    │   └── field_mapper.py   # XOlogic → BC payload, builds description HTML
    ├── bc/
    │   └── client.py         # bigc wrapper: SKU lookup, POST, batch PUT, rate limiting
    ├── tools/
    │   └── prime_categories.py  # One-shot: create BC category tree → category_map.json
    ├── env/
    │   └── .env.example
    ├── input/                # Production feed drop location
    ├── samplefeed/           # Test feed (lutron-data-highlighted.xlsx)
    ├── output/               # error_items.xlsx, warnings.xlsx
    └── logs/
```
