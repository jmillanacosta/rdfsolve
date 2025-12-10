# Makefile for rdfsolve notebook generation
# Can be used both locally and in GitHub Actions

# Variables - can be overridden by environment or command line
DATASET ?=
NOTEBOOK_TYPE ?= all
VENV_DIR ?= .venv
# Use absolute path for Python to avoid issues with cd
PYTHON ?= $(shell pwd)/$(VENV_DIR)/bin/python
PIP ?= $(shell pwd)/$(VENV_DIR)/bin/pip
JUPYTER ?= $(shell pwd)/$(VENV_DIR)/bin/jupyter
MAX_PARALLEL ?= 50
GITHUB_OUTPUT ?= /dev/null
GITHUB_STEP_SUMMARY ?= /dev/null

# Directories
NOTEBOOKS_DIR := notebooks
DOCS_DIR := docs
DOCS_NOTEBOOKS_DIR := $(DOCS_DIR)/notebooks
DOCS_DATA_DIR := $(DOCS_DIR)/data/schema_extraction
ARTIFACTS_DIR := artifacts

SCHEMA_NB_DIR := $(NOTEBOOKS_DIR)/01_schema_extraction
PYDANTIC_NB_DIR := $(NOTEBOOKS_DIR)/02_pydantic_models
NAMESPACE_NB_DIR := $(NOTEBOOKS_DIR)/03_bioregistry_namespaces

SCHEMA_DOCS_DIR := $(DOCS_NOTEBOOKS_DIR)/01_schema_extraction
PYDANTIC_DOCS_DIR := $(DOCS_NOTEBOOKS_DIR)/02_pydantic_models
NAMESPACE_DOCS_DIR := $(DOCS_NOTEBOOKS_DIR)/03_bioregistry_namespaces

# Colors for output
COLOR_RESET := \033[0m
COLOR_BOLD := \033[1m
COLOR_GREEN := \033[32m
COLOR_YELLOW := \033[33m
COLOR_BLUE := \033[34m

.PHONY: help
help: ## Show this help message
	@echo "$(COLOR_BOLD)RDFSolve Notebook Generation Makefile$(COLOR_RESET)"
	@echo ""
	@echo "$(COLOR_BLUE)Usage:$(COLOR_RESET)"
	@echo "  make <target> [DATASET=<name>] [NOTEBOOK_TYPE=<type>]"
	@echo ""
	@echo "$(COLOR_BLUE)Targets:$(COLOR_RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(COLOR_GREEN)%-30s$(COLOR_RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(COLOR_BLUE)Examples:$(COLOR_RESET)"
	@echo "  make all                          # Generate all notebooks for all datasets"
	@echo "  make schema DATASET=aopwikirdf    # Generate schema notebook for one dataset"
	@echo "  make clean                        # Clean generated files"
	@echo "  make install-deps                 # Install Python dependencies"

.PHONY: all
all: install-deps setup ## Run complete notebook generation pipeline for DATASET or all datasets
	@if [ -n "$(DATASET)" ]; then \
		echo "$(COLOR_BLUE)Running pipeline for single dataset: $(DATASET)$(COLOR_RESET)"; \
		$(MAKE) schema DATASET=$(DATASET); \
		$(MAKE) pydantic DATASET=$(DATASET); \
		$(MAKE) namespace DATASET=$(DATASET); \
	else \
		echo "$(COLOR_BLUE)Running pipeline for all datasets$(COLOR_RESET)"; \
		$(MAKE) schema-all; \
		$(MAKE) pydantic-all; \
		$(MAKE) namespace-all; \
	fi
	@$(MAKE) collect

.PHONY: setup
setup: ## Create necessary directories
	@echo "$(COLOR_BLUE)Creating output directories...$(COLOR_RESET)"
	@mkdir -p $(SCHEMA_DOCS_DIR)
	@mkdir -p $(PYDANTIC_DOCS_DIR)
	@mkdir -p $(NAMESPACE_DOCS_DIR)
	@mkdir -p $(DOCS_DATA_DIR)
	@mkdir -p $(SCHEMA_NB_DIR)
	@mkdir -p $(PYDANTIC_NB_DIR)
	@mkdir -p $(NAMESPACE_NB_DIR)
	@echo "$(COLOR_GREEN)✓ Directories created$(COLOR_RESET)"

.PHONY: venv
venv: ## Create virtual environment with uv (Python 3.10+)
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "$(COLOR_GREEN)✓ Virtual environment already exists at $(VENV_DIR)$(COLOR_RESET)"; \
	else \
		echo "$(COLOR_BLUE)Creating virtual environment with uv...$(COLOR_RESET)"; \
		if command -v uv >/dev/null 2>&1; then \
			uv venv $(VENV_DIR) --python 3.10; \
			echo "$(COLOR_GREEN)✓ Virtual environment created at $(VENV_DIR)$(COLOR_RESET)"; \
		else \
			echo "$(COLOR_YELLOW)Error: uv not found. Please install it first:$(COLOR_RESET)"; \
			echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"; \
			exit 1; \
		fi; \
	fi

.PHONY: check-venv
check-venv: venv ## Ensure virtual environment exists and check Python version
	@if [ ! -f "$(PYTHON)" ]; then \
		echo "$(COLOR_YELLOW)Virtual environment Python not found, recreating...$(COLOR_RESET)"; \
		rm -rf $(VENV_DIR); \
		$(MAKE) venv; \
	fi
	@python_version=$$($(PYTHON) -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "0.0"); \
	if [ "$$(echo "$$python_version >= 3.10" | bc -l)" -eq 0 ]; then \
		echo "$(COLOR_YELLOW)Error: Python $$python_version found in venv, but >= 3.10 required$(COLOR_RESET)"; \
		echo "Recreating virtual environment..."; \
		rm -rf $(VENV_DIR); \
		$(MAKE) venv; \
	else \
		echo "$(COLOR_GREEN)✓ Using Python $$python_version from $(VENV_DIR)$(COLOR_RESET)"; \
	fi

.PHONY: install-deps
install-deps: check-venv ## Install Python dependencies with uv
	@echo "$(COLOR_BLUE)Installing Python dependencies with uv...$(COLOR_RESET)"
	@if command -v uv >/dev/null 2>&1; then \
		uv pip install -e .[notebooks]; \
		uv pip install jupyter nbconvert pandas; \
	else \
		echo "$(COLOR_YELLOW)uv not found, using pip...$(COLOR_RESET)"; \
		$(PYTHON) -m pip install --upgrade pip; \
		$(PIP) install -e .[notebooks]; \
		$(PIP) install jupyter nbconvert pandas; \
	fi
	@echo "$(COLOR_GREEN)✓ Dependencies installed$(COLOR_RESET)"

.PHONY: install-system-deps
install-system-deps: ## Install system dependencies (requires sudo)
	@echo "$(COLOR_BLUE)Installing system dependencies...$(COLOR_RESET)"
	sudo apt-get update
	sudo apt-get install -y pandoc
	@echo "$(COLOR_GREEN)✓ System dependencies installed$(COLOR_RESET)"

.PHONY: list-datasets
list-datasets: ## List all available datasets from sources.csv
	@echo "$(COLOR_BLUE)Available datasets:$(COLOR_RESET)"
	@tail -n +2 data/sources.csv | cut -d',' -f1 | grep -v '^$$' | sort

.PHONY: prepare-matrix
prepare-matrix: ## Generate dataset matrix (for CI)
	@echo "$(COLOR_BLUE)Generating dataset matrix...$(COLOR_RESET)"
	@datasets=$$(tail -n +2 data/sources.csv | cut -d',' -f1 | grep -v '^$$' | jq -R . | jq -s . | jq -c .); \
	echo "schema-matrix={\"dataset\":$$datasets}" >> $(GITHUB_OUTPUT); \
	echo "pydantic-matrix={\"dataset\":$$datasets}" >> $(GITHUB_OUTPUT); \
	echo "namespace-matrix={\"dataset\":$$datasets}" >> $(GITHUB_OUTPUT); \
	dataset_count=$$(echo $$datasets | jq length); \
	echo "Generated matrix with $$dataset_count datasets"; \
	echo "Notebook type requested: $(NOTEBOOK_TYPE)"

.PHONY: generate-schema-nb
generate-schema-nb: ## Generate schema notebook for DATASET
	@if [ -z "$(DATASET)" ]; then \
		echo "$(COLOR_YELLOW)Error: DATASET not specified$(COLOR_RESET)"; \
		echo "Usage: make generate-schema-nb DATASET=<name>"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Generating schema notebook for dataset: $(DATASET)$(COLOR_RESET)"
	cd $(NOTEBOOKS_DIR) && $(PYTHON) make_notebooks.py --dataset "$(DATASET)" --type schema
	@echo "$(COLOR_GREEN)✓ Schema notebook generated for $(DATASET)$(COLOR_RESET)"

.PHONY: execute-schema-nb
execute-schema-nb: setup ## Execute and convert schema notebook to HTML
	@if [ -z "$(DATASET)" ]; then \
		echo "$(COLOR_YELLOW)Error: DATASET not specified$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Executing schema notebook for $(DATASET)...$(COLOR_RESET)"
	@cd $(SCHEMA_NB_DIR) && \
	notebook="$(DATASET)_schema.ipynb" && \
	if $(JUPYTER) nbconvert \
		--execute \
		--to html \
		"$$notebook" \
		--output-dir ../../$(SCHEMA_DOCS_DIR) \
		--ExecutePreprocessor.kernel_name=python3 ; then \
		echo "SUCCESS=true" >> $(GITHUB_OUTPUT); \
		echo "$(COLOR_GREEN)✓ Successfully converted: $$notebook$(COLOR_RESET)"; \
		echo "**$(DATASET)**: Schema analysis completed successfully" >> $(GITHUB_STEP_SUMMARY); \
	else \
		echo "SUCCESS=false" >> $(GITHUB_OUTPUT); \
		echo "$(COLOR_YELLOW)⚠ Failed to convert: $$notebook$(COLOR_RESET)"; \
		echo "**$(DATASET)**: Schema analysis failed (timeout or error)" >> $(GITHUB_STEP_SUMMARY); \
		$(MAKE) create-schema-error-report; \
	fi

.PHONY: create-schema-error-report
create-schema-error-report: ## Create error report HTML for failed schema notebook
	@echo "$(COLOR_YELLOW)Creating error report for $(DATASET)...$(COLOR_RESET)"
	@cat > "$(SCHEMA_DOCS_DIR)/$(DATASET)_schema.html" << 'EOF'
	<!DOCTYPE html>
	<html>
	<head>
	    <title>Schema Analysis Failed - $(DATASET)</title>
	</head>
	<body>
	    <h1>Schema Analysis Failed</h1>
	    <h2>Dataset Information</h2>
	    <p><strong>Dataset:</strong> $(DATASET)</p>
	    <p><strong>Attempted:</strong> <script>document.write(new Date().toUTCString())</script></p>
	    <h2>Manual Execution</h2>
	    <pre>cd notebooks/01_schema_extraction
	python make_notebooks.py --dataset $(DATASET) --type schema
	jupyter nbconvert --execute --to html $(DATASET)_schema.ipynb</pre>
	</body>
	</html>
	EOF

.PHONY: schema
schema: install-deps setup generate-schema-nb execute-schema-nb ## Generate and execute schema notebook for DATASET

.PHONY: schema-all
schema-all: ## Generate and execute schema notebooks for all datasets
	@echo "$(COLOR_BLUE)Processing all datasets for schema notebooks...$(COLOR_RESET)"
	@for dataset in $$(tail -n +2 data/sources.csv | cut -d',' -f1 | grep -v '^$$'); do \
		echo "$(COLOR_BLUE)Processing: $$dataset$(COLOR_RESET)"; \
		$(MAKE) schema DATASET=$$dataset || echo "$(COLOR_YELLOW)⚠ Failed: $$dataset$(COLOR_RESET)"; \
	done

.PHONY: generate-pydantic-nb
generate-pydantic-nb: ## Generate pydantic notebook for DATASET
	@if [ -z "$(DATASET)" ]; then \
		echo "$(COLOR_YELLOW)Error: DATASET not specified$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Generating pydantic notebook for dataset: $(DATASET)$(COLOR_RESET)"
	cd $(NOTEBOOKS_DIR) && $(PYTHON) make_notebooks.py --dataset "$(DATASET)" --type pydantic
	@echo "$(COLOR_GREEN)✓ Pydantic notebook generated for $(DATASET)$(COLOR_RESET)"

.PHONY: execute-pydantic-nb
execute-pydantic-nb: setup ## Execute and convert pydantic notebook to HTML
	@if [ -z "$(DATASET)" ]; then \
		echo "$(COLOR_YELLOW)Error: DATASET not specified$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Executing pydantic notebook for $(DATASET)...$(COLOR_RESET)"
	@cd $(PYDANTIC_NB_DIR) && \
	notebook="$(DATASET)_pydantic.ipynb" && \
	if $(JUPYTER) nbconvert \
		--execute \
		--to html \
		"$$notebook" \
		--output-dir ../../$(PYDANTIC_DOCS_DIR) \
		--ExecutePreprocessor.kernel_name=python3 ; then \
		echo "SUCCESS=true" >> $(GITHUB_OUTPUT); \
		echo "$(COLOR_GREEN)✓ Successfully converted: $$notebook$(COLOR_RESET)"; \
		echo "**$(DATASET)**: Pydantic model generation completed successfully" >> $(GITHUB_STEP_SUMMARY); \
	else \
		echo "SUCCESS=false" >> $(GITHUB_OUTPUT); \
		echo "$(COLOR_YELLOW)⚠ Failed to convert: $$notebook$(COLOR_RESET)"; \
		echo "**$(DATASET)**: Pydantic model generation failed (timeout or error)" >> $(GITHUB_STEP_SUMMARY); \
		$(MAKE) create-pydantic-error-report; \
	fi

.PHONY: create-pydantic-error-report
create-pydantic-error-report: ## Create error report HTML for failed pydantic notebook
	@echo "$(COLOR_YELLOW)Creating error report for $(DATASET)...$(COLOR_RESET)"
	@cat > "$(PYDANTIC_DOCS_DIR)/$(DATASET)_pydantic.html" << 'EOF'
	<!DOCTYPE html>
	<html>
	<head>
	    <title>Pydantic Generation Failed - $(DATASET)</title>
	</head>
	<body>
	    <h1>Pydantic Generation Failed</h1>
	    <h2>Dataset Information</h2>
	    <p><strong>Dataset:</strong> $(DATASET)</p>
	    <p><strong>Attempted:</strong> <script>document.write(new Date().toUTCString())</script></p>
	    <h2>Manual Execution</h2>
	    <pre>cd notebooks
	python make_notebooks.py --dataset $(DATASET) --type pydantic
	cd 02_pydantic_models
	jupyter nbconvert --execute --to html $(DATASET)_pydantic.ipynb</pre>
	</body>
	</html>
	EOF

.PHONY: pydantic
pydantic: generate-pydantic-nb execute-pydantic-nb ## Generate and execute pydantic notebook for DATASET

.PHONY: pydantic-all
pydantic-all: ## Generate and execute pydantic notebooks for all datasets
	@echo "$(COLOR_BLUE)Processing all datasets for pydantic notebooks...$(COLOR_RESET)"
	@for dataset in $$(tail -n +2 data/sources.csv | cut -d',' -f1 | grep -v '^$$'); do \
		echo "$(COLOR_BLUE)Processing: $$dataset$(COLOR_RESET)"; \
		$(MAKE) pydantic DATASET=$$dataset || echo "$(COLOR_YELLOW)⚠ Failed: $$dataset$(COLOR_RESET)"; \
	done

.PHONY: generate-namespace-nb
generate-namespace-nb: ## Generate namespace notebook for DATASET
	@if [ -z "$(DATASET)" ]; then \
		echo "$(COLOR_YELLOW)Error: DATASET not specified$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Generating namespace notebook for dataset: $(DATASET)$(COLOR_RESET)"
	cd $(NOTEBOOKS_DIR) && $(PYTHON) make_notebooks.py --dataset "$(DATASET)" --type namespace
	@echo "$(COLOR_GREEN)✓ Namespace notebook generated for $(DATASET)$(COLOR_RESET)"

.PHONY: execute-namespace-nb
execute-namespace-nb: setup ## Execute and convert namespace notebook to HTML
	@if [ -z "$(DATASET)" ]; then \
		echo "$(COLOR_YELLOW)Error: DATASET not specified$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Executing namespace notebook for $(DATASET)...$(COLOR_RESET)"
	@cd $(NAMESPACE_NB_DIR) && \
	notebook="$(DATASET)_namespaces.ipynb" && \
	if $(JUPYTER) nbconvert \
		--execute \
		--to html \
		"$$notebook" \
		--output-dir ../../$(NAMESPACE_DOCS_DIR) \
		--ExecutePreprocessor.kernel_name=python3 ; then \
		echo "SUCCESS=true" >> $(GITHUB_OUTPUT); \
		echo "$(COLOR_GREEN)✓ Successfully converted: $$notebook$(COLOR_RESET)"; \
		echo "**$(DATASET)**: Namespace discovery completed successfully" >> $(GITHUB_STEP_SUMMARY); \
	else \
		echo "SUCCESS=false" >> $(GITHUB_OUTPUT); \
		echo "$(COLOR_YELLOW)⚠ Failed to convert: $$notebook$(COLOR_RESET)"; \
		echo "**$(DATASET)**: Namespace discovery failed (timeout or error)" >> $(GITHUB_STEP_SUMMARY); \
		$(MAKE) create-namespace-error-report; \
	fi

.PHONY: create-namespace-error-report
create-namespace-error-report: ## Create error report HTML for failed namespace notebook
	@echo "$(COLOR_YELLOW)Creating error report for $(DATASET)...$(COLOR_RESET)"
	@cat > "$(NAMESPACE_DOCS_DIR)/$(DATASET)_namespaces.html" << 'EOF'
	<!DOCTYPE html>
	<html>
	<head>
	    <title>Namespace Discovery Failed - $(DATASET)</title>
	</head>
	<body>
	    <h1>Namespace Discovery Failed</h1>
	    <h2>Dataset Information</h2>
	    <p><strong>Dataset:</strong> $(DATASET)</p>
	    <p><strong>Attempted:</strong> <script>document.write(new Date().toUTCString())</script></p>
	    <h2>Manual Execution</h2>
	    <pre>cd notebooks
	python make_notebooks.py --dataset $(DATASET) --type namespace
	cd 03_bioregistry_namespaces
	jupyter nbconvert --execute --to html $(DATASET)_namespaces.ipynb</pre>
	</body>
	</html>
	EOF

.PHONY: namespace
namespace: generate-namespace-nb execute-namespace-nb ## Generate and execute namespace notebook for DATASET

.PHONY: namespace-all
namespace-all: ## Generate and execute namespace notebooks for all datasets
	@echo "$(COLOR_BLUE)Processing all datasets for namespace notebooks...$(COLOR_RESET)"
	@for dataset in $$(tail -n +2 data/sources.csv | cut -d',' -f1 | grep -v '^$$'); do \
		echo "$(COLOR_BLUE)Processing: $$dataset$(COLOR_RESET)"; \
		$(MAKE) namespace DATASET=$$dataset || echo "$(COLOR_YELLOW)⚠ Failed: $$dataset$(COLOR_RESET)"; \
	done

.PHONY: collect
collect: ## Collect and organize all generated results
	@echo "$(COLOR_BLUE)Collecting and organizing results...$(COLOR_RESET)"
	@$(MAKE) setup
	@if [ -d "$(ARTIFACTS_DIR)" ] && [ -n "$$(ls -A $(ARTIFACTS_DIR)/ 2>/dev/null)" ]; then \
		echo "$(COLOR_BLUE)Collecting artifacts from matrix jobs...$(COLOR_RESET)"; \
		$(MAKE) collect-artifacts; \
	else \
		echo "$(COLOR_YELLOW)No artifacts directory found, skipping artifact collection$(COLOR_RESET)"; \
	fi
	@$(MAKE) count-results
	@$(MAKE) generate-results-json
	@echo "$(COLOR_GREEN)✓ Results collected$(COLOR_RESET)"

.PHONY: collect-artifacts
collect-artifacts: ## Collect artifacts from CI runs
	@echo "$(COLOR_BLUE)Copying artifacts...$(COLOR_RESET)"
	@# Schema notebooks and HTML
	@find $(ARTIFACTS_DIR)/ -name "*_schema.ipynb" -exec cp {} $(SCHEMA_NB_DIR)/ \; 2>/dev/null || true
	@find $(ARTIFACTS_DIR)/ -name "*_schema.html" -exec cp {} $(SCHEMA_DOCS_DIR)/ \; 2>/dev/null || true
	@# Pydantic notebooks and HTML
	@find $(ARTIFACTS_DIR)/ -name "*_pydantic.ipynb" -exec cp {} $(PYDANTIC_NB_DIR)/ \; 2>/dev/null || true
	@find $(ARTIFACTS_DIR)/ -name "*_pydantic.html" -exec cp {} $(PYDANTIC_DOCS_DIR)/ \; 2>/dev/null || true
	@# Namespace notebooks and HTML
	@find $(ARTIFACTS_DIR)/ -name "*_namespaces.ipynb" -exec cp {} $(NAMESPACE_NB_DIR)/ \; 2>/dev/null || true
	@find $(ARTIFACTS_DIR)/ -name "*_namespaces.html" -exec cp {} $(NAMESPACE_DOCS_DIR)/ \; 2>/dev/null || true
	@# Data files
	@find $(ARTIFACTS_DIR)/ -path "*/data/schema_extraction/*" -type f \
		\( -name "*.jsonld" -o -name "*.yaml" -o -name "*.csv" -o -name "*.ttl" -o -name "*.nq" -o -name "*.parquet" -o -name "*.json" -o -name "*.jsonl" \) \
		-exec bash -c 'file="$$1"; relative_path="$${file#*/data/schema_extraction/}"; dataset_name=$$(echo "$$relative_path" | cut -d/ -f1); filename=$$(basename "$$relative_path"); mkdir -p "$(DOCS_DATA_DIR)/$$dataset_name"; cp "$$file" "$(DOCS_DATA_DIR)/$$dataset_name/$$filename"' _ {} \; 2>/dev/null || true

.PHONY: count-results
count-results: ## Count generated files
	@echo "$(COLOR_BLUE)Final collection results:$(COLOR_RESET)"
	@schema_nb=$$(find $(SCHEMA_NB_DIR)/ -name "*_schema.ipynb" 2>/dev/null | wc -l); \
	schema_html=$$(find $(SCHEMA_DOCS_DIR)/ -name "*_schema.html" 2>/dev/null | wc -l); \
	pydantic_nb=$$(find $(PYDANTIC_NB_DIR)/ -name "*_pydantic.ipynb" 2>/dev/null | wc -l); \
	pydantic_html=$$(find $(PYDANTIC_DOCS_DIR)/ -name "*_pydantic.html" 2>/dev/null | wc -l); \
	namespace_nb=$$(find $(NAMESPACE_NB_DIR)/ -name "*_namespaces.ipynb" 2>/dev/null | wc -l); \
	namespace_html=$$(find $(NAMESPACE_DOCS_DIR)/ -name "*_namespaces.html" 2>/dev/null | wc -l); \
	data_files=$$(find $(DOCS_DATA_DIR)/ -type f 2>/dev/null | wc -l); \
	echo "  Schema notebooks: $$schema_nb"; \
	echo "  Schema HTML files: $$schema_html"; \
	echo "  Pydantic notebooks: $$pydantic_nb"; \
	echo "  Pydantic HTML files: $$pydantic_html"; \
	echo "  Namespace notebooks: $$namespace_nb"; \
	echo "  Namespace HTML files: $$namespace_html"; \
	echo "  Data files: $$data_files"

.PHONY: generate-results-json
generate-results-json: ## Generate results.json for web interface
	@echo "$(COLOR_BLUE)Generating results.json...$(COLOR_RESET)"
	@bash scripts/generate_results_json.sh

.PHONY: clean
clean: ## Remove generated notebooks and HTML files
	@echo "$(COLOR_YELLOW)Cleaning generated files...$(COLOR_RESET)"
	@rm -f $(SCHEMA_NB_DIR)/*_schema.ipynb
	@rm -f $(SCHEMA_DOCS_DIR)/*_schema.html
	@rm -f $(PYDANTIC_NB_DIR)/*_pydantic.ipynb
	@rm -f $(PYDANTIC_DOCS_DIR)/*_pydantic.html
	@rm -f $(NAMESPACE_NB_DIR)/*_namespaces.ipynb
	@rm -f $(NAMESPACE_DOCS_DIR)/*_namespaces.html
	@rm -f $(DOCS_DIR)/results.json
	@echo "$(COLOR_GREEN)✓ Cleaned$(COLOR_RESET)"

.PHONY: clean-all
clean-all: clean ## Remove all generated files including data
	@echo "$(COLOR_YELLOW)Cleaning all generated files including data...$(COLOR_RESET)"
	@rm -rf $(DOCS_DATA_DIR)/*
	@rm -rf $(ARTIFACTS_DIR)
	@echo "$(COLOR_GREEN)✓ All cleaned$(COLOR_RESET)"

.PHONY: test-one
test-one: ## Quick test with one dataset (usage: make test-one DATASET=aopwikirdf)
	@if [ -z "$(DATASET)" ]; then \
		DATASET=$$(tail -n +2 data/sources.csv | cut -d',' -f1 | grep -v '^$$' | head -1); \
		echo "$(COLOR_BLUE)No DATASET specified, using first dataset: $$DATASET$(COLOR_RESET)"; \
		$(MAKE) schema DATASET=$$DATASET; \
	else \
		$(MAKE) schema DATASET=$(DATASET); \
	fi
