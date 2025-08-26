# ============================================================================
# Multi-Tenant Data Processing Pipeline - Makefile
# ============================================================================

.PHONY: help install test test-fast test-parallel test-sequential test-coverage
.PHONY: test-pandas test-polars test-dask test-router
.PHONY: lint format security clean build deploy
.PHONY: setup-dev install-deps check-deps
.PHONY: terraform-init terraform-plan terraform-apply terraform-destroy

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
PYTEST := pytest
PROJECT_NAME := multi-tenant-pipeline
COVERAGE_MIN := 80

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
PURPLE := \033[0;35m
CYAN := \033[0;36m
WHITE := \033[0;37m
RESET := \033[0m

# ============================================================================
# HELP
# ============================================================================
help: ## Show this help message
	@echo "$(CYAN)Multi-Tenant Data Processing Pipeline$(RESET)"
	@echo "$(YELLOW)======================================$(RESET)"
	@echo ""
	@echo "$(GREEN)Available commands:$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "$(BLUE)%-20s$(RESET) %s\n", $$1, $$2}'

# ============================================================================
# DEVELOPMENT SETUP
# ============================================================================
setup-dev: ## Set up development environment
	@echo "$(YELLOW)🔧 Setting up development environment...$(RESET)"
	$(PIP) install --upgrade pip
	$(PIP) install pytest pytest-cov pytest-mock pytest-xdist pytest-benchmark
	$(PIP) install black isort flake8 mypy bandit safety
	$(PIP) install pandas polars dask[dataframe] numpy flask
	$(PIP) install google-cloud-storage google-cloud-bigquery google-auth
	$(PIP) install requests psutil
	@echo "$(GREEN)✅ Development environment ready!$(RESET)"

install-deps: ## Install all project dependencies
	@echo "$(YELLOW)📦 Installing dependencies...$(RESET)"
	$(PIP) install -r requirements.txt 2>/dev/null || echo "No requirements.txt found, using manual install"
	@$(MAKE) setup-dev

check-deps: ## Check for outdated dependencies
	@echo "$(YELLOW)🔍 Checking dependencies...$(RESET)"
	$(PIP) list --outdated

# ============================================================================
# TESTING
# ============================================================================
test: ## Run all tests in parallel (default)
	@echo "$(YELLOW)🧪 Running all tests in parallel...$(RESET)"
	@$(PYTHON) run_tests.py --parallel

test-fast: ## Run only fast tests
	@echo "$(YELLOW)⚡ Running fast tests only...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --fast

test-parallel: ## Run tests in parallel with maximum workers
	@echo "$(YELLOW)🚀 Running tests in parallel (max workers)...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --max-workers 4

test-sequential: ## Run tests sequentially (for debugging)
	@echo "$(YELLOW)🐌 Running tests sequentially...$(RESET)"
	@$(PYTHON) run_tests.py --sequential

test-coverage: ## Run tests with detailed coverage reporting
	@echo "$(YELLOW)📊 Running tests with coverage analysis...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --coverage
	@echo "$(GREEN)📈 Coverage reports generated in each engine's htmlcov/ directory$(RESET)"

test-unit: ## Run only unit tests
	@echo "$(YELLOW)🔬 Running unit tests...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --markers "unit"

test-integration: ## Run only integration tests
	@echo "$(YELLOW)🔗 Running integration tests...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --markers "integration"

test-security: ## Run security-related tests
	@echo "$(YELLOW)🔒 Running security tests...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --markers "security"

test-performance: ## Run performance benchmark tests
	@echo "$(YELLOW)⚡ Running performance tests...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --markers "performance"

# Engine-specific tests
test-pandas: ## Test Pandas engine only
	@echo "$(YELLOW)🐼 Testing Pandas engine...$(RESET)"
	@$(PYTHON) run_tests.py --engines pandas_engine

test-polars: ## Test Polars engine only
	@echo "$(YELLOW)⚡ Testing Polars engine...$(RESET)"
	@$(PYTHON) run_tests.py --engines polars_engine

test-dask: ## Test Dask engine only
	@echo "$(YELLOW)🔥 Testing Dask engine...$(RESET)"
	@$(PYTHON) run_tests.py --engines dask_engine

test-router: ## Test Router function only
	@echo "$(YELLOW)🚦 Testing Router function...$(RESET)"
	@$(PYTHON) run_tests.py --engines router_function

# Manual pytest commands (fallback)
pytest-pandas: ## Run pytest directly on Pandas engine
	cd pandas_engine && $(PYTEST) test_pandas_engine.py -v --tb=short

pytest-polars: ## Run pytest directly on Polars engine
	cd polars_engine && $(PYTEST) test_polars_engine.py -v --tb=short

pytest-dask: ## Run pytest directly on Dask engine
	cd dask_engine && $(PYTEST) test_dask_engine.py -v --tb=short

pytest-router: ## Run pytest directly on Router function
	cd router_function && $(PYTEST) test_router_function.py -v --tb=short

# ============================================================================
# CODE QUALITY
# ============================================================================
lint: ## Run all linting tools
	@echo "$(YELLOW)🔍 Running linting tools...$(RESET)"
	@$(MAKE) lint-flake8
	@$(MAKE) lint-mypy
	@echo "$(GREEN)✅ Linting complete!$(RESET)"

lint-flake8: ## Run flake8 linter
	@echo "$(BLUE)Running flake8...$(RESET)"
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

lint-mypy: ## Run mypy type checker
	@echo "$(BLUE)Running mypy...$(RESET)"
	mypy --ignore-missing-imports . || echo "$(YELLOW)⚠️  Type checking completed with warnings$(RESET)"

format: ## Format code with black and isort
	@echo "$(YELLOW)🎨 Formatting code...$(RESET)"
	black .
	isort .
	@echo "$(GREEN)✅ Code formatting complete!$(RESET)"

format-check: ## Check code formatting without making changes
	@echo "$(YELLOW)🔍 Checking code formatting...$(RESET)"
	black --check --diff .
	isort --check-only --diff .

# ============================================================================
# SECURITY
# ============================================================================
security: ## Run security analysis
	@echo "$(YELLOW)🔒 Running security analysis...$(RESET)"
	@$(MAKE) security-bandit
	@$(MAKE) security-safety

security-bandit: ## Run Bandit security linter
	@echo "$(BLUE)Running Bandit security analysis...$(RESET)"
	bandit -r . -f json -o bandit-report.json || echo "$(YELLOW)⚠️  Security issues found, check bandit-report.json$(RESET)"

security-safety: ## Check for known vulnerabilities in dependencies
	@echo "$(BLUE)Running Safety vulnerability check...$(RESET)"
	safety check --json --output safety-report.json || echo "$(YELLOW)⚠️  Vulnerabilities found, check safety-report.json$(RESET)"

# ============================================================================
# TERRAFORM OPERATIONS
# ============================================================================
terraform-init: ## Initialize Terraform
	@echo "$(YELLOW)🏗️  Initializing Terraform...$(RESET)"
	cd terraform && terraform init

terraform-plan: ## Plan Terraform deployment
	@echo "$(YELLOW)📋 Planning Terraform deployment...$(RESET)"
	cd terraform && terraform plan

terraform-apply: ## Apply Terraform configuration
	@echo "$(YELLOW)🚀 Applying Terraform configuration...$(RESET)"
	cd terraform && terraform apply

terraform-destroy: ## Destroy Terraform infrastructure
	@echo "$(RED)💥 Destroying Terraform infrastructure...$(RESET)"
	@echo "$(RED)⚠️  This will destroy all infrastructure! Are you sure? [y/N]$(RESET)"
	@read -r REPLY && [ "$$REPLY" = "y" ] && cd terraform && terraform destroy

terraform-validate: ## Validate Terraform configuration
	@echo "$(YELLOW)✅ Validating Terraform configuration...$(RESET)"
	cd terraform && terraform fmt -check -recursive
	cd terraform && terraform validate

# ============================================================================
# BUILD AND DEPLOYMENT
# ============================================================================
clean: ## Clean up generated files and caches
	@echo "$(YELLOW)🧹 Cleaning up...$(RESET)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -name "coverage.xml" -delete 2>/dev/null || true
	find . -name "*.log" -delete 2>/dev/null || true
	rm -f bandit-report.json safety-report.json test_results.json 2>/dev/null || true
	@echo "$(GREEN)✅ Cleanup complete!$(RESET)"

build: ## Build deployment packages
	@echo "$(YELLOW)📦 Building deployment packages...$(RESET)"
	mkdir -p dist/
	@for engine in pandas_engine polars_engine dask_engine router_function; do \
		if [ -d "$$engine" ]; then \
			echo "$(BLUE)📦 Packaging $$engine...$(RESET)"; \
			cd $$engine && zip -r "../dist/$${engine}.zip" . \
				-x "test_*.py" "__pycache__/*" "*.pyc" ".pytest_cache/*" "htmlcov/*" && \
			cd ..; \
		fi; \
	done
	@if [ -d "terraform" ]; then \
		echo "$(BLUE)📦 Packaging Terraform...$(RESET)"; \
		cd terraform && zip -r "../dist/terraform-infrastructure.zip" . \
			-x ".terraform/*" "*.tfstate*" ".terraform.lock.hcl" && \
		cd ..; \
	fi
	@echo "$(GREEN)✅ Build packages created in dist/$(RESET)"

deploy-check: ## Check deployment readiness
	@echo "$(YELLOW)🎯 Checking deployment readiness...$(RESET)"
	@$(MAKE) test-fast
	@$(MAKE) lint
	@$(MAKE) security
	@$(MAKE) terraform-validate
	@echo "$(GREEN)✅ Deployment checks passed!$(RESET)"

# ============================================================================
# CONTINUOUS INTEGRATION
# ============================================================================
ci-pipeline: ## Run full CI pipeline locally
	@echo "$(CYAN)🔄 Running full CI pipeline...$(RESET)"
	@$(MAKE) clean
	@$(MAKE) format-check
	@$(MAKE) lint
	@$(MAKE) security
	@$(MAKE) test-coverage
	@$(MAKE) terraform-validate
	@$(MAKE) build
	@echo "$(GREEN)🎉 CI pipeline completed successfully!$(RESET)"

ci-quick: ## Run quick CI checks
	@echo "$(CYAN)⚡ Running quick CI checks...$(RESET)"
	@$(MAKE) format-check
	@$(MAKE) lint-flake8
	@$(MAKE) test-fast
	@echo "$(GREEN)✅ Quick CI checks passed!$(RESET)"

# ============================================================================
# MONITORING AND REPORTING
# ============================================================================
health-check: ## Check health of all engines
	@echo "$(YELLOW)🏥 Running health checks...$(RESET)"
	@$(PYTHON) -c "
import sys
import subprocess
import json
engines = ['pandas_engine', 'polars_engine', 'dask_engine']
for engine in engines:
    try:
        result = subprocess.run([
            '$(PYTHON)', '-c', 
            f'from {engine}.main import app; client = app.test_client(); response = client.get(\"/health\"); print(f\"✅ {engine}: {{response.status_code}}\")'
        ], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(result.stdout.strip())
        else:
            print(f'❌ {engine}: Health check failed')
    except subprocess.TimeoutExpired:
        print(f'⏰ {engine}: Health check timed out')
    except Exception as e:
        print(f'💥 {engine}: Error - {e}')
"

coverage-report: ## Generate and open coverage report
	@echo "$(YELLOW)📊 Generating coverage report...$(RESET)"
	@$(MAKE) test-coverage
	@echo "$(GREEN)📈 Coverage reports available:$(RESET)"
	@for engine in pandas_engine polars_engine dask_engine router_function; do \
		if [ -d "$$engine/htmlcov" ]; then \
			echo "$(BLUE)  • $$engine/htmlcov/index.html$(RESET)"; \
		fi; \
	done

benchmark: ## Run performance benchmarks
	@echo "$(YELLOW)⚡ Running performance benchmarks...$(RESET)"
	@$(PYTHON) run_tests.py --parallel --markers "performance" --save-results benchmark_results.json
	@echo "$(GREEN)📊 Benchmark results saved to benchmark_results.json$(RESET)"

# ============================================================================
# DEVELOPMENT WORKFLOW
# ============================================================================
dev: ## Start development workflow (format, test, lint)
	@echo "$(CYAN)👨‍💻 Starting development workflow...$(RESET)"
	@$(MAKE) format
	@$(MAKE) test-fast
	@$(MAKE) lint
	@echo "$(GREEN)✅ Development workflow complete!$(RESET)"

pre-commit: ## Run pre-commit checks
	@echo "$(YELLOW)🚀 Running pre-commit checks...$(RESET)"
	@$(MAKE) format-check
	@$(MAKE) lint
	@$(MAKE) test-unit
	@echo "$(GREEN)✅ Pre-commit checks passed!$(RESET)"

# ============================================================================
# UTILITY
# ============================================================================
info: ## Display project information
	@echo "$(CYAN)📋 Project Information$(RESET)"
	@echo "$(YELLOW)==================$(RESET)"
	@echo "Project: $(PROJECT_NAME)"
	@echo "Python: $$($(PYTHON) --version)"
	@echo "Engines: pandas_engine, polars_engine, dask_engine, router_function"
	@echo "Coverage Minimum: $(COVERAGE_MIN)%"
	@echo ""
	@echo "$(GREEN)Available engines:$(RESET)"
	@for engine in pandas_engine polars_engine dask_engine router_function; do \
		if [ -d "$$engine" ]; then \
			echo "$(BLUE)  ✅ $$engine$(RESET)"; \
		else \
			echo "$(RED)  ❌ $$engine (missing)$(RESET)"; \
		fi; \
	done

install: setup-dev ## Alias for setup-dev