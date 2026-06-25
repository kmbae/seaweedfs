# SeaweedFS RDMA Sidecar Makefile

.PHONY: help build test clean docker-build docker-test docker-clean integration-test

# Default target
help: ## Show this help message
	@echo "SeaweedFS RDMA Sidecar - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make build           # Build all components locally"
	@echo "  make docker-test     # Run complete Docker integration tests"
	@echo "  make test            # Run unit tests"

# Local Build Targets
build: build-go build-rust ## Build all components locally

build-go: ## Build Go components (sidecar, demo-server, test-rdma, swvfs-rdma-daemon)
	@echo "🔨 Building Go components..."
	go build -o bin/sidecar ./cmd/sidecar
	go build -o bin/demo-server ./cmd/demo-server  
	go build -o bin/test-rdma ./cmd/test-rdma
	go build -o bin/swvfs-rdma-daemon ./cmd/swvfs-rdma-daemon
	@echo "✅ Go build complete"

build-rust: ## Build Rust RDMA engine
	@echo "🦀 Building Rust RDMA engine..."
	cd rdma-engine && cargo build --release
	@echo "✅ Rust build complete"

# Testing Targets
test: test-go test-rust ## Run all unit tests

test-go: ## Run Go tests
	@echo "🧪 Running Go tests..."
	go test ./...
	@echo "✅ Go tests complete"

test-rust: ## Run Rust tests
	@echo "🧪 Running Rust tests..."
	cd rdma-engine && cargo test
	@echo "✅ Rust tests complete"

integration-test: build ## Run local integration test
	@echo "🔗 Running local integration test..."
	./scripts/demo-e2e.sh
	@echo "✅ Local integration test complete"

# Docker Targets
docker-build: ## Build all Docker images
	@echo "🐳 Building Docker images..."
	docker-compose build
	@echo "✅ Docker images built"

docker-start: ## Start Docker services
	@echo "🚀 Starting Docker services..."
	./tests/docker-test-helper.sh start
	@echo "✅ Docker services started"

docker-test: ## Run Docker integration tests
	@echo "🧪 Running Docker integration tests..."
	./tests/docker-test-helper.sh test
	@echo "✅ Docker integration tests complete"

docker-stop: ## Stop Docker services
	@echo "🛑 Stopping Docker services..."
	./tests/docker-test-helper.sh stop
	@echo "✅ Docker services stopped"

docker-clean: ## Clean Docker services and volumes
	@echo "🧹 Cleaning Docker environment..."
	./tests/docker-test-helper.sh clean
	docker system prune -f
	@echo "✅ Docker cleanup complete"

docker-logs: ## Show Docker logs
	./tests/docker-test-helper.sh logs

docker-status: ## Show Docker service status
	./tests/docker-test-helper.sh status

docker-shell: ## Open interactive shell in test container
	./tests/docker-test-helper.sh shell

# RDMA Simulation Targets
rdma-sim-build: ## Build RDMA simulation environment
	@echo "🚀 Building RDMA simulation environment..."
	docker-compose -f docker-compose.rdma-sim.yml build
	@echo "✅ RDMA simulation images built"

rdma-sim-start: ## Start RDMA simulation environment
	@echo "🚀 Starting RDMA simulation environment..."
	docker-compose -f docker-compose.rdma-sim.yml up -d
	@echo "✅ RDMA simulation environment started"

rdma-sim-test: ## Run RDMA simulation tests
	@echo "🧪 Running RDMA simulation tests..."
	docker-compose -f docker-compose.rdma-sim.yml run --rm integration-tests-rdma
	@echo "✅ RDMA simulation tests complete"

rdma-sim-stop: ## Stop RDMA simulation environment
	@echo "🛑 Stopping RDMA simulation environment..."
	docker-compose -f docker-compose.rdma-sim.yml down
	@echo "✅ RDMA simulation environment stopped"

rdma-sim-clean: ## Clean RDMA simulation environment
	@echo "🧹 Cleaning RDMA simulation environment..."
	docker-compose -f docker-compose.rdma-sim.yml down -v --remove-orphans
	docker system prune -f
	@echo "✅ RDMA simulation cleanup complete"

rdma-sim-status: ## Check RDMA simulation status
	@echo "📊 RDMA simulation status:"
	docker-compose -f docker-compose.rdma-sim.yml ps
	@echo ""
	@echo "🔍 RDMA device status:"
	docker-compose -f docker-compose.rdma-sim.yml exec rdma-simulation /opt/rdma-sim/test-rdma.sh || true

rdma-sim-shell: ## Open shell in RDMA simulation container
	@echo "🐚 Opening RDMA simulation shell..."
	docker-compose -f docker-compose.rdma-sim.yml exec rdma-simulation /bin/bash

rdma-sim-logs: ## Show RDMA simulation logs
	docker-compose -f docker-compose.rdma-sim.yml logs

rdma-sim-ucx: ## Show UCX information in simulation
	@echo "📋 UCX information in simulation:"
	docker-compose -f docker-compose.rdma-sim.yml exec rdma-simulation /opt/rdma-sim/ucx-info.sh

# Development Targets
dev-setup: ## Set up development environment
	@echo "🛠️  Setting up development environment..."
	go mod tidy
	cd rdma-engine && cargo check
	chmod +x scripts/*.sh tests/*.sh
	@echo "✅ Development environment ready"

format: ## Format code
	@echo "✨ Formatting code..."
	go fmt ./...
	cd rdma-engine && cargo fmt
	@echo "✅ Code formatted"

lint: ## Run linters
	@echo "🔍 Running linters..."
	go vet ./...
	cd rdma-engine && cargo clippy -- -D warnings
	@echo "✅ Linting complete"

# Cleanup Targets
clean: clean-go clean-rust ## Clean all build artifacts

clean-go: ## Clean Go build artifacts
	@echo "🧹 Cleaning Go artifacts..."
	rm -rf bin/
	go clean -testcache
	@echo "✅ Go artifacts cleaned"

clean-rust: ## Clean Rust build artifacts  
	@echo "🧹 Cleaning Rust artifacts..."
	cd rdma-engine && cargo clean
	@echo "✅ Rust artifacts cleaned"

# Full Workflow Targets
check: format lint test ## Format, lint, and test everything

ci: check integration-test docker-test ## Complete CI workflow

demo: build ## Run local demo
	@echo "🎮 Starting local demo..."
	./scripts/demo-e2e.sh

# Docker Development Workflow
docker-dev: docker-clean docker-build docker-test ## Complete Docker development cycle

# Quick targets
quick-test: build ## Quick local test
	./bin/test-rdma --help

quick-docker: ## Quick Docker test
	docker-compose up -d rdma-engine rdma-sidecar
	sleep 5
	curl -s http://localhost:8081/health | jq '.'
	docker-compose down

# Help and Documentation
docs: ## Generate/update documentation
	@echo "📚 Documentation ready:"
	@echo "  README.md - Main project documentation"
	@echo "  DOCKER-TESTING.md - Docker integration testing guide"
	@echo "  Use 'make help' for available commands"

# Environment Info
info: ## Show environment information
	@echo "🔍 Environment Information:"
	@echo "  Go Version: $$(go version)"
	@echo "  Rust Version: $$(cd rdma-engine && cargo --version)"
	@echo "  Docker Version: $$(docker --version)"
	@echo "  Docker Compose Version: $$(docker-compose --version)"
	@echo ""
	@echo "🏗️  Project Structure:"
	@echo "  Go Components: cmd/ pkg/"
	@echo "  Rust Engine: rdma-engine/"  
	@echo "  Tests: tests/"
	@echo "  Scripts: scripts/"
