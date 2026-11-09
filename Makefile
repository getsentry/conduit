gocd: ## Build GoCD pipelines
	rm -rf ./gocd/generated-pipelines
	mkdir -p ./gocd/generated-pipelines
	cd ./gocd/templates && jb install && jb update
	# Format
	find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnetfmt -i
	# Lint
	find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnet-lint -J ./gocd/templates/vendor
	# Build
	cd ./gocd/templates && find . -type f \( -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnet --ext-code output-files=true -J vendor -m ../generated-pipelines
	# Convert JSON to yaml
	cd ./gocd/generated-pipelines && find . -type f \( -name '*.yaml' \) -print0 | xargs -n 1 -0 yq -p json -o yaml -i
.PHONY: gocd

check: ## Run every quality gate (CI runs this)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-features --tests -- -D clippy::all
	pre-commit run --all-files
.PHONY: check

fix: ## Run safe autofixes
	cargo fmt --all
	rumdl fmt .
	typos --write-changes
.PHONY: fix
