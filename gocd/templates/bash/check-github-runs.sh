#!/bin/bash

checks-githubactions-checkruns \
  getsentry/conduit \
  "${GO_REVISION_GETSENTRY_REPO}" \
  "build-gateway-prod-amd64" \
  "build-publish-prod-amd64" \
  "build-gateway-prod-arm64" \
  "build-publish-prod-arm64" \
  "Tests (ubuntu)" \
  --skip-check="${SKIP_GITHUB_CHECKS}" \
  --timeout-mins=10
