#!/bin/bash

checks-githubactions-checkruns \
  getsentry/conduit \
  "${GO_REVISION_CONDUIT_REPO}" \
  "build-gateway-prod-amd64" \
  "build-publish-prod-amd64" \
  "build-gateway-prod-arm64" \
  "build-publish-prod-arm64" \
  "Tests (ubuntu)" \
  --timeout-mins=10
