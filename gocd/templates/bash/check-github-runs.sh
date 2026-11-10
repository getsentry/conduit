#!/bin/bash

checks-githubactions-checkruns2 \
  getsentry/conduit \
  "${GO_REVISION_CONDUIT_REPO}" \
  "build-all-amd64" \
  "build-all-arm64" \
  "Tests (ubuntu)" \
  --timeout-mins=10
