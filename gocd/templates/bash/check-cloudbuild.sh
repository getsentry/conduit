#!/bin/bash

checks-googlecloud-check-cloudbuild \
  sentryio \
  conduit \
  conduit-main-trigger \
  "${GO_REVISION_CONDUIT_REPO}" \
  main \
  --location=us-central1