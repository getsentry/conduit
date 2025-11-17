#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR},component=gateway" \
  --image="us-central1-docker.pkg.dev/sentryio/conduit/conduit-gateway:${GO_REVISION_CONDUIT_REPO}" \
  --container-name="conduit" \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR},component=publish" \
  --image="us-central1-docker.pkg.dev/sentryio/conduit/conduit-publish:${GO_REVISION_CONDUIT_REPO}" \
  --container-name="conduit"
