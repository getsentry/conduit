#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR},component=gateway" \
  --image="us-docker.pkg.dev/sentryio/conduit-mr/conduit-gateway:${GO_REVISION_CONDUIT_REPO}" \
  --container-name="conduit" \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR},component=publish" \
  --image="us-docker.pkg.dev/sentryio/conduit-mr/conduit-publish:${GO_REVISION_CONDUIT_REPO}" \
  --container-name="conduit" \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR},component=cleanup" \
  --image="us-docker.pkg.dev/sentryio/conduit-mr/conduit-cleanup:${GO_REVISION_CONDUIT_REPO}" \
  --container-name="conduit"
