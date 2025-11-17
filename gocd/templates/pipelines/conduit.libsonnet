local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

function(region) {
  environment_variables: {
    // SENTRY_REGION is used by the dev-infra scripts to connect to GKE
    SENTRY_REGION: region,
  },
  materials: {
    conduit_repo: {
      git: 'git@github.com:getsentry/conduit.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'main',
    },
  },
  lock_behavior: 'unlockWhenFinished',
  stages: [
    {
      checks: {
        fetch_materials: true,
        jobs: {
          checks: {
            timeout: 10,
            elastic_profile_id: 'conduit',
            environment_variables: {
              GITHUB_TOKEN: '{{SECRET:[devinfra-github][token]}}',
            },
            tasks: [
              gocdtasks.script(importstr '../bash/check-github-runs.sh'),
            ],
          },
        },
      },
    },
    {
      'deploy-primary': {
        fetch_materials: true,
        jobs: {
          deploy: {
            timeout: 20,
            elastic_profile_id: 'conduit',
            environment_variables: {
              LABEL_SELECTOR: 'service=conduit',
            },
            tasks: [
              gocdtasks.script(importstr '../bash/deploy.sh'),
            ],
          },
        },
      },
    },
  ],
}
