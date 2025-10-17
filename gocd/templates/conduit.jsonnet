local conduit = import './pipelines/conduit.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local pipedream_config = {
  name: 'conduit',
  auto_deploy: true,
  exclude_regions: [
    'us',
    'de',
    'customer-1',
    'customer-2',
    'customer-4',
    'customer-7',
  ],
  materials: {
    conduit_repo: {
      git: 'git@github.com:getsentry/conduit.git',
      shallow_clone: true,
      auto_update: true,
      branch: 'main',
      destination: 'conduit',
    },
  },
  rollback: {
    material_name: 'conduit_repo',
    stage: 'deploy-primary',
    elastic_profile_id: 'conduit',
  },
};

pipedream.render(pipedream_config, conduit)
