#!/bin/sh
# Selects which conduit service binary to run based on the first argument.
# Usage: entrypoint.sh <gateway|publish|cleanup> [args...]
set -eu

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <gateway|publish|cleanup> [args...]" >&2
    exit 1
fi

service="$1"
shift

case "$service" in
    gateway|publish|cleanup) ;;
    *)
        echo "unknown service: $service (expected gateway, publish or cleanup)" >&2
        exit 1
        ;;
esac

exec "/usr/local/bin/$service" "$@"
