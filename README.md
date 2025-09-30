# Conduit

Conduit is the streaming service layer of Sentry's Real-Time Delivery Platform. It provides a scalable and multi-tenant way to delivery real-time data streams to clients.

## Overview

Conduit is built as a Rust workspace consisting of multiple services that work together to provide reliable, high-performance real-time data streaming:

- **Gateway Service**: Handles client connections and delivers real-time data streams via SSE
- **Publish Service**: Accepts data from upstream services and publishes it to the appropriate streams
- **Broker**: Shared library for Redis-based message brokering between services

## Architecture

```mermaid
graph LR
    P[Publisher] --> |POST /publish| PS[Publish<br/>Service]
    PS --> |XADD| R[(Redis<br/>Streams)]
    R --> |XREAD| GW[Gateway<br/>Service]
    C[Client<br/>Browser] -.-> |GET /events| GW
    GW ==> |SSE| C[Client<br/>Browser]

style P fill:#e1f5fe
style PS fill:#81c784
style R fill:#ff8a65
style GW fill:#81c784
style C fill:#e1f5fe
```

### Usage Examples

#### Publishing a Stream

This example shows how to publish a multi-part stream using the phase system:

```http
POST /publish/{org_id}/{channel_id}
Content-Type: application/x-protobuf

PublishRequest {
  channel_id: "97f58c19-ce38-4c6b-8a5b-47f3405691f0"
  message_id: "70a29bd8-e4b6-4875-a478-76ee24ff4ae9"
  phase: START
  sequence: 1
  payload: {}
}
```

```http
POST /publish/{org_id}/{channel_id}
Content-Type: application/x-protobuf

PublishRequest {
  channel_id: "97f58c19-ce38-4c6b-8a5b-47f3405691f0"
  message_id: "1a1b8d8b-68c5-4bbd-bb45-09b4bb01e2af"
  phase: DELTA
  sequence: 2
  payload: {
    "value": "hello",
  }
}
```

```http
POST /publish/{org_id}/{channel_id}
Content-Type: application/x-protobuf

PublishRequest {
  channel_id: "97f58c19-ce38-4c6b-8a5b-47f3405691f0"
  message_id: "8a5b915d-d434-4f82-a892-584ff9866654"
  phase: DELTA
  sequence: 3
  payload: {
    "value": ", world",
  }
}
```

```http
POST /publish/{org_id}/{channel_id}
Content-Type: application/x-protobuf

PublishRequest {
  channel_id: "97f58c19-ce38-4c6b-8a5b-47f3405691f0"
  message_id: "70a29bd8-e4b6-4875-a478-76ee24ff4ae9"
  phase: END
  sequence: 4
  payload: {}
}
```
