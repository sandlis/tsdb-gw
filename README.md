# Grafana Gateways

Ingest and query metrics using [grafana.com](www.grafana.com) accounts/apikeys as authentication. Handles traffic toward [Cortex](https://github.com/weaveworks/cortex) and [Metrictank](https://github.com/grafana/metrictank).

# cortex-gw
  * Proxies requests towards cortex
  * Uses basic auth to verify requests.
  The username corresponds to the instance id of the desired cortex tenant, `username: <instance-id>`
  The password is either a [grafana.com](www.grafana.com) api_key or a key located in the file auth, `password: <api_key>`

# tsdb-gw
  * Uses basic auth to verify requests.
  The username defaults to `api_key`, `username: api_key`
  The password is either a [grafana.com](www.grafana.com) api_key or a key located in the file auth, `password: <api_key>`

## Ingest

1. Carbon
2. Prometheus Remote Write
3. OpenTSDB HTTP write
4. DataDog JSON