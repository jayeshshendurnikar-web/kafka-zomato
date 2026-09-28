# Live delivery tracking

React + Vite rider/customer app, Express API, KafkaJS worker, MongoDB, Redis and Socket.IO. Node.js 22.12+, Docker and a reachable `MONGODB_URI` in `server/.env` are required.

## Run locally

From this directory:

```sh
npm install
npm run setup
npm run infra
npm run demo
```

Open **http://127.0.0.1:5173**. In the Customer tab, track `ORDER-1001`. Switch to Rider, select **Demo ride · New Delhi**, and start sharing for the same order. The customer connection stays active while switching tabs. You can also open separate browser windows or use **My device GPS**. GPS requires location permission and HTTPS or localhost; mobile browsers may suspend location sharing in the background.

`npm run demo` supplies local environment overrides without overwriting your existing `server/.env`. Compose uses Kafka port **19092** and Redis port **16379**. Ctrl+C stops all three app processes; `docker compose stop` stops infrastructure while preserving data.

For your own infrastructure, merge `server/.env.example` into `server/.env`, then run `npm run dev`. MongoDB uses your existing `MONGODB_URI`; Compose supplies Kafka and Redis. The Vite proxy targets API port 5000; change `client/vite.config.js` if you use another API port.

**Running from `server/`:** `npm run dev` now starts **both the API and consumer worker**, with watch mode. `npm run dev:api` and `npm run dev:worker` start individual processes when needed. Restart an older `npm run dev` terminal once to pick up this script change. Start React separately with `npm --prefix client run dev` from the repository root, or use root `npm run dev` to start all three processes.

For two riders, use two browser tabs with different order/rider IDs, e.g. `ORDER-1001` / `RIDER-01` and `ORDER-1002` / `RIDER-02`. Each customer tracks the matching order ID. Stop sharing before changing an active rider's order or ID; the form locks those fields while sending.

## MongoDB and activity logs

The worker reuses `connectDB`, `disconnectDB` and `getMongoClient`. It writes **one latest-location document per order** into `rider_locations` in the database selected by `MONGODB_URI`. The `mongo.ready` console event prints the database and collection. With the currently configured URI, the database is `test`, so inspect **`test.rider_locations`** in Compass. The connection string is not printed.

Each document contains `_id` (order ID), `orderId`, `riderId`, `latitude`, `longitude`, `timestamp` (GPS Unix milliseconds) and `persistedAt`. Timestamp-conditional writes prevent stale or duplicate events from overwriting newer Mongo data, including concurrent first inserts. This collection retains the latest point, not an unbounded GPS history. Previously committed Kafka events are not automatically replayed; pending messages and new updates are persisted.

The worker persists Mongo first, then updates Redis. Kafka is acknowledged only when both operations finish. A Mongo or Redis error triggers retry; a retry still performs the Redis step even when Mongo already contains the point. If Mongo is down, live processing waits for recovery. Pending messages older than five minutes still reach Mongo but are not republished as fresh live locations.

Server console events include `api.location.received`, `kafka.publish.accepted`, `kafka.consume.received`, `mongo.location.saved`, `redis.location.saved_and_published`, `websocket.location.sent`, snapshot reads, subscriptions, disconnects, ignored timestamps and errors. JSON records include `orderId`, rider/timestamp context and process ID. Browser DevTools logs rider sends, customer snapshots, received updates and rendered/ignored points. Set `ACTIVITY_LOGS=false` to suppress server informational activity logs; warnings/errors remain visible. No bearer tokens or database credentials are logged.

Worker instances maintain separate 15-second Redis readiness leases, refreshed every five seconds while consuming and Mongo is connected. If no healthy worker lease exists, new location POSTs return `503` and console logs explain that the worker must be started. Snapshot GETs can still serve the last cached location. Leases indicate recent worker availability, not zero Kafka lag.

## Flow

```text
React rider → POST API → Kafka (key = orderId) → consumer worker
                                                      ↓
                                      MongoDB latest per order
                                                      ↓
                                 Redis timestamp check + save + publish
                                                      ↓
                              subscribed WebSocket servers → React customer
```

- The rider sends one fresh location about every four seconds. The API validates coordinates/timestamps, checks order-scoped authorization, and uses a Redis-backed rate limiter shared across API processes. `202` means Kafka accepted the event; it does not mean the worker has already cached it.
- The existing `publishEvent` and `startConsumer` helpers are reused. Kafka config retains `connectKafka`, `disconnectKafka`, `ensureKafkaTopics`, `getKafkaProducer`, `createKafkaConsumer` and `stopKafkaConsumer` for existing callers. The producer uses `orderId` as the partition key.
- The worker processes each partition in order. After Mongo persistence, a Redis Lua script compares the timestamp, saves the latest point with a TTL and publishes it atomically. Older and equal timestamps do nothing. Persistence/cache failures propagate to the consumer instead of silently acknowledging a lost write. Invalid events are discarded; expired backlog reaches Mongo only.
- Each WebSocket process subscribes only to orders with connected local customers. A per-order queue serializes subscribe/unsubscribe races. Redis pub/sub carries the update to every server hosting customers for that order; each server broadcasts locally once.
- On initial connect and every reconnect, the customer installs its live listener, waits for a subscription ACK, **then** fetches the API snapshot from Redis. Snapshot and live messages share the same timestamp guard. A late snapshot cannot overwrite a newer live point. Failed snapshots retry; responses from earlier connections are ignored.
- Redis subscriber interruption closes client transports, triggering reconnect and snapshot recovery. Redis pub/sub itself does not retain messages.

## Structure

```text
client/src/components/   React forms, map and location display
client/src/hooks/        React lifecycle and subscription ownership
client/src/lib/          Reusable API, rider and customer tracking functions
server/api/              Express app, validation and rate limiter
server/producer/         Kafka producer and location publish service
server/consumer/         Kafka consumer runner, message handler, worker process and status
server/storage/          MongoDB (model, repository) and Redis (store, Lua scripts)
server/realtime/         Socket.IO ↔ per-order Redis subscriptions
server/config/           Database, Kafka, Redis and app configurations
server/auth/             Order/role-scoped signed tracking tokens
server/server.js         API + WebSocket process entrypoint
```

## API

`POST /api/orders/:orderId/location` with a rider bearer token:

```json
{
  "riderId": "RIDER-01",
  "latitude": 28.6304,
  "longitude": 77.2177,
  "timestamp": 1790574000000
}
```

Use the current Unix time in milliseconds, e.g. `Date.now()`; the example timestamp above is illustrative. The path supplies `orderId`. Responses: `202` accepted, `400` invalid input, `403` unauthorized, `429` rate limited with `Retry-After`, `503` unavailable.

`GET /api/orders/:orderId/location` with a customer bearer token returns `{ "location": {...} }` or `{ "location": null }`. Snapshot responses use `Cache-Control: no-store`. Health endpoints: `/health/live` and `/health/ready`. Readiness includes the API connection flags and recent worker availability; monitor Kafka lag separately.

Socket.IO connects with `{ auth: { orderId, token }, transports: ['websocket'] }`. Send `tracking:subscribe` with an acknowledgment; after `{ ok: true }`, fetch the snapshot. Listen for `location:update`. One connection belongs to one authorized order.

## Authorization and deployment

Demo mode is opt-in and rejected when `NODE_ENV=production`. For authenticated use, set `DEMO_MODE=false` and `TRACKING_TOKEN_SECRET` to a random secret of at least 32 characters. Create one-hour tokens for local testing:

```sh
cd server
npm run token -- customer ORDER-1001
npm run token -- rider ORDER-1001 RIDER-01
```

In a deployed app, the trusted order/login service should issue these scoped tokens after checking order membership and rider assignment. There is no public token-minting endpoint. Tokens expire; the current UI asks the user to connect with a fresh token. Use HTTPS/WSS and private, authenticated Kafka/Redis infrastructure.

Build React using `npm run build`; then Express serves `client/dist` on its own port. Start API and worker separately with `npm --prefix server start` and `npm --prefix server run worker` after provisioning topics with `npm --prefix server run topics`. Run them under a process supervisor.

Scale API/WebSocket processes behind a load balancer that supports WebSocket upgrades. WebSocket-only Socket.IO transport avoids polling stickiness. Scale workers using the same consumer group up to the topic's partition count (default six). The supplied Compose stack is a single-broker local setup; production needs replication and broker/Redis durability appropriate to your availability requirements.

The cache TTL defaults to 24 hours; incoming points older than five minutes or over 30 seconds in the future are rejected. TTL must exceed this accepted time window. Run Redis with persistence and `noeviction` for monotonic cache behavior while keys exist. A data-loss restore can lose cached timestamps; customer guards still protect already-rendered locations. Timestamps depend on rider device clocks, so keep clocks synchronized. Equal timestamps are intentionally treated as duplicates. The app retains only the latest point in Redis, and the UI keeps at most 100 trail points. Older Kafka backlog still needs to be consumed, so monitor lag rather than treating this as a history or ETA service.

The map uses Leaflet with OpenStreetMap tiles; fonts and map tiles require internet access. Tracking coordinates remain visible independently of tile loading.

## Verification

```sh
npm test
npm run build
KAFKA_BROKERS=localhost:19092 REDIS_URL=redis://127.0.0.1:16379 npm run test:integration
# With npm run demo running:
npx playwright install chromium
npm run test:browser
```

Tests cover Mongo failure propagation, idempotent retries after Redis failures, stale backlog handling, concurrent Mongo inserts, order isolation, two-rider Kafka/Mongo/Redis/WebSocket delivery, reconnect snapshots and worker availability. Integration tests create a unique Kafka topic and Mongo test collection and remove their own data. Browser tests use two rider/customer sessions with unique test order IDs, verify independent live updates, and check the active-form field locks.

Implementation references: [Socket.IO client lifecycle](https://socket.io/docs/v4/client-api/), [Redis atomic Lua execution](https://redis.io/docs/latest/develop/programmability/eval-intro/), [KafkaJS consumers](https://kafka.js.org/docs/consuming), [React effect cleanup](https://react.dev/reference/react/useEffect).
