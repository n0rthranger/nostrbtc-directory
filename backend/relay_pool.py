"""Persistent relay connection pool for streaming subscriber events.

Instead of connecting/disconnecting to relays every 5 minutes per subscriber,
maintains long-lived WebSocket connections to popular relays and subscribes
to all subscriber pubkeys through them. Events stream in real-time.
"""

import asyncio
import json
import logging
import os
import time
from collections import OrderedDict

import websockets

import db
import discovery
import nostr_crypto

logger = logging.getLogger("nostrbtc.pool")

# Max pubkeys per REQ filter (some relays reject very large filters)
MAX_BATCH_SIZE = int(os.environ.get("RELAY_POOL_MAX_BATCH_SIZE", "150"))

# How often to refresh subscriber list and relay mappings
REBUILD_INTERVAL = int(os.environ.get("RELAY_POOL_REBUILD_INTERVAL", "300"))

# Relay cache TTL for discovery results
RELAY_CACHE_TTL = int(os.environ.get("RELAY_POOL_CACHE_TTL", "86400"))


class LRUSet:
    """Bounded set using OrderedDict for LRU eviction."""

    def __init__(self, maxsize: int = 50000):
        self._data = OrderedDict()
        self._maxsize = maxsize
        self._lock = asyncio.Lock()

    async def add(self, key) -> bool:
        """Add key. Returns True if new, False if already seen."""
        async with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
                return False
            if len(self._data) >= self._maxsize:
                self._data.popitem(last=False)
            self._data[key] = None
            return True

    def add_sync(self, key) -> bool:
        """Synchronous add — NOT thread-safe, only use from the same event loop thread."""
        if key in self._data:
            self._data.move_to_end(key)
            return False
        if len(self._data) >= self._maxsize:
            self._data.popitem(last=False)
        self._data[key] = None
        return True

    def __len__(self):
        return len(self._data)


class RelayConnection:
    """Manages a single persistent WebSocket connection to one relay."""

    def __init__(self, url: str, event_queue: asyncio.Queue):
        self.url = url
        self._event_queue = event_queue
        self._ws = None
        self._subscribed_pubkeys: set = set()
        self._sub_counter = 0
        self._active_sub_ids: list = []
        self._reconnect_delay = 1.0
        self._running = False
        self._seen = LRUSet(50000)
        self._task = None
        self._dropped_count = 0

    async def start(self):
        """Start the connection loop as a background task."""
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        """Main loop: connect, listen, reconnect on failure."""
        while self._running:
            try:
                self._ws = await asyncio.wait_for(
                    websockets.connect(self.url, open_timeout=10, close_timeout=5,
                                       ping_interval=30, ping_timeout=10),
                    timeout=15
                )
                self._reconnect_delay = 1.0
                logger.info(f"Pool: connected to {self.url}")

                # Re-subscribe if we had pubkeys
                if self._subscribed_pubkeys:
                    await self._send_subscriptions(self._subscribed_pubkeys)

                await self._listen()

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    logger.debug(f"Pool: {self.url} connection error: {e}")
            finally:
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 300)

    async def _listen(self):
        """Receive loop: parse messages and queue events."""
        async for raw in self._ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            if not isinstance(msg, list) or len(msg) < 2:
                continue

            if msg[0] == "EVENT" and len(msg) >= 3:
                event = msg[2]
                if not isinstance(event, dict):
                    continue
                event_id = event.get("id")
                if event_id and nostr_crypto.verify_event(event) and await self._seen.add(event_id):
                    try:
                        self._event_queue.put_nowait(event)
                    except asyncio.QueueFull:
                        self._dropped_count += 1
                        if self._dropped_count % 100 == 1:
                            logger.warning(f"Pool: queue full, {self._dropped_count} events dropped for {self.url}")

            elif msg[0] == "NOTICE":
                logger.debug(f"Pool: NOTICE from {self.url}: {msg[1] if len(msg) > 1 else ''}")

            elif msg[0] == "CLOSED" and len(msg) >= 3:
                reason = str(msg[2]) if len(msg) > 2 else ""
                logger.warning(f"Pool: sub closed by {self.url}: {reason}")

    async def update_subscriptions(self, pubkeys: set):
        """Update which pubkeys are subscribed on this connection."""
        self._subscribed_pubkeys = set(pubkeys)
        if self._ws and self._ws.state.name == "OPEN":
            # Close old subs
            for sub_id in self._active_sub_ids:
                try:
                    await self._ws.send(json.dumps(["CLOSE", sub_id]))
                except Exception:
                    pass
            self._active_sub_ids = []
            # Send new subs
            if pubkeys:
                await self._send_subscriptions(pubkeys)

    async def _send_subscriptions(self, pubkeys: set):
        """Send REQ messages in batches with delays to avoid overwhelming strfry."""
        pubkey_list = list(pubkeys)
        self._active_sub_ids = []

        for i in range(0, len(pubkey_list), MAX_BATCH_SIZE):
            batch = pubkey_list[i:i + MAX_BATCH_SIZE]
            self._sub_counter += 1
            sub_id = f"pool-{self._sub_counter}"
            self._active_sub_ids.append(sub_id)

            since = int(time.time()) - 600  # 10 min lookback on connect

            # Filter 1: events authored by subscribers
            author_filter = {
                "authors": batch,
                "since": since,
            }
            # Filter 2: reactions, zaps, reposts targeting subscribers
            # (kinds: 7=reaction, 9735=zap receipt, 6=repost)
            mention_filter = {
                "#p": batch,
                "kinds": [7, 9735, 6],
                "since": since,
            }
            try:
                await self._ws.send(json.dumps(["REQ", sub_id, author_filter, mention_filter]))
            except Exception as e:
                logger.debug(f"Pool: failed to send REQ to {self.url}: {e}")
                break
            # Small delay between batches to avoid "too many concurrent REQs"
            if i + MAX_BATCH_SIZE < len(pubkey_list):
                await asyncio.sleep(0.2)

    async def stop(self):
        """Stop the connection."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass


class StrfryPusher:
    """Consumes events from the queue and pushes to strfry in batches."""

    def __init__(self, event_queue: asyncio.Queue, batch_size: int = 100,
                 flush_interval: float = 2.0):
        self._queue = event_queue
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._seen = LRUSet(100000)
        self._task = None
        self._events_pushed = 0

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        """Main loop: drain queue, batch, push."""
        buffer = []
        last_flush = time.time()

        while True:
            try:
                # Wait for events with timeout
                try:
                    event = await asyncio.wait_for(
                        self._queue.get(), timeout=self._flush_interval
                    )
                    event_id = event.get("id")
                    if event_id and await self._seen.add(event_id):
                        buffer.append(event)
                except asyncio.TimeoutError:
                    pass

                # Flush if buffer is full or interval elapsed
                now = time.time()
                if buffer and (len(buffer) >= self._batch_size or
                               now - last_flush >= self._flush_interval):
                    await self._flush(buffer)
                    buffer = []
                    last_flush = now

            except asyncio.CancelledError:
                # Final flush
                if buffer:
                    await self._flush(buffer)
                break
            except Exception as e:
                logger.error(f"Pool pusher error: {e}")
                await asyncio.sleep(1)

    async def _flush(self, events: list):
        """Push a batch of events to strfry and update last_synced_at."""
        if not events:
            return

        try:
            async with websockets.connect(
                discovery.STRFRY_URL, open_timeout=5, close_timeout=5
            ) as ws:
                for event in events:
                    await ws.send(json.dumps(["EVENT", event]))
                    try:
                        await asyncio.wait_for(ws.recv(), timeout=1)
                    except asyncio.TimeoutError:
                        pass

            self._events_pushed += len(events)

            # Update last_synced_at per pubkey
            pubkey_max_ts = {}
            for ev in events:
                pk = ev.get("pubkey", "")
                ts = ev.get("created_at", 0)
                if pk and ts > pubkey_max_ts.get(pk, 0):
                    pubkey_max_ts[pk] = ts

            for pk, ts in pubkey_max_ts.items():
                try:
                    db.update_last_synced(pk, ts)
                except Exception:
                    pass

            if len(events) > 0:
                logger.info(f"Pool: pushed {len(events)} events to strfry "
                           f"({len(pubkey_max_ts)} pubkeys)")

        except Exception as e:
            logger.error(f"Pool: strfry push failed ({len(events)} events): {e}")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def total_pushed(self):
        return self._events_pushed


class RelayPool:
    """Orchestrator: manages persistent connections to all relevant relays."""

    def __init__(self):
        self._connections: dict[str, RelayConnection] = {}
        self._event_queue = asyncio.Queue(maxsize=50000)
        self._pusher = StrfryPusher(self._event_queue)
        self._relay_pubkeys: dict[str, set] = {}
        self._rebuild_lock = asyncio.Lock()
        self._running = False
        self._refresh_task = None

    async def start(self):
        """Start the pool: build connections and begin streaming."""
        self._running = True
        await self._pusher.start()
        await self.rebuild()
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        logger.info("Pool: started")

    async def _refresh_loop(self):
        """Periodically rebuild to pick up new/expired subscribers."""
        while self._running:
            await asyncio.sleep(REBUILD_INTERVAL)
            try:
                await self.rebuild()
            except Exception as e:
                logger.error(f"Pool: rebuild failed: {e}")

    async def rebuild(self):
        """Rebuild relay-pubkey mappings and update connections."""
        async with self._rebuild_lock:
            start = time.time()

            # Get active subscribers
            subscribers = db.get_active_subscribers()
            if not subscribers:
                # No subscribers — close all connections
                for conn in list(self._connections.values()):
                    await conn.stop()
                self._connections.clear()
                self._relay_pubkeys.clear()
                return

            pubkeys = [s["pubkey"] for s in subscribers]

            # Discover relays for all pubkeys (with caching)
            relay_map = {}  # pubkey -> [relay_urls]
            sem = asyncio.Semaphore(10)

            async def discover_cached(pk):
                cached = db.get_cached_relays(pk, RELAY_CACHE_TTL)
                if cached:
                    return pk, cached
                async with sem:
                    relays = await discovery.discover_relays(pk)
                    db.cache_relays(pk, relays)
                    return pk, relays

            results = await asyncio.gather(
                *[discover_cached(pk) for pk in pubkeys],
                return_exceptions=True
            )

            for result in results:
                if isinstance(result, Exception):
                    continue
                pk, relays = result
                relay_map[pk] = relays

            # Invert: relay_url -> set of pubkeys
            new_relay_pubkeys: dict[str, set] = {}
            for pk, relays in relay_map.items():
                for relay in relays:
                    # Skip internal strfry
                    if relay == discovery.STRFRY_URL:
                        continue
                    if relay not in new_relay_pubkeys:
                        new_relay_pubkeys[relay] = set()
                    new_relay_pubkeys[relay].add(pk)

            # Always include essential relays with all pubkeys
            all_pubkeys = set(pubkeys)
            for relay in discovery.ESSENTIAL_RELAYS:
                if relay not in new_relay_pubkeys:
                    new_relay_pubkeys[relay] = set()
                new_relay_pubkeys[relay].update(all_pubkeys)

            # Prune relays with very few subscribers (not essential)
            essential_set = set(discovery.ESSENTIAL_RELAYS)
            pruned = {}
            for relay, pks in new_relay_pubkeys.items():
                if relay in essential_set or len(pks) >= 2:
                    pruned[relay] = pks
            new_relay_pubkeys = pruned

            self._relay_pubkeys = new_relay_pubkeys

            # Close connections for relays no longer needed
            for url in list(self._connections.keys()):
                if url not in new_relay_pubkeys:
                    await self._connections[url].stop()
                    del self._connections[url]
                    logger.info(f"Pool: disconnected from {url}")

            # Create new connections or update existing ones (staggered to avoid REQ flood)
            for url, pks in new_relay_pubkeys.items():
                if url not in self._connections:
                    conn = RelayConnection(url, self._event_queue)
                    self._connections[url] = conn
                    await conn.start()
                await self._connections[url].update_subscriptions(pks)
                await asyncio.sleep(0.3)  # stagger REQs across relays

            elapsed = time.time() - start
            logger.info(
                f"Pool: rebuild complete — {len(self._connections)} relays, "
                f"{len(all_pubkeys)} subscribers, {elapsed:.1f}s"
            )

    async def add_subscriber(self, pubkey: str):
        """Immediately add a new subscriber to the pool."""
        async with self._rebuild_lock:
            try:
                relays = await discovery.discover_relays(pubkey)
                db.cache_relays(pubkey, relays)
            except Exception as e:
                logger.error(f"Pool: discovery failed for new sub {pubkey[:16]}: {e}")
                return

            essential_set = set(discovery.ESSENTIAL_RELAYS)

            for relay in set(relays) | essential_set:
                if relay == discovery.STRFRY_URL:
                    continue
                if relay not in self._relay_pubkeys:
                    self._relay_pubkeys[relay] = set()
                self._relay_pubkeys[relay].add(pubkey)

                if relay not in self._connections:
                    conn = RelayConnection(relay, self._event_queue)
                    self._connections[relay] = conn
                    await conn.start()

                await self._connections[relay].update_subscriptions(
                    self._relay_pubkeys[relay]
                )

            logger.info(f"Pool: added subscriber {pubkey[:16]}... to {len(relays)} relays")

    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        for conn in self._connections.values():
            await conn.stop()
        await self._pusher.stop()
        logger.info("Pool: stopped")

    def get_stats(self) -> dict:
        return {
            "connections": len(self._connections),
            "subscribers": len(set().union(*self._relay_pubkeys.values())) if self._relay_pubkeys else 0,
            "queue_depth": self._event_queue.qsize(),
            "events_pushed": self._pusher.total_pushed,
        }
