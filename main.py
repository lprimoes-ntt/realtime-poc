from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import count
from typing import Any, AsyncGenerator, Protocol

import uvicorn
from confluent_kafka import Consumer
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from processor import (
    ProcessResult,
    process_microbatch,
    read_gold_summary,
    recompute_gold_from_silver,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(
            "Invalid integer for %s=%s. Using default=%s", name, value, default
        )
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning(
            "Invalid float for %s=%s. Using default=%s", name, value, default
        )
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    logger.warning(
        "Invalid boolean for %s=%s. Using default=%s", name, value, default
    )
    return default


def _parse_cors_origins() -> list[str]:
    raw_value = os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173",
    )
    origins = [origin.strip() for origin in raw_value.split(",") if origin.strip()]
    return origins or ["http://localhost:5173"]


def _default_cors_origin_regex() -> str:
    return os.getenv(
        "CORS_ALLOW_ORIGIN_REGEX",
        r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    )


@dataclass(slots=True)
class RuntimeConfig:
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"
    )
    topic_pattern: str = os.getenv("KAFKA_TOPIC_PATTERN", "^shorisql_.*")
    ui_consumer_group_id: str = os.getenv(
        "KAFKA_UI_CONSUMER_GROUP_ID", "shori-python-consumer-ui-1"
    )
    lakehouse_consumer_group_id: str = os.getenv(
        "KAFKA_LAKEHOUSE_CONSUMER_GROUP_ID", "shori-lakehouse-pipeline"
    )
    ui_auto_offset_reset: str = os.getenv("KAFKA_UI_AUTO_OFFSET_RESET", "latest")
    lakehouse_auto_offset_reset: str = os.getenv(
        "KAFKA_LAKEHOUSE_AUTO_OFFSET_RESET", "earliest"
    )
    ui_poll_timeout_sec: float = _env_float("UI_POLL_TIMEOUT_SEC", 0.02)
    lakehouse_poll_timeout_sec: float = _env_float("LAKEHOUSE_POLL_TIMEOUT_SEC", 0.02)
    consumer_unassigned_restart_sec: float = _env_float(
        "KAFKA_UNASSIGNED_RESTART_SEC", 5.0
    )
    consumer_restart_backoff_sec: float = _env_float(
        "KAFKA_CONSUMER_RESTART_BACKOFF_SEC", 2.0
    )
    batch_flush_interval_sec: float = _env_float("LAKEHOUSE_FLUSH_INTERVAL_SEC", 1.0)
    batch_max_size: int = _env_int("LAKEHOUSE_MAX_BATCH_SIZE", 20000)
    gold_refresh_interval_sec: float = _env_float("GOLD_REFRESH_INTERVAL_SEC", 10.0)
    sse_client_queue_size: int = _env_int("SSE_CLIENT_QUEUE_SIZE", 20000)
    sse_client_overflow_limit: int = _env_int("SSE_CLIENT_OVERFLOW_LIMIT", 10)
    sse_heartbeat_sec: float = _env_float("SSE_HEARTBEAT_SEC", 1.0)
    emit_raw_cdc_events: bool = _env_bool("SSE_EMIT_CDC_RAW", False)
    cdc_stats_interval_sec: float = _env_float("SSE_CDC_STATS_INTERVAL_SEC", 1.0)
    cors_allow_origins: list[str] = field(default_factory=_parse_cors_origins)
    cors_allow_origin_regex: str = field(default_factory=_default_cors_origin_regex)


@dataclass(slots=True)
class PipelineMetrics:
    events_received: int = 0
    events_dropped: int = 0
    batches_ok: int = 0
    batches_failed: int = 0
    last_batch_at: float | None = None
    last_error: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def increment_events_received(self) -> None:
        with self._lock:
            self.events_received += 1

    def increment_events_dropped(self, amount: int = 1) -> None:
        with self._lock:
            self.events_dropped += amount

    def record_batch_success(self) -> None:
        with self._lock:
            self.batches_ok += 1
            self.last_batch_at = time.time()
            self.last_error = None

    def record_batch_failure(self, message: str) -> None:
        with self._lock:
            self.batches_failed += 1
            self.last_batch_at = time.time()
            self.last_error = message

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "events_received": self.events_received,
                "events_dropped": self.events_dropped,
                "batches_ok": self.batches_ok,
                "batches_failed": self.batches_failed,
                "last_batch_at": _to_iso(self.last_batch_at),
                "last_error": self.last_error,
            }


@dataclass(slots=True)
class ClientStream:
    queue: asyncio.Queue[dict[str, Any]]
    overflow_count: int = 0


class SupportsCommit(Protocol):
    def commit(self, asynchronous: bool = True) -> Any: ...


CONFIG = RuntimeConfig()
METRICS = PipelineMetrics()

app = FastAPI(title="CDC Showcase API")
allow_credentials = "*" not in CONFIG.cors_allow_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=CONFIG.cors_allow_origins,
    allow_origin_regex=CONFIG.cors_allow_origin_regex,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

app_loop: asyncio.AbstractEventLoop | None = None
shutdown_signal = threading.Event()
lakehouse_running = threading.Event()

ui_consumer_thread: threading.Thread | None = None
lakehouse_thread: threading.Thread | None = None

sse_clients: dict[int, ClientStream] = {}
next_client_id = count(1)


def _to_iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _new_event(event_type: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        "data": data or {},
    }


def _broadcast_event(event: dict[str, Any]) -> None:
    disconnected_clients: list[int] = []

    for client_id, client in list(sse_clients.items()):
        delivered = False

        try:
            client.queue.put_nowait(event)
            delivered = True
        except asyncio.QueueFull:
            METRICS.increment_events_dropped()

            try:
                client.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

            try:
                client.queue.put_nowait(event)
                delivered = True
            except asyncio.QueueFull:
                METRICS.increment_events_dropped()

        if delivered:
            client.overflow_count = 0
            continue

        client.overflow_count += 1
        if client.overflow_count >= CONFIG.sse_client_overflow_limit:
            disconnected_clients.append(client_id)

    for client_id in disconnected_clients:
        sse_clients.pop(client_id, None)
        logger.warning(
            "Disconnected SSE client %s after %s queue overflows.",
            client_id,
            CONFIG.sse_client_overflow_limit,
        )


def publish_event(event: dict[str, Any]) -> None:
    if app_loop is None or not app_loop.is_running():
        return
    app_loop.call_soon_threadsafe(_broadcast_event, event)


def register_client() -> tuple[int, asyncio.Queue[dict[str, Any]]]:
    client_id = next(next_client_id)
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
        maxsize=CONFIG.sse_client_queue_size
    )
    sse_clients[client_id] = ClientStream(queue=queue)

    initial_summary = read_gold_summary()
    if initial_summary is not None:
        try:
            queue.put_nowait(
                _new_event(
                    "lakehouse_update",
                    {
                        "summary": initial_summary,
                        "processed": 0,
                        "failed": 0,
                        "gold_recomputed": False,
                    },
                )
            )
        except asyncio.QueueFull:
            METRICS.increment_events_dropped()

    logger.info(
        "SSE client %s connected (active clients=%s)", client_id, len(sse_clients)
    )
    return client_id, queue


def unregister_client(client_id: int) -> None:
    if sse_clients.pop(client_id, None) is not None:
        logger.info(
            "SSE client %s disconnected (active clients=%s)",
            client_id,
            len(sse_clients),
        )


def process_and_commit_batch(
    consumer: SupportsCommit,
    batch: list[dict[str, Any]],
    refresh_gold: bool,
) -> ProcessResult:
    result = process_microbatch(batch, refresh_gold=refresh_gold)
    if result.success:
        consumer.commit(asynchronous=False)
    return result


def _next_gold_refresh(now: float) -> float:
    interval = max(CONFIG.gold_refresh_interval_sec, 0.0)
    return now + interval


def _parse_message_payload(raw_value: bytes) -> dict[str, Any] | None:
    try:
        decoded = raw_value.decode("utf-8")
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return None
        return payload
    except UnicodeDecodeError, json.JSONDecodeError:
        return None


def _sleep_with_shutdown(delay_sec: float) -> None:
    remaining = max(delay_sec, 0.0)
    while remaining > 0 and not shutdown_signal.is_set():
        step = min(remaining, 0.2)
        time.sleep(step)
        remaining -= step


def run_kafka_consumer() -> None:
    conf: dict[str, str | bool] = {
        "bootstrap.servers": CONFIG.kafka_bootstrap_servers,
        "group.id": CONFIG.ui_consumer_group_id,
        "auto.offset.reset": CONFIG.ui_auto_offset_reset,
        "enable.auto.commit": "true",
    }

    while not shutdown_signal.is_set():
        consumer: Consumer | None = None
        try:
            consumer = Consumer(conf)
            consumer.subscribe([CONFIG.topic_pattern])
            logger.info("UI consumer started. Pattern=%s", CONFIG.topic_pattern)
            unassigned_since = time.monotonic()
            stats_interval_start = time.monotonic()
            stats_events = 0

            while not shutdown_signal.is_set():
                assigned = consumer.assignment()
                if assigned:
                    unassigned_since = time.monotonic()
                elif (
                    time.monotonic() - unassigned_since
                    >= CONFIG.consumer_unassigned_restart_sec
                ):
                    logger.warning(
                        "UI consumer had no partition assignment for %.1fs; restarting consumer.",
                        CONFIG.consumer_unassigned_restart_sec,
                    )
                    break

                now = time.monotonic()
                elapsed = now - stats_interval_start
                if elapsed >= CONFIG.cdc_stats_interval_sec:
                    metrics_snapshot = METRICS.snapshot()
                    publish_event(
                        _new_event(
                            "cdc_stats",
                            {
                                "events_in_interval": stats_events,
                                "interval_sec": elapsed,
                                "events_per_sec": int(stats_events / elapsed) if elapsed > 0 else 0,
                                "total_received": metrics_snapshot["events_received"],
                                "total_dropped": metrics_snapshot["events_dropped"],
                            },
                        )
                    )
                    stats_events = 0
                    stats_interval_start = now

                msg = consumer.poll(timeout=CONFIG.ui_poll_timeout_sec)
                if msg is None:
                    continue

                error = msg.error()
                if error is not None:
                    if error.name() in {"_PARTITION_EOF", "UNKNOWN_TOPIC_OR_PART"}:
                        continue
                    logger.error("UI consumer Kafka error: %s", error)
                    continue

                raw_value = msg.value()
                if raw_value is None:
                    continue

                payload = _parse_message_payload(raw_value)
                if payload is None:
                    continue

                METRICS.increment_events_received()
                stats_events += 1

                if CONFIG.emit_raw_cdc_events:
                    publish_event(
                        _new_event(
                            "cdc_raw",
                            {
                                "topic": msg.topic() or "unknown_topic",
                                "partition": msg.partition(),
                                "offset": msg.offset(),
                                "payload": payload,
                            },
                        )
                    )

        except Exception as exc:
            logger.error("UI consumer error: %s", exc, exc_info=True)
            publish_event(
                _new_event(
                    "pipeline_error",
                    {
                        "message": f"UI consumer error; retrying: {exc}",
                        "fatal": False,
                    },
                )
            )
        finally:
            if consumer is not None:
                consumer.close()
            logger.info("UI consumer stopped.")

        if not shutdown_signal.is_set():
            _sleep_with_shutdown(CONFIG.consumer_restart_backoff_sec)


def run_lakehouse_pipeline() -> None:
    conf: dict[str, str | bool] = {
        "bootstrap.servers": CONFIG.kafka_bootstrap_servers,
        "group.id": CONFIG.lakehouse_consumer_group_id,
        "auto.offset.reset": CONFIG.lakehouse_auto_offset_reset,
        "enable.auto.commit": "false",
    }

    lakehouse_running.set()
    while not shutdown_signal.is_set() and lakehouse_running.is_set():
        consumer: Consumer | None = None
        batch: list[dict[str, Any]] = []
        last_flush = time.monotonic()
        next_gold_refresh_at = _next_gold_refresh(time.monotonic())
        gold_dirty = False

        try:
            consumer = Consumer(conf)
            consumer.subscribe([CONFIG.topic_pattern])
            logger.info("Lakehouse pipeline started. Pattern=%s", CONFIG.topic_pattern)
            unassigned_since = time.monotonic()

            while not shutdown_signal.is_set() and lakehouse_running.is_set():
                assigned = consumer.assignment()
                if assigned:
                    unassigned_since = time.monotonic()
                elif (
                    time.monotonic() - unassigned_since
                    >= CONFIG.consumer_unassigned_restart_sec
                ):
                    logger.warning(
                        "Lakehouse consumer had no partition assignment for %.1fs; restarting consumer.",
                        CONFIG.consumer_unassigned_restart_sec,
                    )
                    batch.clear()
                    break

                msg = consumer.poll(timeout=CONFIG.lakehouse_poll_timeout_sec)
                if msg is not None:
                    error = msg.error()
                    if error is None:
                        raw_value = msg.value()
                        if raw_value is not None:
                            payload = _parse_message_payload(raw_value)
                            if payload is not None:
                                batch.append(
                                    {
                                        "topic": msg.topic() or "unknown_topic",
                                        "partition": msg.partition(),
                                        "offset": msg.offset(),
                                        "payload": payload,
                                    }
                                )
                    elif error.name() not in {
                        "_PARTITION_EOF",
                        "UNKNOWN_TOPIC_OR_PART",
                    }:
                        logger.error("Lakehouse Kafka error: %s", error)

                now = time.monotonic()
                if gold_dirty and len(batch) == 0 and now >= next_gold_refresh_at:
                    try:
                        summary = recompute_gold_from_silver()
                    except Exception as exc:
                        error_message = f"Gold aggregation failed: {exc}"
                        METRICS.record_batch_failure(error_message)
                        publish_event(
                            _new_event(
                                "pipeline_error",
                                {
                                    "message": error_message,
                                    "fatal": True,
                                },
                            )
                        )
                        lakehouse_running.clear()
                        logger.error(
                            "Lakehouse pipeline stopped due to fatal error: %s",
                            error_message,
                        )
                        break

                    next_gold_refresh_at = _next_gold_refresh(now)
                    gold_dirty = False
                    if summary is not None:
                        publish_event(
                            _new_event(
                                "lakehouse_update",
                                {
                                    "summary": summary,
                                    "processed": 0,
                                    "failed": 0,
                                    "gold_recomputed": True,
                                },
                            )
                        )

                should_flush = len(batch) >= CONFIG.batch_max_size or (
                    len(batch) > 0
                    and (now - last_flush) >= CONFIG.batch_flush_interval_sec
                )
                if not should_flush:
                    continue

                refresh_gold = now >= next_gold_refresh_at
                result = process_and_commit_batch(
                    consumer, batch, refresh_gold=refresh_gold
                )

                if result.success:
                    METRICS.record_batch_success()
                    if result.gold_recomputed:
                        next_gold_refresh_at = _next_gold_refresh(now)
                        gold_dirty = False
                    elif result.processed > 0:
                        gold_dirty = True
                    if result.gold_summary is not None:
                        publish_event(
                            _new_event(
                                "lakehouse_update",
                                {
                                    "summary": result.gold_summary,
                                    "processed": result.processed,
                                    "failed": result.failed,
                                    "gold_recomputed": result.gold_recomputed,
                                },
                            )
                        )
                    batch.clear()
                    last_flush = time.monotonic()
                    continue

                error_message = result.error_message or "Unknown Lakehouse error"
                METRICS.record_batch_failure(error_message)
                publish_event(
                    _new_event(
                        "pipeline_error",
                        {
                            "message": error_message,
                            "fatal": True,
                        },
                    )
                )
                lakehouse_running.clear()
                logger.error(
                    "Lakehouse pipeline stopped due to fatal error: %s", error_message
                )
                break
        except Exception as exc:
            error_message = f"Lakehouse consumer error; retrying: {exc}"
            METRICS.record_batch_failure(error_message)
            publish_event(
                _new_event(
                    "pipeline_error",
                    {
                        "message": error_message,
                        "fatal": False,
                    },
                )
            )
            logger.error("Lakehouse consumer error: %s", exc, exc_info=True)
        finally:
            if consumer is not None:
                consumer.close()
            logger.info("Lakehouse pipeline stopped.")

        if not shutdown_signal.is_set() and lakehouse_running.is_set():
            _sleep_with_shutdown(CONFIG.consumer_restart_backoff_sec)


@app.on_event("startup")
async def startup_event() -> None:
    global app_loop, ui_consumer_thread, lakehouse_thread

    app_loop = asyncio.get_running_loop()
    shutdown_signal.clear()
    lakehouse_running.clear()

    try:
        recompute_gold_from_silver()
    except Exception as exc:
        logger.warning("Startup Gold refresh failed: %s", exc)

    ui_consumer_thread = threading.Thread(target=run_kafka_consumer, daemon=True)
    lakehouse_thread = threading.Thread(target=run_lakehouse_pipeline, daemon=True)

    ui_consumer_thread.start()
    lakehouse_thread.start()
    logger.info("FastAPI startup complete. Background threads launched.")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    shutdown_signal.set()
    lakehouse_running.clear()

    for thread in [ui_consumer_thread, lakehouse_thread]:
        if thread is not None:
            thread.join(timeout=3)

    logger.info("FastAPI shutdown complete.")


async def event_generator(
    request: Request,
    client_id: int,
    queue: asyncio.Queue[dict[str, Any]],
) -> AsyncGenerator[str, None]:
    try:
        while not shutdown_signal.is_set():
            if await request.is_disconnected():
                break

            if client_id not in sse_clients:
                break

            try:
                event = await asyncio.wait_for(
                    queue.get(), timeout=CONFIG.sse_heartbeat_sec
                )
            except asyncio.TimeoutError:
                heartbeat = _new_event("heartbeat")
                yield f"data: {json.dumps(heartbeat)}\n\n"
                continue

            yield f"data: {json.dumps(event)}\n\n"
    finally:
        unregister_client(client_id)


@app.get("/api/stream")
async def sse_endpoint(request: Request) -> StreamingResponse:
    client_id, queue = register_client()
    return StreamingResponse(
        event_generator(request, client_id, queue),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/health")
async def health_endpoint() -> dict[str, Any]:
    ui_alive = (
        ui_consumer_thread.is_alive() if ui_consumer_thread is not None else False
    )
    lakehouse_alive = (
        lakehouse_thread.is_alive() if lakehouse_thread is not None else False
    )

    status = "ok"
    if not lakehouse_running.is_set() and lakehouse_alive:
        status = "degraded"
    if METRICS.snapshot()["last_error"]:
        status = "degraded"

    return {
        "status": status,
        "threads": {
            "ui_consumer_alive": ui_alive,
            "lakehouse_alive": lakehouse_alive,
            "lakehouse_running": lakehouse_running.is_set(),
        },
        "clients": {"connected": len(sse_clients)},
        "metrics": METRICS.snapshot(),
        "config": {
            "emit_raw_cdc_events": CONFIG.emit_raw_cdc_events,
            "cdc_stats_interval_sec": CONFIG.cdc_stats_interval_sec,
            "batch_flush_interval_sec": CONFIG.batch_flush_interval_sec,
            "batch_max_size": CONFIG.batch_max_size,
            "gold_refresh_interval_sec": CONFIG.gold_refresh_interval_sec,
            "sse_client_queue_size": CONFIG.sse_client_queue_size,
            "sse_client_overflow_limit": CONFIG.sse_client_overflow_limit,
        },
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
