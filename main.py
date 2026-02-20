import asyncio
import json
import logging
import threading
from typing import Any, AsyncGenerator, Dict, Optional

import uvicorn
from confluent_kafka import Consumer
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# Setup simple logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CDC Showcase API")

# Allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global queue to bridge the sync Kafka consumer and async FastAPI
# We use an asyncio.Queue, but because we are putting into it from a sync thread,
# we need to use the thread-safe run_coroutine_threadsafe.
cdc_events_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=5000)
loop: Optional[asyncio.AbstractEventLoop] = None
consumer_thread_running = True


def run_kafka_consumer() -> None:
    """Runs a blocking Kafka consumer loop in a separate thread."""
    global loop, consumer_thread_running

    conf: dict[str, str | int | float | bool | None] = {
        "bootstrap.servers": "localhost:19092",
        "group.id": "shori-python-consumer-ui-1",  # Changed to force a new consumer group
        "auto.offset.reset": "latest",  # We only care about live events for the dashboard
        "enable.auto.commit": "true",
    }

    consumer: Consumer | None = None
    try:
        consumer = Consumer(conf)
        topics = ["^shorisql_.*"]
        consumer.subscribe(topics)
        logger.info(
            f"Consumer started in background thread. Subscribed to patterns: {topics}"
        )

        while consumer_thread_running:
            msg = consumer.poll(timeout=0.1)  # Faster polling
            if msg is None:
                continue

            error = msg.error()
            if error is not None:
                if error.name() == "_PARTITION_EOF":
                    continue
                elif error.name() == "UNKNOWN_TOPIC_OR_PART":
                    # Topics might not be created by Debezium yet
                    logger.debug(f"Waiting for topic creation: {error}")
                else:
                    logger.error(f"Kafka error: {error}")
                continue

            # We have a valid message
            topic = msg.topic() or "unknown_topic"
            raw_value = msg.value()

            if not raw_value:
                continue

            payload_str = raw_value.decode("utf-8")

            try:
                # Try to parse the Debezium JSON payload
                payload_json = json.loads(payload_str)

                # We package it nicely for the UI
                event_data = {
                    "topic": topic,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "payload": payload_json,
                }

                # Push to the async queue safely from this sync thread
                if loop and loop.is_running():
                    try:
                        asyncio.run_coroutine_threadsafe(
                            cdc_events_queue.put(event_data), loop
                        )
                    except asyncio.QueueFull:
                        logger.warning(
                            "Queue full, dropping event. Frontend might be disconnected or slow."
                        )

            except json.JSONDecodeError:
                logger.warning(
                    f"Could not parse message from {topic} as JSON: {payload_str[:50]}..."
                )

    except Exception as e:
        logger.error(f"Consumer thread crashed: {e}")
    finally:
        logger.info("Closing background consumer.")
        if consumer is not None:
            consumer.close()


@app.on_event("startup")
async def startup_event() -> None:
    """Start the background consumer thread when FastAPI boots."""
    global loop, consumer_thread_running
    loop = asyncio.get_running_loop()
    consumer_thread_running = True

    # Start the Kafka consumer in a daemon thread so it dies when the app dies
    thread = threading.Thread(target=run_kafka_consumer, daemon=True)
    thread.start()
    logger.info("FastAPI startup complete, background consumer launched.")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Signal the background thread to stop."""
    global consumer_thread_running
    consumer_thread_running = False
    logger.info("Shutting down...")


async def event_generator(request: Request) -> AsyncGenerator[str, None]:
    """Generates SSE events from the asyncio.Queue."""
    global consumer_thread_running
    while consumer_thread_running:
        if await request.is_disconnected():
            logger.info("Client disconnected from SSE stream.")
            break

        try:
            # Wait for an event, but timeout occasionally to check disconnects
            # We await the item directly from the queue.
            event_data = await asyncio.wait_for(cdc_events_queue.get(), timeout=1.0)

            # Format as Server-Sent Events (SSE) string
            # Format: "data: {json_string}\n\n"
            data_str = json.dumps(event_data)
            yield f"data: {data_str}\n\n"

        except asyncio.TimeoutError:
            # Just loop and check request.is_disconnected() again
            # We can optionally send a heartbeat ping
            yield ": ping\n\n"

        except Exception as e:
            logger.error(f"Error yielding event: {e}")
            break

    logger.info("Exiting SSE generator loop.")


@app.get("/api/stream")
async def sse_endpoint(request: Request) -> StreamingResponse:
    """SSE endpoint for the frontend to consume live CDC events."""
    return StreamingResponse(
        event_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Required to disable Nginx buffering if used
        },
    )


if __name__ == "__main__":
    # If run directly via python main.py
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
