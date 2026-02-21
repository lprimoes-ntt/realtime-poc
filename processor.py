from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)

BRONZE_PATH = "data/datalake/bronze/tickets"
SILVER_PATH = "data/datalake/silver/tickets"
GOLD_PATH = "data/datalake/gold/tickets_summary"


@dataclass(slots=True)
class ProcessResult:
    success: bool
    gold_summary: dict[str, dict[str, int]] | None
    processed: int
    failed: int
    error_message: str | None = None
    gold_recomputed: bool = False


def _table_exists(path: str) -> bool:
    return Path(path, "_delta_log").exists()


def _parse_ticket_record(event: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    envelope = event.get("payload")
    if not isinstance(envelope, dict):
        return None, True

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None, True

    source = payload.get("source")
    if not isinstance(source, dict):
        return None, True

    if source.get("table") != "Tickets":
        return None, False

    op = payload.get("op")
    if op not in {"c", "u", "d", "r"}:
        return None, True

    record_state = payload.get("after") if op != "d" else payload.get("before")
    if not isinstance(record_state, dict):
        return None, True

    ts_ms = payload.get("ts_ms")
    if not isinstance(ts_ms, int):
        ts_ms = 0

    return (
        {
            "id": record_state.get("id"),
            "project_id": record_state.get("project_id"),
            "reporter_id": record_state.get("reporter_id"),
            "status": record_state.get("status"),
            "priority": record_state.get("priority"),
            "source_db": source.get("db", "unknown"),
            "op": op,
            "ts_ms": ts_ms,
        },
        False,
    )


def _build_gold_summary(df_gold: pl.DataFrame) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for row in df_gold.iter_rows(named=True):
        db = row["source_db"]
        status = row["status"]
        count = row["count"]
        if not isinstance(db, str) or not isinstance(status, str) or not isinstance(
            count, int
        ):
            continue
        if db not in summary:
            summary[db] = {}
        summary[db][status] = count
    return summary


def read_gold_summary() -> dict[str, dict[str, int]] | None:
    if not _table_exists(GOLD_PATH):
        return None

    try:
        df_gold = pl.scan_delta(GOLD_PATH).collect()
    except Exception as exc:
        logger.warning("Failed reading Gold summary table: %s", exc)
        return None

    return _build_gold_summary(df_gold)


def recompute_gold_from_silver() -> dict[str, dict[str, int]] | None:
    if not _table_exists(SILVER_PATH):
        return None

    df_silver = pl.scan_delta(SILVER_PATH).collect()
    if df_silver.is_empty():
        empty_gold = pl.DataFrame(
            {
                "source_db": pl.Series([], dtype=pl.Utf8),
                "status": pl.Series([], dtype=pl.Utf8),
                "count": pl.Series([], dtype=pl.Int64),
            }
        )
        write_deltalake(GOLD_PATH, empty_gold.to_arrow(), mode="overwrite")
        return {}

    df_gold = df_silver.group_by(["source_db", "status"]).agg(pl.len().alias("count"))
    write_deltalake(GOLD_PATH, df_gold.to_arrow(), mode="overwrite")
    return _build_gold_summary(df_gold)


def process_microbatch(
    events: list[dict[str, Any]], refresh_gold: bool = True
) -> ProcessResult:
    if not events:
        return ProcessResult(
            success=True,
            gold_summary=None,
            processed=0,
            failed=0,
            gold_recomputed=False,
        )

    try:
        df_bronze = pl.DataFrame({"raw_payload": [json.dumps(event) for event in events]})
        write_deltalake(BRONZE_PATH, df_bronze.to_arrow(), mode="append")
    except Exception as exc:
        logger.error("Bronze write failed: %s", exc, exc_info=True)
        return ProcessResult(
            success=False,
            gold_summary=None,
            processed=0,
            failed=len(events),
            error_message=f"Bronze write failed: {exc}",
            gold_recomputed=False,
        )

    parsed_records: list[dict[str, Any]] = []
    failed_records = 0

    for event in events:
        parsed, failed = _parse_ticket_record(event)
        if failed:
            failed_records += 1
        if parsed is not None:
            parsed_records.append(parsed)

    if not parsed_records:
        return ProcessResult(
            success=True,
            gold_summary=None,
            processed=0,
            failed=failed_records,
            gold_recomputed=False,
        )

    df_silver_batch = pl.DataFrame(parsed_records)
    df_silver_batch = (
        df_silver_batch.sort("ts_ms", descending=True)
        .unique(subset=["id", "source_db"], keep="first")
        .sort(["source_db", "id"])
    )

    try:
        if _table_exists(SILVER_PATH):
            dt = DeltaTable(SILVER_PATH)
            (
                dt.merge(
                    source=df_silver_batch.to_arrow(),
                    predicate="target.id = source.id AND target.source_db = source.source_db",
                    source_alias="source",
                    target_alias="target",
                    merge_schema=True,
                )
                .when_matched_update_all(predicate="source.op != 'd'")
                .when_matched_delete(predicate="source.op = 'd'")
                .when_not_matched_insert_all(predicate="source.op != 'd'")
                .execute()
            )
        else:
            df_initial = df_silver_batch.filter(pl.col("op") != "d")
            if not df_initial.is_empty():
                write_deltalake(SILVER_PATH, df_initial.to_arrow(), mode="overwrite")
    except Exception as exc:
        logger.error("Silver merge failed: %s", exc, exc_info=True)
        return ProcessResult(
            success=False,
            gold_summary=None,
            processed=0,
            failed=len(events),
            error_message=f"Silver merge failed: {exc}",
            gold_recomputed=False,
        )

    if not refresh_gold:
        return ProcessResult(
            success=True,
            gold_summary=None,
            processed=df_silver_batch.height,
            failed=failed_records,
            gold_recomputed=False,
        )

    try:
        gold_summary = recompute_gold_from_silver()
    except Exception as exc:
        logger.error("Gold aggregation failed: %s", exc, exc_info=True)
        return ProcessResult(
            success=False,
            gold_summary=None,
            processed=0,
            failed=len(events),
            error_message=f"Gold aggregation failed: {exc}",
            gold_recomputed=False,
        )

    return ProcessResult(
        success=True,
        gold_summary=gold_summary,
        processed=df_silver_batch.height,
        failed=failed_records,
        gold_recomputed=True,
    )
