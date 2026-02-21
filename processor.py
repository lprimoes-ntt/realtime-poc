import logging
from typing import List, Dict, Any
import json
import polars as pl
from deltalake import write_deltalake, DeltaTable

logger = logging.getLogger(__name__)

BRONZE_PATH = "data/datalake/bronze/tickets"
SILVER_PATH = "data/datalake/silver/tickets"
GOLD_PATH = "data/datalake/gold/tickets_summary"

def process_microbatch(events: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not events:
        return None
        
    try:
        # 1. BRONZE LAYER (Append raw JSON payloads)
        df_bronze = pl.DataFrame({"raw_payload": [json.dumps(e) for e in events]})
        write_deltalake(BRONZE_PATH, df_bronze.to_arrow(), mode="append")
        
        # 2. SILVER LAYER (Clean & Upsert)
        parsed_records = []
        for e in events:
            # e["payload"] is the full Debezium envelope
            envelope = e.get("payload", {})
            if not isinstance(envelope, dict):
                continue
                
            payload = envelope.get("payload", {})
            if not payload:
                continue

            op = payload.get("op")
            source = payload.get("source") or {}
            source_db = source.get("db", "unknown")
            ts_ms = payload.get("ts_ms", 0)
            table = source.get("table")
            
            # We are currently only building Gold aggregations for Tickets
            if table != "Tickets":
                continue
            
            # Debezium 'after' state for inserts/updates, 'before' for deletes
            record_state = payload.get("after") if op != "d" else payload.get("before")
            if not record_state:
                continue
                
            parsed_records.append({
                "id": record_state.get("id"),
                "project_id": record_state.get("project_id"),
                "reporter_id": record_state.get("reporter_id"),
                "status": record_state.get("status"),
                "priority": record_state.get("priority"),
                "source_db": source_db,
                "op": op,
                "ts_ms": ts_ms
            })
            
        if not parsed_records:
            return None
            
        df_silver_batch = pl.DataFrame(parsed_records)
        
        # Deduplicate to keep only the latest operation for each (id, source_db)
        df_silver_batch = df_silver_batch.sort("ts_ms", descending=True).unique(subset=["id", "source_db"], keep="first")
        
        # Upsert (Merge) into Silver Delta Table
        try:
            dt = DeltaTable(SILVER_PATH)
            # Table exists, perform merge
            (
                dt.merge(
                    source=df_silver_batch.to_arrow(),
                    predicate="target.id = source.id AND target.source_db = source.source_db",
                    source_alias="source",
                    target_alias="target",
                    merge_schema=True
                )
                .when_matched_update_all(predicate="source.op != 'd'")
                .when_matched_delete(predicate="source.op = 'd'")
                .when_not_matched_insert_all(predicate="source.op != 'd'")
                .execute()
            )
        except Exception as e:
            import os
            if not os.path.exists(SILVER_PATH):
                # If table doesn't exist, create it via overwrite
                df_initial = df_silver_batch.filter(pl.col("op") != "d")
                if not df_initial.is_empty():
                    write_deltalake(SILVER_PATH, df_initial.to_arrow(), mode="overwrite")
            else:
                logger.error(f"Silver merge failed on existing table: {e}", exc_info=True)
                # Ensure we don't destroy the data table due to schema evolution mismatches
                pass
            
        # 3. GOLD LAYER (Aggregate by source_db and status)
        try:
            df_silver = pl.scan_delta(SILVER_PATH).collect()
            
            df_gold = (
                df_silver
                .group_by(["source_db", "status"])
                .agg(pl.len().alias("count"))
            )
            
            write_deltalake(GOLD_PATH, df_gold.to_arrow(), mode="overwrite")
            
            # Convert Gold layer to a simple dict for the UI
            gold_summary = {}
            for row in df_gold.iter_rows(named=True):
                db = row["source_db"]
                status = row["status"]
                count = row["count"]
                if db not in gold_summary:
                    gold_summary[db] = {}
                gold_summary[db][status] = count
                
            return gold_summary
            
        except Exception as e:
            logger.error(f"Gold aggregation failed: {e}")
            return None
        
    except Exception as e:
        logger.error(f"Error processing lakehouse micro-batch: {e}", exc_info=True)
        return None
