import { useEffect, useMemo, useRef, useState } from "react";
import {
  AreaChart,
  BadgeDelta,
  Card,
  Flex,
  Grid,
  Metric,
  Text,
  BarChart,
} from "@tremor/react";

interface CDCEventPayload {
  topic: string;
  partition: number;
  offset: number;
  payload: Record<string, unknown>;
}

interface LakehousePayload {
  summary: Record<string, Record<string, number>>;
  processed: number;
  failed: number;
  gold_recomputed: boolean;
}

interface PipelineErrorPayload {
  message: string;
  fatal: boolean;
}

interface BaseSSEEvent {
  ts: string;
}

interface CDCStatsPayload {
  events_in_interval: number;
  interval_sec: number;
  events_per_sec: number;
  total_received: number;
  total_dropped: number;
}

interface CDCStatsEvent extends BaseSSEEvent {
  type: "cdc_stats";
  data: CDCStatsPayload;
}

interface CDCEvent extends BaseSSEEvent {
  type: "cdc_raw";
  data: CDCEventPayload;
}

interface LakehouseUpdateEvent extends BaseSSEEvent {
  type: "lakehouse_update";
  data: LakehousePayload;
}

interface PipelineErrorEvent extends BaseSSEEvent {
  type: "pipeline_error";
  data: PipelineErrorPayload;
}

interface HeartbeatEvent extends BaseSSEEvent {
  type: "heartbeat";
  data: Record<string, never>;
}

type SSEEvent = CDCEvent | LakehouseUpdateEvent | PipelineErrorEvent | HeartbeatEvent;
type ParsedSSEEvent = SSEEvent | CDCStatsEvent;

interface ChartDataPoint {
  time: string;
  "Events/Sec": number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseEvent(raw: string): ParsedSSEEvent | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!isRecord(parsed)) {
    return null;
  }

  const type = parsed.type;
  const ts = parsed.ts;
  const data = parsed.data;

  if (typeof type !== "string" || typeof ts !== "string" || !isRecord(data)) {
    return null;
  }

  if (type === "heartbeat") {
    return { type, ts, data: {} };
  }

  if (type === "pipeline_error") {
    const message = typeof data.message === "string" ? data.message : "Unknown pipeline error";
    const fatal = data.fatal === true;
    return {
      type,
      ts,
      data: { message, fatal },
    };
  }

  if (type === "lakehouse_update") {
    const summary = isRecord(data.summary) ? (data.summary as Record<string, Record<string, number>>) : {};
    const processed = typeof data.processed === "number" ? data.processed : 0;
    const failed = typeof data.failed === "number" ? data.failed : 0;
    const goldRecomputed = data.gold_recomputed === true;

    return {
      type,
      ts,
      data: {
        summary,
        processed,
        failed,
        gold_recomputed: goldRecomputed,
      },
    };
  }

  if (type === "cdc_raw") {
    if (typeof data.topic !== "string" || typeof data.partition !== "number" || typeof data.offset !== "number") {
      return null;
    }

    return {
      type,
      ts,
      data: {
        topic: data.topic,
        partition: data.partition,
        offset: data.offset,
        payload: isRecord(data.payload) ? data.payload : {},
      },
    };
  }

  if (type === "cdc_stats") {
    const eventsInInterval = typeof data.events_in_interval === "number" ? data.events_in_interval : 0;
    const intervalSec = typeof data.interval_sec === "number" ? data.interval_sec : 0;
    const eventsPerSec = typeof data.events_per_sec === "number" ? data.events_per_sec : 0;
    const totalReceived = typeof data.total_received === "number" ? data.total_received : 0;
    const totalDropped = typeof data.total_dropped === "number" ? data.total_dropped : 0;

    return {
      type,
      ts,
      data: {
        events_in_interval: eventsInInterval,
        interval_sec: intervalSec,
        events_per_sec: eventsPerSec,
        total_received: totalReceived,
        total_dropped: totalDropped,
      },
    };
  }

  return null;
}

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";
const MAX_LOCAL_QUEUE = Number(import.meta.env.VITE_MAX_LOCAL_EVENT_QUEUE ?? "5000");
const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 15000;

export default function App() {
  const [isConnected, setIsConnected] = useState(false);
  const [totalEvents, setTotalEvents] = useState(0);
  const [currentThroughput, setCurrentThroughput] = useState(0);
  const [droppedEvents, setDroppedEvents] = useState(0);
  const [pipelineError, setPipelineError] = useState<string | null>(null);
  const [lakehouseData, setLakehouseData] = useState<Record<string, Record<string, number>>>({});
  const [chartData, setChartData] = useState<ChartDataPoint[]>([]);

  const eventsInCurrentSecond = useRef(0);
  const eventQueue = useRef<CDCEvent[]>([]);
  const hasStatsFeed = useRef(false);
  const reconnectAttempt = useRef(0);
  const reconnectTimeoutId = useRef<number | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    let isActive = true;

    const connect = () => {
      if (!isActive) {
        return;
      }

      const eventSource = new EventSource(`${API_BASE_URL}/api/stream`);
      eventSourceRef.current = eventSource;

      eventSource.onopen = () => {
        reconnectAttempt.current = 0;
        setIsConnected(true);
      };

      eventSource.onmessage = (event) => {
        const parsed = parseEvent(event.data);
        if (!parsed) {
          return;
        }

        if (parsed.type === "heartbeat") {
          return;
        }

        if (parsed.type === "pipeline_error") {
          setPipelineError(parsed.data.message);
          return;
        }

        if (parsed.type === "lakehouse_update") {
          setPipelineError(null);
          setLakehouseData(parsed.data.summary);
          return;
        }

        if (parsed.type === "cdc_stats") {
          hasStatsFeed.current = true;
          const timestamp = new Date(parsed.ts);
          const timeLabel = timestamp.toLocaleTimeString([], { hour12: false });

          setTotalEvents(parsed.data.total_received);
          setCurrentThroughput(parsed.data.events_per_sec);
          setDroppedEvents(parsed.data.total_dropped);
          setChartData((previous) => {
            const next = [...previous, { time: timeLabel, "Events/Sec": parsed.data.events_per_sec }];
            if (next.length > 60) {
              next.shift();
            }
            return next;
          });
          return;
        }

        const queue = eventQueue.current;
        if (queue.length >= MAX_LOCAL_QUEUE) {
          queue.shift();
          setDroppedEvents((previous) => previous + 1);
        }
        queue.push(parsed);
      };

      eventSource.onerror = () => {
        setIsConnected(false);
        eventSource.close();

        if (eventSourceRef.current === eventSource) {
          eventSourceRef.current = null;
        }

        if (reconnectTimeoutId.current !== null) {
          window.clearTimeout(reconnectTimeoutId.current);
        }

        const attempt = reconnectAttempt.current;
        const delay = Math.min(
          BASE_RECONNECT_DELAY_MS * Math.pow(2, attempt),
          MAX_RECONNECT_DELAY_MS,
        );
        reconnectAttempt.current += 1;

        reconnectTimeoutId.current = window.setTimeout(() => {
          if (isActive) {
            connect();
          }
        }, delay);
      };
    };

    const processingIntervalId = window.setInterval(() => {
      if (hasStatsFeed.current) {
        return;
      }
      const queue = eventQueue.current;
      if (queue.length === 0) {
        return;
      }

      const chunkSize = Math.max(1, Math.ceil(queue.length / 10));
      const chunk = queue.splice(0, chunkSize);

      setTotalEvents((previous) => previous + chunk.length);
      eventsInCurrentSecond.current += chunk.length;
    }, 100);

    const throughputIntervalId = window.setInterval(() => {
      if (hasStatsFeed.current) {
        return;
      }
      const now = new Date();
      const timeLabel = now.toLocaleTimeString([], { hour12: false });

      const eventsThisSecond = eventsInCurrentSecond.current;
      eventsInCurrentSecond.current = 0;

      setCurrentThroughput(eventsThisSecond);
      setChartData((previous) => {
        const next = [...previous, { time: timeLabel, "Events/Sec": eventsThisSecond }];
        if (next.length > 60) {
          next.shift();
        }
        return next;
      });
    }, 1000);

    connect();

    return () => {
      isActive = false;

      if (reconnectTimeoutId.current !== null) {
        window.clearTimeout(reconnectTimeoutId.current);
      }

      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      window.clearInterval(processingIntervalId);
      window.clearInterval(throughputIntervalId);
    };
  }, []);

  const barChartData = useMemo(() => {
    const allStatuses = new Set<string>();
    Object.values(lakehouseData).forEach((dbData) => {
      Object.keys(dbData).forEach((status) => allStatuses.add(status));
    });

    const rows: Array<Record<string, string | number>> = [];
    allStatuses.forEach((status) => {
      const row: Record<string, string | number> = { status };
      Object.keys(lakehouseData).forEach((dbName) => {
        row[dbName] = lakehouseData[dbName][status] ?? 0;
      });
      rows.push(row);
    });

    return rows;
  }, [lakehouseData]);

  const sourceDatabases = Object.keys(lakehouseData);

  return (
    <main className="p-4 md:p-10 mx-auto max-w-7xl h-screen">
      <Flex justifyContent="between" alignItems="center" className="mb-8">
        <div>
          <Metric className="text-white font-bold">Real-time Medallion Architecture</Metric>
          <Text className="text-gray-400">
            SQL Server - Debezium - Redpanda - Polars - Delta Lake - React
          </Text>
        </div>
        <BadgeDelta
          deltaType={isConnected ? "increase" : "decrease"}
          isIncreasePositive={true}
          size="lg"
        >
          {isConnected ? "Connected & Streaming" : "Disconnected"}
        </BadgeDelta>
      </Flex>

      {pipelineError && (
        <Card className="mb-6 bg-rose-900/40 border-rose-700 ring-0 shadow-lg">
          <Text className="text-rose-200">Pipeline Error</Text>
          <Metric className="text-rose-100 text-base mt-2">{pipelineError}</Metric>
        </Card>
      )}

      <Grid numItemsSm={2} numItemsLg={4} className="gap-6 mb-6">
        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg">
          <Text className="text-gray-400">Total Raw Events Received</Text>
          <Metric className="text-white">{totalEvents.toLocaleString("en-US")}</Metric>
        </Card>

        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg">
          <Text className="text-gray-400">Current Ingestion Throughput</Text>
          <Metric className="text-white">
            {currentThroughput} <span className="text-sm font-normal text-gray-500">msg/sec</span>
          </Metric>
        </Card>

        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg">
          <Text className="text-gray-400">Dropped Events (Server SSE)</Text>
          <Metric className="text-white">{droppedEvents.toLocaleString("en-US")}</Metric>
        </Card>

        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg hidden lg:block">
          <Text className="text-gray-400">Target Pipeline</Text>
          <Metric className="text-white">dbo.Tickets - Gold Layer</Metric>
        </Card>
      </Grid>

      <Card className="mb-6 bg-gray-800 border-gray-700 ring-0 shadow-lg">
        <Text className="text-gray-400 mb-4">Tickets by Status per Database (Gold Layer)</Text>
        {barChartData.length > 0 ? (
          <BarChart
            className="h-72 mt-4"
            data={barChartData}
            index="status"
            categories={sourceDatabases}
            colors={["blue", "emerald", "amber", "rose", "indigo", "cyan"]}
            valueFormatter={(value) => Intl.NumberFormat("en-US").format(Number(value))}
            stack={true}
            yAxisWidth={48}
          />
        ) : (
          <div className="h-72 mt-4 flex items-center justify-center text-gray-500">
            Waiting for Lakehouse pipeline to process first batch...
          </div>
        )}
      </Card>

      <Card className="mb-6 bg-gray-800 border-gray-700 ring-0 shadow-lg">
        <Text className="text-gray-400 mb-4">Raw Ingestion Pulse (Last 60s)</Text>
        <AreaChart
          className="h-72 mt-4"
          data={chartData}
          index="time"
          categories={["Events/Sec"]}
          colors={["emerald"]}
          showLegend={false}
          showGridLines={false}
          yAxisWidth={40}
          curveType="monotone"
          animationDuration={300}
        />
      </Card>
    </main>
  );
}
