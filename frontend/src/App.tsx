import { useEffect, useState, useRef, useMemo } from "react";
import {
  Card,
  Grid,
  Text,
  Metric,
  Flex,
  AreaChart,
  BarChart,
  BadgeDelta,
} from "@tremor/react";

interface CDCEvent {
  topic: string;
  partition: number;
  offset: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  payload: any;
}

interface LakehouseUpdate {
  type: "lakehouse_update";
  data: Record<string, Record<string, number>>;
}

type SSEEvent = CDCEvent | LakehouseUpdate;

interface ChartDataPoint {
  time: string;
  "Events/Sec": number;
}

export default function App() {
  const [isConnected, setIsConnected] = useState(false);
  const [totalEvents, setTotalEvents] = useState(0);
  const [currentThroughput, setCurrentThroughput] = useState(0);
  const [lakehouseData, setLakehouseData] = useState<Record<string, Record<string, number>>>({});

  // Chart data: Events per second over the last 60 seconds
  const [chartData, setChartData] = useState<ChartDataPoint[]>([]);

  // Refs for tracking throughput and smoothing
  const eventsInCurrentSecond = useRef(0);
  const eventQueue = useRef<CDCEvent[]>([]);

  useEffect(() => {
    // Connect to FastAPI SSE
    const eventSource = new EventSource("http://localhost:8000/api/stream");

    eventSource.onopen = () => {
      console.log("Connected to SSE");
      setIsConnected(true);
    };

    eventSource.onmessage = (event) => {
      // Ignore ping heartbeats
      if (event.data === "ping") return;

      try {
        const data: SSEEvent = JSON.parse(event.data);
        
        if ("type" in data && data.type === "lakehouse_update") {
          setLakehouseData(data.data);
        } else {
          // Push to buffer instead of processing immediately
          eventQueue.current.push(data as CDCEvent);
        }
      } catch (err) {
        console.error("Failed to parse SSE data", err);
      }
    };

    eventSource.onerror = (err) => {
      console.error("SSE Error", err);
      setIsConnected(false);
      eventSource.close();

      // Try to reconnect after 3 seconds
      setTimeout(() => {
        setIsConnected(true); // Forces re-render to trigger useEffect again
      }, 3000);
    };

    // --- Fast Processing Loop (Runs every 100ms) to drain the queue smoothly ---
    const processingIntervalId = setInterval(() => {
      const queue = eventQueue.current;
      if (queue.length === 0) return;

      // Take a chunk out of the queue (e.g. 10% of the queue, or at least 1)
      // This spreads massive bursts (e.g. 9000 events) across the whole second
      const chunkSize = Math.max(1, Math.ceil(queue.length / 10));
      const chunk = queue.splice(0, chunkSize);

      setTotalEvents((prev) => prev + chunk.length);
      eventsInCurrentSecond.current += chunk.length;
    }, 100);

    // --- Throughput Calculation Loop (Runs every 1s) ---
    const intervalId = setInterval(() => {
      const now = new Date();
      const timeStr = now.toLocaleTimeString([], { hour12: false });

      const rawEventsThisSec = eventsInCurrentSecond.current;
      eventsInCurrentSecond.current = 0; // Reset for next second

      setCurrentThroughput(rawEventsThisSec);

      setChartData((prev) => {
        const newData = [...prev, { time: timeStr, "Events/Sec": rawEventsThisSec }];
        
        // Keep last 60 seconds of data points
        if (newData.length > 60) newData.shift();
        return newData;
      });
    }, 1000);

    return () => {
      eventSource.close();
      clearInterval(intervalId);
      clearInterval(processingIntervalId);
    };
  }, [isConnected]);

  // Format Lakehouse data for Tremor BarChart
  const barChartData = useMemo(() => {
    // Collect all unique statuses
    const allStatuses = new Set<string>();
    Object.values(lakehouseData).forEach((dbData) => {
      Object.keys(dbData).forEach((status) => allStatuses.add(status));
    });

    const formattedData = Array.from(allStatuses).map((status) => {
      const row: any = { status };
      Object.keys(lakehouseData).forEach((db) => {
        row[db] = lakehouseData[db][status] || 0;
      });
      return row;
    });

    return formattedData;
  }, [lakehouseData]);

  const sourceDatabases = Object.keys(lakehouseData);

  return (
    <main className="p-4 md:p-10 mx-auto max-w-7xl h-screen">
      <Flex justifyContent="between" alignItems="center" className="mb-8">
        <div>
          <Metric className="text-white font-bold">Real-time Medallion Architecture</Metric>
          <Text className="text-gray-400">
            SQL Server ➔ Debezium ➔ Redpanda ➔ Polars ➔ Delta Lake ➔ React
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

      {/* Top Metrics */}
      <Grid numItemsSm={2} numItemsLg={3} className="gap-6 mb-6">
        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg">
          <Text className="text-gray-400">Total Raw Events Processed</Text>
          <Metric className="text-white">{totalEvents.toLocaleString()}</Metric>
        </Card>
        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg">
          <Text className="text-gray-400">Current Ingestion Throughput</Text>
          <Metric className="text-white">
            {currentThroughput}{" "}
            <span className="text-sm font-normal text-gray-500">msg/sec</span>
          </Metric>
        </Card>
        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg hidden lg:block">
          <Text className="text-gray-400">Target Pipeline</Text>
          <Metric className="text-white">dbo.Tickets ➔ Gold Layer</Metric>
        </Card>
      </Grid>

      {/* Lakehouse Gold Layer Chart */}
      <Card className="mb-6 bg-gray-800 border-gray-700 ring-0 shadow-lg">
        <Text className="text-gray-400 mb-4">
          Tickets by Status per Database (Gold Layer)
        </Text>
        {barChartData.length > 0 ? (
          <BarChart
            className="h-72 mt-4"
            data={barChartData}
            index="status"
            categories={sourceDatabases}
            colors={["blue", "emerald", "amber", "rose", "indigo", "cyan"]}
            valueFormatter={(number) => Intl.NumberFormat("us").format(number).toString()}
            stack={true}
            yAxisWidth={48}
          />
        ) : (
          <div className="h-72 mt-4 flex items-center justify-center text-gray-500">
            Waiting for Lakehouse pipeline to process first batch...
          </div>
        )}
      </Card>

      {/* Throughput Chart */}
      <Card className="mb-6 bg-gray-800 border-gray-700 ring-0 shadow-lg">
        <Text className="text-gray-400 mb-4">
          Raw Ingestion Pulse (Last 60s)
        </Text>
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
