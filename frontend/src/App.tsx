import { useEffect, useState, useRef } from "react";
import {
  Card,
  Grid,
  Text,
  Metric,
  Flex,
  AreaChart,
  BadgeDelta,
} from "@tremor/react";

interface CDCEvent {
  topic: string;
  partition: number;
  offset: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  payload: any;
}

interface ChartDataPoint {
  time: string;
  "Events/Sec": number;
}

export default function App() {
  const [isConnected, setIsConnected] = useState(false);
  const [totalEvents, setTotalEvents] = useState(0);
  const [currentThroughput, setCurrentThroughput] = useState(0);

  // Keep the last X events for the terminal view
  const [recentEvents, setRecentEvents] = useState<CDCEvent[]>([]);
  const maxRecentEvents = 50;

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
        const data: CDCEvent = JSON.parse(event.data);
        // Push to buffer instead of processing immediately
        eventQueue.current.push(data);
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

      setRecentEvents((prev) => {
        // Reverse chunk so newest events are at the top
        const newEvents = [...chunk.reverse(), ...prev];
        return newEvents.slice(0, maxRecentEvents);
      });
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

  return (
    <main className="p-4 md:p-10 mx-auto max-w-7xl h-screen">
      <Flex justifyContent="between" alignItems="center" className="mb-8">
        <div>
          <Metric className="text-white font-bold">Real-time CDC Showcase</Metric>
          <Text className="text-gray-400">
            SQL Server ➔ Debezium ➔ Redpanda ➔ FastAPI ➔ React
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
          <Text className="text-gray-400">Total Events Processed</Text>
          <Metric className="text-white">{totalEvents.toLocaleString()}</Metric>
        </Card>
        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg">
          <Text className="text-gray-400">Current Throughput</Text>
          <Metric className="text-white">
            {currentThroughput}{" "}
            <span className="text-sm font-normal text-gray-500">msg/sec</span>
          </Metric>
        </Card>
        <Card className="bg-gray-800 border-gray-700 ring-0 shadow-lg hidden lg:block">
          <Text className="text-gray-400">Target Tables</Text>
          <Metric className="text-white">dbo.test_cdc</Metric>
        </Card>
      </Grid>

      {/* Main Chart */}
      <Card className="mb-6 bg-gray-800 border-gray-700 ring-0 shadow-lg">
        <Text className="text-gray-400 mb-4">
          Event Throughput Pulse (Last 60s)
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

      {/* Terminal View */}
      <Card className="bg-gray-900 border-gray-700 ring-0 shadow-lg p-0 overflow-hidden flex flex-col h-96">
        <div className="bg-gray-950 px-4 py-2 border-b border-gray-800 flex items-center gap-2">
          <div className="w-3 h-3 rounded-full bg-red-500"></div>
          <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
          <div className="w-3 h-3 rounded-full bg-green-500"></div>
          <Text className="ml-2 font-mono text-xs text-gray-500">
            Raw CDC Feed (Latest 50)
          </Text>
        </div>
        <div className="p-4 overflow-y-auto font-mono text-sm space-y-2">
          {recentEvents.length === 0 ? (
            <div className="text-gray-600 animate-pulse">
              Waiting for events...
            </div>
          ) : (
            recentEvents.map((ev, i) => (
              <div
                key={`${ev.topic}-${ev.offset}-${i}`}
                className="border-b border-gray-800 pb-2 flex"
              >
                <span className="text-blue-400 mr-2 flex-shrink-0">
                  [{ev.topic.split(".")[2] || ev.topic}]
                </span>
                <span className="text-emerald-400 mr-2 flex-shrink-0">
                  p:{ev.partition} o:{ev.offset}
                </span>
                <span className="text-gray-300 truncate">
                  {JSON.stringify(ev.payload).substring(0, 150)}
                  {JSON.stringify(ev.payload).length > 150 ? "..." : ""}
                </span>
              </div>
            ))
          )}
        </div>
      </Card>
    </main>
  );
}
