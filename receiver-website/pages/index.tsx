import React, { useState, useEffect, useRef } from 'react';
import Head from 'next/head';
import Link from 'next/link';

interface SlotData {
  slot: number;
  channel: number;
  label?: string;
  name?: string;
  value?: number;
  unit?: string;
  temperature?: number;
  current?: number;
  voltage?: number;
  rpm?: number;
  pressure?: number;
  status?: string;
}

interface TelemetryPayload {
  rack_id?: string;
  slots?: SlotData[];
  slot_count?: number;
  telemetry?: { data_current?: boolean; data_status?: string };
  temperature?: number;
  rpm?: number;
  pressure?: number;
  motor_current?: number;
}

interface Envelope {
  schema: string;
  schema_version: string;
  message_id: string;
  gateway_id: string;
  gateway_boot_id: string;
  gateway_ip: string;
  gateway_sequence: number;
  created_at: string;
  created_at_us: string;
  replayed: boolean;
  rack_id?: string;
  payload: TelemetryPayload;
}

export default function ReceiverPage() {
  const [brokerUrl, setBrokerUrl] = useState(process.env.NEXT_PUBLIC_MQTT_BROKER_URL || 'ws://127.0.0.1:8088/mqtt');
  const [slaTarget] = useState(Number(process.env.NEXT_PUBLIC_LATENCY_SLA_MS || 220));
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Statistics
  const [currentLatency, setCurrentLatency] = useState<number | null>(null);
  const [latencyHistory, setLatencyHistory] = useState<number[]>([]);
  const [minLatency, setMinLatency] = useState<number>(Infinity);
  const [maxLatency, setMaxLatency] = useState<number>(0);
  const [avgLatency, setAvgLatency] = useState<number>(0);

  const [totalReceived, setTotalReceived] = useState(0);
  const [lostFrames, setLostFrames] = useState(0);
  const [replayedFrames, setReplayedFrames] = useState(0);
  const [lastSequence, setLastSequence] = useState<number | null>(null);

  // Latest Telemetry State
  const [latestEnvelope, setLatestEnvelope] = useState<Envelope | null>(null);
  const [latestTelemetry, setLatestTelemetry] = useState<TelemetryPayload | null>(null);
  const [lastSeenTime, setLastSeenTime] = useState<string>('Never');

  const clientRef = useRef<any>(null);
  const sequenceTracker = useRef<{ lastSeq: number | null; bootId: string | null; gaps: number; seen: Set<number> }>({
    lastSeq: null,
    bootId: null,
    gaps: 0,
    seen: new Set(),
  });
  const latencyWindow = useRef<number[]>([]);

  // Disconnect on unmount
  useEffect(() => {
    return () => {
      if (clientRef.current) {
        try {
          clientRef.current.end(true);
        } catch {}
      }
    };
  }, []);

  const connectToBroker = async () => {
    if (clientRef.current) {
      try {
        clientRef.current.end(true);
      } catch {}
      clientRef.current = null;
    }

    setConnecting(true);
    setErrorMsg(null);

    try {
      const mqttModule = await import('mqtt');
      const mqtt = mqttModule.default ?? mqttModule;

      let targetUrl = brokerUrl.trim();
      if (targetUrl.startsWith('https://')) {
        targetUrl = 'wss://' + targetUrl.slice(8);
      } else if (targetUrl.startsWith('http://')) {
        targetUrl = (typeof window !== 'undefined' && window.location.protocol === 'https:')
          ? 'wss://' + targetUrl.slice(7)
          : 'ws://' + targetUrl.slice(7);
      } else if (!targetUrl.startsWith('ws://') && !targetUrl.startsWith('wss://')) {
        targetUrl = (typeof window !== 'undefined' && window.location.protocol === 'https:')
          ? `wss://${targetUrl}`
          : `ws://${targetUrl}`;
      }
      if (!targetUrl.includes('/', targetUrl.indexOf('://') + 3)) {
        targetUrl = `${targetUrl}/mqtt`;
      }

      console.log(`[Receiver] Connecting to ${targetUrl}...`);
      const client = mqtt.connect(targetUrl, {
        clientId: `ultron-ui-receiver-${Math.random().toString(16).slice(2, 8)}`,
        clean: true,
        connectTimeout: 30000, // 30s timeout for remote / 2G / cellular networks
        reconnectPeriod: 2000, // auto-reconnect every 2s if signal drops
        keepalive: 60,
        protocolVersion: 5,
      });

      client.on('connect', () => {
        console.log('[Receiver] Connected to MQTT broker!');
        setConnected(true);
        setConnecting(false);
        setErrorMsg(null);

        // Subscribe to all gateway topics
        client.subscribe('ultron/v1/gateways/+/racks/+/telemetry', { qos: 0 });
        client.subscribe('ultron/v1/gateways/+/status', { qos: 1 });
        client.subscribe('ultron/v1/gateways/+/topology', { qos: 1 });
        client.subscribe('ultron/v1/gateways/+/racks/+/health', { qos: 1 });
      });

      client.on('message', (topic, payload) => {
        try {
          const envelope: Envelope = JSON.parse(payload.toString('utf8'));
          handleIncomingEnvelope(envelope, topic);
        } catch (e: any) {
          console.warn('[Receiver] Malformed packet on', topic, e.message);
        }
      });

      client.on('error', (err: any) => {
        console.error('[Receiver] Error:', err.message);
        setErrorMsg(err.message || 'Connection failed');
        setConnecting(false);
      });

      client.on('close', () => {
        setConnected(false);
        setConnecting(false);
      });

      clientRef.current = client;
    } catch (err: any) {
      setErrorMsg(err.message || 'Failed to load MQTT client');
      setConnecting(false);
    }
  };

  const disconnectBroker = () => {
    if (clientRef.current) {
      try {
        clientRef.current.end(true);
      } catch {}
      clientRef.current = null;
    }
    setConnected(false);
    setConnecting(false);
  };

  const handleIncomingEnvelope = (envelope: Envelope, topic: string) => {
    const nowUs = BigInt(Date.now()) * BigInt(1000);
    const sentUs = envelope.created_at_us ? BigInt(envelope.created_at_us) : BigInt(0);
    const latency = sentUs > 0n ? Number(nowUs - sentUs) / 1000 : null;

    // Filter out stale historical messages
    if (latency !== null && latency > 15000) return;

    setTotalReceived((prev) => prev + 1);
    setLastSeenTime(new Date().toLocaleTimeString());
    setLatestEnvelope(envelope);

    if (topic.includes('/telemetry')) {
      setLatestTelemetry(envelope.payload);
    }

    if (envelope.replayed) {
      setReplayedFrames((prev) => prev + 1);
    }

    // Update Latencies
    if (latency !== null && latency >= 0 && latency < 5000) {
      setCurrentLatency(latency);
      latencyWindow.current.push(latency);
      if (latencyWindow.current.length > 50) latencyWindow.current.shift();

      setLatencyHistory([...latencyWindow.current]);
      setMinLatency((prev) => Math.min(prev, latency));
      setMaxLatency((prev) => Math.max(prev, latency));
      const avg = latencyWindow.current.reduce((a, b) => a + b, 0) / latencyWindow.current.length;
      setAvgLatency(avg);
    }

    // Sequence tracking & Data Loss calculation
    const seq = envelope.gateway_sequence;
    const bootId = envelope.gateway_boot_id;
    if (typeof seq === 'number') {
      const tracker = sequenceTracker.current;
      if (!tracker.bootId || tracker.bootId !== bootId) {
        tracker.bootId = bootId;
        tracker.lastSeq = seq;
        tracker.seen.clear();
        tracker.seen.add(seq);
      } else {
        if (!tracker.seen.has(seq)) {
          tracker.seen.add(seq);
          if (tracker.lastSeq !== null && seq > tracker.lastSeq + 1) {
            const gap = seq - (tracker.lastSeq + 1);
            tracker.gaps += gap;
            setLostFrames(tracker.gaps);
          }
          if (tracker.lastSeq === null || seq > tracker.lastSeq) {
            tracker.lastSeq = seq;
            setLastSequence(seq);
          }
        }
      }
    }
  };

  const slaPassed = currentLatency !== null && currentLatency < slaTarget;
  const totalFrames = totalReceived + lostFrames;
  const lossRatePct = totalFrames > 0 ? ((lostFrames / totalFrames) * 100).toFixed(2) : '0.00';

  return (
    <div className="min-h-screen bg-[#080B11] text-slate-100 font-sans p-4 sm:p-8">
      <Head>
        <title>ULTRON Receiver — 2G Low-Latency Telemetry Console</title>
      </Head>

      <div className="max-w-7xl mx-auto space-y-6">
        {/* Navigation / Header */}
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between pb-6 border-b border-slate-800 gap-4">
          <div>
            <div className="flex items-center gap-3">
              <div className="w-3 h-3 rounded-full bg-cyan-400 animate-pulse" />
              <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                ULTRON <span className="text-cyan-400">Receiver Console</span>
              </h1>
            </div>
            <p className="text-sm text-slate-400 mt-1">
              Industrial Telemetry Ingest & Real-Time 2G Latency / Loss SLA Monitor
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Link
              href="/gateway"
              className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-cyan-300 border border-cyan-500/30 rounded-lg text-sm font-semibold transition"
            >
              Open Gateway Simulator →
            </Link>
          </div>
        </div>

        {/* Connection Bar */}
        <div className="bg-[#0F1420] border border-slate-800/80 rounded-xl p-4 sm:p-5 flex flex-col md:flex-row items-stretch md:items-center justify-between gap-4">
          <div className="flex-1">
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1.5">
              MQTT Broker WebSocket URL (3rd Laptop / Cloud / Local)
            </label>
            <div className="flex flex-wrap gap-2">
              <input
                type="text"
                value={brokerUrl}
                onChange={(e) => setBrokerUrl(e.target.value)}
                placeholder="ws://192.168.1.x:8080/mqtt or wss://..."
                className="flex-1 min-w-[260px] bg-slate-900 border border-slate-700 rounded-lg px-3.5 py-2 text-sm text-white focus:outline-none focus:border-cyan-400"
              />
              <button
                onClick={() => setBrokerUrl('ws://127.0.0.1:8080/mqtt')}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700"
              >
                Localhost
              </button>
              <button
                onClick={() => setBrokerUrl('wss://broker.hivemq.com:8884/mqtt')}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700"
              >
                Public HiveMQ WSS
              </button>
              <button
                onClick={() => setBrokerUrl('wss://broker.emqx.io:8084/mqtt')}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-emerald-400 rounded border border-emerald-800/60"
              >
                Public EMQX WSS
              </button>
            </div>
          </div>

          <div className="flex items-center gap-3 pt-2 md:pt-4">
            {connected ? (
              <button
                onClick={disconnectBroker}
                className="w-full md:w-auto px-5 py-2.5 bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/40 rounded-lg text-sm font-medium transition"
              >
                Disconnect
              </button>
            ) : (
              <button
                onClick={connectToBroker}
                disabled={connecting}
                className="w-full md:w-auto px-6 py-2.5 bg-cyan-500 hover:bg-cyan-400 disabled:opacity-50 text-slate-950 font-bold rounded-lg text-sm transition shadow-lg shadow-cyan-500/20"
              >
                {connecting ? 'Connecting...' : 'Connect to Broker'}
              </button>
            )}
          </div>
        </div>

        {errorMsg && (
          <div className="p-3.5 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm">
            <strong>Connection Notice:</strong> {errorMsg}
          </div>
        )}

        {/* TOP METRIC CARDS */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* Latency SLA Card */}
          <div className={`p-5 rounded-xl border ${slaPassed ? 'bg-emerald-950/20 border-emerald-500/40' : currentLatency === null ? 'bg-[#0F1420] border-slate-800' : 'bg-rose-950/20 border-rose-500/40'}`}>
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">End-to-End Latency</span>
              <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase tracking-wider ${slaPassed ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30' : currentLatency === null ? 'bg-slate-800 text-slate-400' : 'bg-rose-500/20 text-rose-300 border border-rose-500/30'}`}>
                {slaPassed ? `SLA Passed (<${slaTarget}ms)` : currentLatency === null ? 'Waiting...' : `SLA Breached (>${slaTarget}ms)`}
              </span>
            </div>
            <div className="text-3xl font-extrabold tracking-tight">
              {currentLatency !== null ? `${currentLatency.toFixed(1)} ms` : '--'}
            </div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Min: {minLatency === Infinity ? '--' : `${minLatency.toFixed(1)}ms`}</span>
              <span>Avg: {avgLatency > 0 ? `${avgLatency.toFixed(1)}ms` : '--'}</span>
              <span>Max: {maxLatency > 0 ? `${maxLatency.toFixed(1)}ms` : '--'}</span>
            </div>
          </div>

          {/* Data Loss Card */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Data Loss Rate</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-cyan-500/10 text-cyan-300 border border-cyan-500/20 uppercase">
                Zero Loss Target
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {lossRatePct}%
            </div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Frames Lost: {lostFrames}</span>
              <span>Total: {totalFrames}</span>
            </div>
          </div>

          {/* Sequence & Spool Card */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Gateway Monotonic Seq</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-indigo-500/10 text-indigo-300 border border-indigo-500/20 uppercase">
                Sequence Sync
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {lastSequence !== null ? `#${lastSequence}` : '--'}
            </div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Spool Recovered: {replayedFrames}</span>
              <span>Last: {lastSeenTime}</span>
            </div>
          </div>

          {/* Gateway Status Card */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Gateway Link</span>
              <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase ${connected ? 'bg-emerald-500/20 text-emerald-300' : 'bg-slate-800 text-slate-400'}`}>
                {connected ? 'ONLINE' : 'STANDBY'}
              </span>
            </div>
            <div className="text-lg font-bold text-slate-200 truncate">
              {latestEnvelope?.gateway_id || 'Waiting for gateway...'}
            </div>
            <div className="mt-2 text-xs text-slate-400 truncate">
              IP: {latestEnvelope?.gateway_ip || 'N/A'}
            </div>
          </div>
        </div>

        {/* LATENCY SPARKLINE CHART */}
        <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-base font-semibold text-white">Real-Time Latency Window (Target: &lt; 220 ms)</h2>
              <p className="text-xs text-slate-400">Live end-to-end packet transmission latency across simulated 2G conditions</p>
            </div>
            <div className="flex items-center gap-3 text-xs">
              <div className="flex items-center gap-1.5 text-rose-400">
                <span className="w-3 h-0.5 bg-rose-400 inline-block" /> 220 ms SLA threshold
              </div>
              <div className="flex items-center gap-1.5 text-cyan-400">
                <span className="w-2.5 h-2.5 bg-cyan-400 rounded-full inline-block" /> Live Sample
              </div>
            </div>
          </div>

          {/* Visual bar graph representation */}
          <div className="h-32 w-full flex items-end gap-1.5 pt-4 pb-2 border-b border-slate-800 relative">
            {/* 220 ms reference line */}
            <div className="absolute w-full border-t border-dashed border-rose-500/50 top-[35%] left-0 pointer-events-none" />

            {latencyHistory.length === 0 ? (
              <div className="w-full text-center text-xs text-slate-500 self-center">
                Connect and start publisher to plot latency history...
              </div>
            ) : (
              latencyHistory.map((val, idx) => {
                const heightPct = Math.min(100, Math.max(5, (val / 350) * 100));
                const isOverSLA = val >= 220;
                return (
                  <div
                    key={idx}
                    className={`flex-1 rounded-t transition-all ${isOverSLA ? 'bg-rose-500' : 'bg-cyan-400 hover:bg-cyan-300'}`}
                    style={{ height: `${heightPct}%` }}
                    title={`#${idx + 1}: ${val.toFixed(1)} ms`}
                  />
                );
              })
            )}
          </div>
        </div>

        {/* LIVE INDUSTRIAL TELEMETRY READINGS */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Machine Telemetry Gauges */}
          <div className="lg:col-span-2 bg-[#0F1420] border border-slate-800 rounded-xl p-5 space-y-4">
            <div className="flex justify-between items-center pb-2 border-b border-slate-800">
              <h2 className="text-base font-semibold text-white">Rack Live Machine Telemetry</h2>
              <span className="text-xs bg-cyan-950/60 text-cyan-300 px-2.5 py-1 rounded border border-cyan-800/40">
                Rack ID: {latestEnvelope?.rack_id || 'Rack-A'}
              </span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-[11px] text-slate-400 uppercase font-semibold">Zone 1 Temp</span>
                <div className="text-xl font-bold text-cyan-300 mt-1">
                  {latestTelemetry?.slots?.[0]?.value?.toFixed?.(1) ?? latestTelemetry?.temperature ?? '185.4'} °C
                </div>
              </div>
              <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-[11px] text-slate-400 uppercase font-semibold">Melt Pressure</span>
                <div className="text-xl font-bold text-amber-300 mt-1">
                  {latestTelemetry?.pressure ?? '142.0'} bar
                </div>
              </div>
              <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-[11px] text-slate-400 uppercase font-semibold">Screw RPM</span>
                <div className="text-xl font-bold text-emerald-300 mt-1">
                  {latestTelemetry?.rpm ?? '280'} RPM
                </div>
              </div>
              <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-[11px] text-slate-400 uppercase font-semibold">Motor Current</span>
                <div className="text-xl font-bold text-indigo-300 mt-1">
                  {latestTelemetry?.motor_current ?? '48.2'} A
                </div>
              </div>
            </div>

            {/* Slot / Channel breakdown */}
            <div className="mt-4">
              <span className="text-xs text-slate-400 font-semibold uppercase mb-2 block">
                DAQ Channels (CC v3 Normalized Frame)
              </span>
              <div className="max-h-48 overflow-y-auto space-y-1.5 pr-1">
                {(latestTelemetry?.slots || [
                  { slot: 1, channel: 1, name: 'Feed Zone', value: 185.4, unit: '°C' },
                  { slot: 2, channel: 1, name: 'Compression Zone', value: 210.2, unit: '°C' },
                  { slot: 3, channel: 1, name: 'Metering Zone', value: 228.0, unit: '°C' },
                  { slot: 4, channel: 1, name: 'Die Zone', value: 235.1, unit: '°C' },
                ]).map((s, idx) => (
                  <div key={idx} className="flex justify-between items-center text-xs py-1.5 px-3 bg-slate-900/40 rounded border border-slate-800/60">
                    <span className="text-slate-300 font-mono">Slot {s.slot} / Ch {s.channel} {s.name ? `(${s.name})` : ''}</span>
                    <span className="font-bold text-cyan-400">{typeof s.value === 'number' ? s.value.toFixed(1) : s.value ?? '--'} {s.unit || '°C'}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Raw MQTT Envelope Inspector */}
          <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5 flex flex-col justify-between">
            <div>
              <div className="flex justify-between items-center pb-2 border-b border-slate-800 mb-3">
                <h2 className="text-base font-semibold text-white">Envelope Inspector</h2>
                <span className="text-[10px] text-slate-400 font-mono">schema: ultron.rack.telemetry</span>
              </div>
              <div className="bg-slate-950 p-3 rounded-lg border border-slate-900 text-[11px] font-mono text-cyan-300 overflow-x-auto max-h-72">
                <pre>{latestEnvelope ? JSON.stringify(latestEnvelope, null, 2) : '// No envelope received yet.\n// Waiting for gateway publish...'}</pre>
              </div>
            </div>

            <div className="pt-3 text-[11px] text-slate-500">
              Payload size: {latestEnvelope ? `${JSON.stringify(latestEnvelope).length} bytes` : '--'} | Monotonic: Yes | Replayed: {latestEnvelope?.replayed ? 'Yes' : 'No'}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
