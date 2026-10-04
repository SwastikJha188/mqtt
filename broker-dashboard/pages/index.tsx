import React, { useState, useEffect, useRef } from 'react';
import Head from 'next/head';
import Link from 'next/link';

interface BrokerMessage {
  id: string;
  topic: string;
  payloadStr: string;
  payloadObj?: any;
  qos: number;
  retain: boolean;
  sizeBytes: number;
  timestamp: string;
  latencyMs?: number | null;
}

export default function BrokerPage() {
  const [brokerUrl, setBrokerUrl] = useState(process.env.NEXT_PUBLIC_MQTT_BROKER_URL || 'ws://127.0.0.1:8088/mqtt');
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Filter
  const [topicFilter, setTopicFilter] = useState('all');

  // Traffic Stats
  const [messages, setMessages] = useState<BrokerMessage[]>([]);
  const [totalMessages, setTotalMessages] = useState(0);
  const [totalBytes, setTotalBytes] = useState(0);
  const [activeTopics, setActiveTopics] = useState<Set<string>>(new Set());
  const [activeClients, setActiveClients] = useState<Set<string>>(new Set());
  const [currentKbps, setCurrentKbps] = useState(0);

  // 2G Speed Simulation Monitor
  const [maxBandwidthKbps] = useState(Number(process.env.NEXT_PUBLIC_2G_MAX_KBPS || 120)); // 2G Limit: 120 kbps

  const clientRef = useRef<any>(null);
  const bytesInWindow = useRef<number>(0);

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

  // Compute live kbps every 1 second
  useEffect(() => {
    const timer = setInterval(() => {
      const kbps = (bytesInWindow.current * 8) / 1000;
      setCurrentKbps(kbps);
      bytesInWindow.current = 0;
    }, 1000);
    return () => clearInterval(timer);
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

      console.log(`[Broker Console] Connecting to ${targetUrl}...`);
      const client = mqtt.connect(targetUrl, {
        clientId: `ultron-broker-monitor-${Math.random().toString(16).slice(2, 8)}`,
        clean: true,
        connectTimeout: 30000, // 30s timeout for remote / 2G / cellular networks
        reconnectPeriod: 2000, // auto-reconnect every 2s if signal drops
        keepalive: 60,
        protocolVersion: 4, // MQTT 3.1.1 (universal compatibility & no broker quota locks)
      });

      client.on('connect', () => {
        console.log('[Broker Console] Connected to MQTT broker!');
        setConnected(true);
        setConnecting(false);
        setErrorMsg(null);

        // Sniff all ULTRON topics
        client.subscribe('ultron/#', { qos: 1 });
        client.subscribe('#', { qos: 0 });
      });

      client.on('message', (topic: string, payload: Buffer, packet: any) => {
        const payloadStr = payload.toString('utf8');
        const sizeBytes = payload.length;
        bytesInWindow.current += sizeBytes;

        let parsed: any = null;
        let latency: number | null = null;
        const isRetained = packet?.retain ?? false;
        try {
          parsed = JSON.parse(payloadStr);
          if (parsed.created_at_us && !isRetained) {
            const nowUs = BigInt(Date.now()) * BigInt(1000);
            const sentUs = BigInt(parsed.created_at_us);
            const diff = Number(nowUs - sentUs) / 1000;
            if (diff >= 0 && diff < 10000) {
              latency = diff;
            }
          }
          if (parsed.gateway_id) {
            setActiveClients((prev) => new Set([...prev, parsed.gateway_id]));
          }
        } catch {}

        setActiveTopics((prev) => new Set([...prev, topic]));
        setTotalMessages((prev) => prev + 1);
        setTotalBytes((prev) => prev + sizeBytes);

        const newMsg: BrokerMessage = {
          id: `${Date.now()}-${Math.random()}`,
          topic,
          payloadStr,
          payloadObj: parsed,
          qos: packet?.qos ?? 0,
          retain: packet?.retain ?? false,
          sizeBytes,
          timestamp: new Date().toLocaleTimeString(),
          latencyMs: latency,
        };

        setMessages((prev) => [newMsg, ...prev.slice(0, 99)]);
      });

      client.on('error', (err: any) => {
        console.error('[Broker Console] Error:', err.message);
        setErrorMsg(err.message || 'Connection failed');
        setConnecting(false);
      });

      client.on('close', () => {
        setConnected(false);
        setConnecting(false);
      });

      clientRef.current = client;
    } catch (err: any) {
      setErrorMsg(err.message || 'Failed to initialize MQTT');
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

  const filteredMessages = messages.filter((m) => {
    if (topicFilter === 'all') return true;
    if (topicFilter === 'telemetry') return m.topic.includes('/telemetry');
    if (topicFilter === 'status') return m.topic.includes('/status') || m.topic.includes('/topology');
    if (topicFilter === 'health') return m.topic.includes('/health') || m.topic.includes('/inventory');
    return true;
  });

  const bandwidthUsagePct = Math.min(100, (currentKbps / maxBandwidthKbps) * 100);

  return (
    <div className="min-h-screen bg-[#080B11] text-slate-100 font-sans p-4 sm:p-8">
      <Head>
        <title>ULTRON Broker Console — 2G Traffic Sniffer & Health Monitor</title>
      </Head>

      <div className="max-w-7xl mx-auto space-y-6">
        {/* Navigation / Header */}
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between pb-6 border-b border-slate-800 gap-4">
          <div>
            <div className="flex items-center gap-3">
              <div className="w-3 h-3 rounded-full bg-amber-400 animate-pulse" />
              <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                ULTRON <span className="text-amber-400">Broker Console</span>
              </h1>
            </div>
            <p className="text-sm text-slate-400 mt-1">
              MQTT Broker Traffic Sniffer, 2G Bandwidth Monitor & Active Client Topology
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Link
              href="/gateway"
              className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-emerald-300 border border-emerald-500/30 rounded-lg text-xs font-semibold transition"
            >
              Gateway Simulator →
            </Link>
            <Link
              href="/receiver"
              className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-cyan-300 border border-cyan-500/30 rounded-lg text-xs font-semibold transition"
            >
              Receiver Console →
            </Link>
          </div>
        </div>

        {/* Broker Connection Bar */}
        <div className="bg-[#0F1420] border border-slate-800/80 rounded-xl p-4 sm:p-5 flex flex-col md:flex-row items-stretch md:items-center justify-between gap-4">
          <div className="flex-1">
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1.5">
              Target MQTT Broker WebSocket URL (3rd Laptop / Cloud / Local)
            </label>
            <div className="flex flex-wrap gap-2">
              <input
                type="text"
                value={brokerUrl}
                onChange={(e) => setBrokerUrl(e.target.value)}
                placeholder="ws://192.168.1.x:8080/mqtt or wss://..."
                className="flex-1 min-w-[260px] bg-slate-900 border border-slate-700 rounded-lg px-3.5 py-2 text-sm text-white focus:outline-none focus:border-amber-400"
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
                className="w-full md:w-auto px-6 py-2.5 bg-amber-500 hover:bg-amber-400 disabled:opacity-50 text-slate-950 font-bold rounded-lg text-sm transition shadow-lg shadow-amber-500/20"
              >
                {connecting ? 'Connecting...' : 'Inspect Broker'}
              </button>
            )}
          </div>
        </div>

        {errorMsg && (
          <div className="p-3.5 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm">
            <strong>Connection Notice:</strong> {errorMsg}
          </div>
        )}

        {/* METRIC OVERVIEW CARDS */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* 2G Bandwidth Utilization Card */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">2G Wire Throughput</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30 uppercase">
                Cap: 120 kbps
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {currentKbps.toFixed(1)} <span className="text-base font-normal text-slate-400">kbps</span>
            </div>
            {/* Bandwidth meter */}
            <div className="mt-3 w-full bg-slate-800 rounded-full h-2 overflow-hidden">
              <div
                className={`h-full transition-all duration-300 ${bandwidthUsagePct > 80 ? 'bg-rose-500' : 'bg-amber-400'}`}
                style={{ width: `${bandwidthUsagePct}%` }}
              />
            </div>
            <div className="mt-1 text-[11px] text-slate-400 flex justify-between">
              <span>{bandwidthUsagePct.toFixed(0)}% of 2G link</span>
              <span>Headroom: {(maxBandwidthKbps - currentKbps).toFixed(1)} kbps</span>
            </div>
          </div>

          {/* Messages Handled */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Messages Routed</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-cyan-500/10 text-cyan-300 border border-cyan-500/20 uppercase">
                Live Feed
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {totalMessages}
            </div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Total Data: {(totalBytes / 1024).toFixed(1)} KB</span>
              <span>Buffer: {messages.length}</span>
            </div>
          </div>

          {/* Active Topics */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Active Topic Branches</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-indigo-500/10 text-indigo-300 border border-indigo-500/20 uppercase">
                MQTT Hierarchy
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {activeTopics.size}
            </div>
            <div className="mt-2 text-xs text-slate-400 truncate">
              Prefix: ultron/v1/gateways/...
            </div>
          </div>

          {/* Connected Publishers */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Identified Publishers</span>
              <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase ${connected ? 'bg-emerald-500/20 text-emerald-300' : 'bg-slate-800 text-slate-400'}`}>
                {connected ? 'BROKER ONLINE' : 'DISCONNECTED'}
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {activeClients.size}
            </div>
            <div className="mt-2 text-xs text-slate-400 truncate">
              {activeClients.size > 0 ? Array.from(activeClients).join(', ') : 'Waiting for traffic...'}
            </div>
          </div>
        </div>

        {/* TOPIC TREE EXPLORER & MESSAGE FEED */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Active Topics Sidebar */}
          <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5 space-y-3">
            <h2 className="text-base font-semibold text-white pb-2 border-b border-slate-800">
              Discovered Topics
            </h2>

            <div className="space-y-1 text-xs">
              <button
                onClick={() => setTopicFilter('all')}
                className={`w-full text-left px-2.5 py-1.5 rounded transition ${topicFilter === 'all' ? 'bg-amber-500/20 text-amber-300 font-bold' : 'text-slate-400 hover:bg-slate-900'}`}
              >
                All Topics ({activeTopics.size})
              </button>
              <button
                onClick={() => setTopicFilter('telemetry')}
                className={`w-full text-left px-2.5 py-1.5 rounded transition ${topicFilter === 'telemetry' ? 'bg-cyan-500/20 text-cyan-300 font-bold' : 'text-slate-400 hover:bg-slate-900'}`}
              >
                Telemetry Topics
              </button>
              <button
                onClick={() => setTopicFilter('status')}
                className={`w-full text-left px-2.5 py-1.5 rounded transition ${topicFilter === 'status' ? 'bg-emerald-500/20 text-emerald-300 font-bold' : 'text-slate-400 hover:bg-slate-900'}`}
              >
                Status & Topology
              </button>
              <button
                onClick={() => setTopicFilter('health')}
                className={`w-full text-left px-2.5 py-1.5 rounded transition ${topicFilter === 'health' ? 'bg-indigo-500/20 text-indigo-300 font-bold' : 'text-slate-400 hover:bg-slate-900'}`}
              >
                Health & Inventory
              </button>
            </div>

            <div className="pt-4 border-t border-slate-800">
              <span className="text-[11px] uppercase tracking-wider text-slate-500 font-semibold block mb-2">
                Active Topic Paths
              </span>
              <div className="max-h-60 overflow-y-auto space-y-1 pr-1 font-mono text-[11px] text-slate-400">
                {activeTopics.size === 0 ? (
                  <span className="text-slate-600">No topics sniffed yet.</span>
                ) : (
                  Array.from(activeTopics).map((t, idx) => (
                    <div key={idx} className="p-1.5 bg-slate-900/60 rounded break-all border border-slate-800/40">
                      {t}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

          {/* Live Message Packet Stream */}
          <div className="lg:col-span-3 bg-[#0F1420] border border-slate-800 rounded-xl p-5 space-y-4">
            <div className="flex justify-between items-center pb-2 border-b border-slate-800">
              <div>
                <h2 className="text-base font-semibold text-white">Live Wire Traffic Sniffer</h2>
                <p className="text-xs text-slate-400">Streaming packets intercepted across the broker</p>
              </div>
              <button
                onClick={() => setMessages([])}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700"
              >
                Clear Stream
              </button>
            </div>

            <div className="max-h-[520px] overflow-y-auto space-y-2.5 pr-2">
              {filteredMessages.length === 0 ? (
                <div className="text-center py-16 text-slate-500 text-sm">
                  {connected
                    ? 'Listening for messages... Start the Gateway Publisher to view live traffic.'
                    : 'Broker monitor disconnected. Click "Inspect Broker" to begin sniffing.'}
                </div>
              ) : (
                filteredMessages.map((msg) => (
                  <div
                    key={msg.id}
                    className="p-3 bg-slate-900/70 border border-slate-800/80 rounded-lg hover:border-slate-700 transition space-y-1.5"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-cyan-400 font-bold">{msg.topic}</span>
                        <span className="text-[10px] px-1.5 py-0.2 bg-slate-800 text-slate-300 rounded">
                          QoS {msg.qos}
                        </span>
                        {msg.retain && (
                          <span className="text-[10px] px-1.5 py-0.2 bg-amber-500/20 text-amber-300 border border-amber-500/30 rounded font-semibold">
                            Retained
                          </span>
                        )}
                      </div>

                      <div className="flex items-center gap-3 text-slate-400 text-[11px]">
                        <span>{msg.sizeBytes} B</span>
                        {msg.latencyMs != null && (
                          <span className={`font-mono font-bold ${msg.latencyMs < 220 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {msg.latencyMs.toFixed(1)} ms
                          </span>
                        )}
                        <span>{msg.timestamp}</span>
                      </div>
                    </div>

                    {/* Compact JSON Preview */}
                    <div className="bg-slate-950 p-2 rounded text-[11px] font-mono text-slate-300 max-h-24 overflow-y-auto overflow-x-auto border border-slate-900">
                      <pre>{msg.payloadObj ? JSON.stringify(msg.payloadObj, null, 2) : msg.payloadStr}</pre>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
