import React, { useState, useEffect, useRef } from 'react';
import Head from 'next/head';
import Link from 'next/link';

interface NetworkProfile {
  id: string;
  name: string;
  speedLabel: string;
  bandwidthKbps: number;
  delayMs: number;
  badge: string;
}

const NETWORK_PROFILES: NetworkProfile[] = [
  { id: '2g', name: '2G EDGE', speedLabel: '120 kbps', bandwidthKbps: 120, delayMs: 100, badge: 'bg-amber-500/20 text-amber-300 border-amber-500/40' },
  { id: '3g', name: '3G HSPA+', speedLabel: '7.2 Mbps', bandwidthKbps: 7200, delayMs: 45, badge: 'bg-orange-500/20 text-orange-300 border-orange-500/40' },
  { id: '4g', name: '4G LTE', speedLabel: '50 Mbps', bandwidthKbps: 50000, delayMs: 25, badge: 'bg-blue-500/20 text-blue-300 border-blue-500/40' },
  { id: '5g', name: '5G Ultra', speedLabel: '500 Mbps', bandwidthKbps: 500000, delayMs: 8, badge: 'bg-purple-500/20 text-purple-300 border-purple-500/40' },
  { id: '1g', name: '1 Gbps Fiber', speedLabel: '1 Gbps', bandwidthKbps: 1000000, delayMs: 2, badge: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40' },
  { id: '10g', name: '10 Gbps DC', speedLabel: '10 Gbps', bandwidthKbps: 10000000, delayMs: 0, badge: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40' },
];

function formatBandwidth(kbps: number): string {
  if (kbps >= 1000000) {
    return `${(kbps / 1000000).toFixed(2)} Gbps`;
  } else if (kbps >= 1000) {
    return `${(kbps / 1000).toFixed(2)} Mbps`;
  } else {
    return `${kbps.toFixed(1)} kbps`;
  }
}

function formatBytes(bytes: number): string {
  if (bytes >= 1073741824) {
    return `${(bytes / 1073741824).toFixed(2)} GB`;
  } else if (bytes >= 1048576) {
    return `${(bytes / 1048576).toFixed(2)} MB`;
  } else if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  } else {
    return `${bytes} B`;
  }
}

export default function GatewayPage() {
  // Broker Connection
  const [brokerUrl, setBrokerUrl] = useState(process.env.NEXT_PUBLIC_MQTT_BROKER_URL || 'ws://127.0.0.1:8088/mqtt');
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Gateway Identity
  const [gatewayId, setGatewayId] = useState(process.env.NEXT_PUBLIC_GATEWAY_ID || 'ultron-gw-demo-01');
  const [rackId, setRackId] = useState(process.env.NEXT_PUBLIC_RACK_ID || 'Rack-A');
  const [gatewayIp, setGatewayIp] = useState(process.env.NEXT_PUBLIC_GATEWAY_IP || '192.168.1.13');

  // Network Speed & Cadence Controls (from kbps to Gbps)
  const [networkProfileId, setNetworkProfileId] = useState('2g');
  const [publishIntervalMs, setPublishIntervalMs] = useState(1000); // 10ms - 5000ms
  const [payloadDensity, setPayloadDensity] = useState(1); // 1x to 500x multiplier
  const [simulatedNetworkDelay, setSimulatedNetworkDelay] = useState(100);
  const [qosTelemetry, setQosTelemetry] = useState<0 | 1>(0);
  const [stripDuplicateLatest, setStripDuplicateLatest] = useState(false);

  // Machine Parameters (Sliders)
  const [zone1Temp, setZone1Temp] = useState(195.5);
  const [zone2Temp, setZone2Temp] = useState(215.0);
  const [meltPressure, setMeltPressure] = useState(145.2);
  const [screwRpm, setScrewRpm] = useState(275);
  const [motorCurrent, setMotorCurrent] = useState(52.4);

  // Simulation State
  const [isPublishing, setIsPublishing] = useState(false);
  const [totalPublished, setTotalPublished] = useState(0);
  const [currentSequence, setCurrentSequence] = useState(1);
  const [bytesSent, setBytesSent] = useState(0);
  const [currentKbps, setCurrentKbps] = useState(0);

  // Offline Spooling Simulation
  const [isSimulatedCut, setIsSimulatedCut] = useState(false);
  const [spooledMessages, setSpooledMessages] = useState<any[]>([]);

  const clientRef = useRef<any>(null);
  const bootIdRef = useRef<string>(`boot-${Math.random().toString(16).slice(2, 10)}`);
  const publishTimerRef = useRef<any>(null);
  const sequenceRef = useRef(1);
  const bytesInWindow = useRef(0);

  // Calculate live bitrate throughput
  useEffect(() => {
    const t = setInterval(() => {
      const kbps = (bytesInWindow.current * 8) / 1000;
      setCurrentKbps(kbps);
      bytesInWindow.current = 0;
    }, 1000);
    return () => clearInterval(t);
  }, []);

  // Dynamically apply network speed profile
  const applyNetworkProfile = (prof: NetworkProfile) => {
    setNetworkProfileId(prof.id);
    setSimulatedNetworkDelay(prof.delayMs);
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (publishTimerRef.current) clearInterval(publishTimerRef.current);
      if (clientRef.current) {
        try {
          clientRef.current.end(true);
        } catch {}
      }
    };
  }, []);

  const connectBroker = async () => {
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

      console.log(`[Gateway] Connecting to broker at ${targetUrl}...`);
      const client = mqtt.connect(targetUrl, {
        clientId: `ultron-gw-${gatewayId}-${Math.random().toString(16).slice(2, 6)}`,
        clean: true,
        connectTimeout: 30000, // 30s timeout for remote / 2G / cellular networks
        reconnectPeriod: 2000, // auto-reconnect every 2s if signal drops
        keepalive: 60,
        protocolVersion: 4, // MQTT 3.1.1 (universal compatibility & no broker quota locks)
        will: {
          topic: `ultron/v1/gateways/${gatewayId}/status`,
          payload: Buffer.from(
            JSON.stringify({
              schema: 'ultron.gateway.status',
              schema_version: '2.0',
              gateway_id: gatewayId,
              payload: { state: 'OFFLINE', mqtt_state: 'DISCONNECTED', reason: 'unexpected_disconnect' },
            })
          ),
          qos: 1,
          retain: true,
        },
      });

      client.on('connect', () => {
        console.log('[Gateway] Connected to broker successfully!');
        setConnected(true);
        setConnecting(false);
        setErrorMsg(null);

        // Publish initial retained state
        publishRetainedState(client);
      });

      client.on('error', (err: any) => {
        console.error('[Gateway] Connection error:', err.message);
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
    if (publishTimerRef.current) {
      clearInterval(publishTimerRef.current);
      publishTimerRef.current = null;
      setIsPublishing(false);
    }
    if (clientRef.current) {
      try {
        clientRef.current.end(true);
      } catch {}
      clientRef.current = null;
    }
    setConnected(false);
    setConnecting(false);
  };

  const publishRetainedState = (client: any) => {
    if (!client || !client.connected) return;

    // 1. Status
    client.publish(
      `ultron/v1/gateways/${gatewayId}/status`,
      JSON.stringify({
        schema: 'ultron.gateway.status',
        schema_version: '2.0',
        message_id: crypto.randomUUID(),
        gateway_id: gatewayId,
        gateway_boot_id: bootIdRef.current,
        gateway_ip: gatewayIp,
        gateway_sequence: sequenceRef.current++,
        created_at: new Date().toISOString(),
        created_at_us: String(Date.now() * 1000),
        replayed: false,
        payload: { state: 'ONLINE', mqtt_state: 'CONNECTED', uptime_s: 0 },
      }),
      { qos: 1, retain: true }
    );

    // 2. Topology
    client.publish(
      `ultron/v1/gateways/${gatewayId}/topology`,
      JSON.stringify({
        schema: 'ultron.gateway.topology',
        schema_version: '2.0',
        message_id: crypto.randomUUID(),
        gateway_id: gatewayId,
        gateway_boot_id: bootIdRef.current,
        gateway_ip: gatewayIp,
        gateway_sequence: sequenceRef.current++,
        created_at: new Date().toISOString(),
        created_at_us: String(Date.now() * 1000),
        replayed: false,
        payload: {
          known_racks: 1,
          connected_racks: 1,
          racks: [{ rack_id: rackId, status: 'connected', data_current: true }],
        },
      }),
      { qos: 1, retain: true }
    );
  };

  // Dynamically update cadence timer while active
  useEffect(() => {
    if (isPublishing && publishTimerRef.current) {
      clearInterval(publishTimerRef.current);
      publishTimerRef.current = setInterval(sendTelemetrySample, publishIntervalMs);
    }
  }, [publishIntervalMs, isPublishing]);

  const togglePublishing = () => {
    if (isPublishing) {
      if (publishTimerRef.current) clearInterval(publishTimerRef.current);
      publishTimerRef.current = null;
      setIsPublishing(false);
    } else {
      setIsPublishing(true);
      publishTimerRef.current = setInterval(sendTelemetrySample, publishIntervalMs);
      sendTelemetrySample();
    }
  };

  const sendTelemetrySample = () => {
    const seq = sequenceRef.current++;
    setCurrentSequence(seq);

    const nowUs = Date.now() * 1000;
    const sendTimestampUs = nowUs - simulatedNetworkDelay * 1000;

    const baseSlots = [
      { slot: 1, channel: 1, name: 'Feed Zone', value: zone1Temp, unit: '°C' },
      { slot: 2, channel: 1, name: 'Compression Zone', value: zone2Temp, unit: '°C' },
      { slot: 3, channel: 1, name: 'Melt Pressure', value: meltPressure, unit: 'bar' },
      { slot: 4, channel: 1, name: 'Screw Speed', value: screwRpm, unit: 'RPM' },
      { slot: 5, channel: 1, name: 'Drive Motor', value: motorCurrent, unit: 'A' },
    ];

    // Payload density multiplier to saturate from kbps up to Gbps
    let expandedSlots = baseSlots;
    if (payloadDensity > 1) {
      expandedSlots = [];
      const totalRacks = Math.min(payloadDensity, 250);
      for (let r = 0; r < totalRacks; r++) {
        baseSlots.forEach((s) => {
          expandedSlots.push({
            ...s,
            slot: r * 5 + s.slot,
            name: `Rack${r + 1} ${s.name}`,
            value: Number((s.value + (r % 7)).toFixed(2)),
          });
        });
      }
    }

    const payload = {
      rack_id: rackId,
      temperature: zone1Temp,
      pressure: meltPressure,
      rpm: screwRpm,
      motor_current: motorCurrent,
      density_factor: payloadDensity,
      total_channels: expandedSlots.length,
      slots: expandedSlots,
    };

    const envelope = {
      schema: 'ultron.rack.telemetry',
      schema_version: '2.0',
      message_id: crypto.randomUUID(),
      gateway_id: gatewayId,
      gateway_boot_id: bootIdRef.current,
      gateway_ip: gatewayIp,
      gateway_sequence: seq,
      created_at: new Date(Math.floor(sendTimestampUs / 1000)).toISOString(),
      created_at_us: String(sendTimestampUs),
      replayed: false,
      rack_id: rackId,
      payload,
    };

    const messageJson = JSON.stringify(envelope);
    const msgBytes = messageJson.length;
    bytesInWindow.current += msgBytes;
    setBytesSent((prev) => prev + msgBytes);
    setTotalPublished((prev) => prev + 1);

    // If cellular signal is currently "Cut", spool into offline buffer!
    if (isSimulatedCut || !connected) {
      setSpooledMessages((prev) => [...prev, { topic: `ultron/v1/gateways/${gatewayId}/racks/${rackId}/telemetry`, envelope }]);
      return;
    }

    // Publish to Broker
    if (clientRef.current && clientRef.current.connected) {
      const topic = `ultron/v1/gateways/${gatewayId}/racks/${rackId}/telemetry`;
      clientRef.current.publish(topic, messageJson, { qos: qosTelemetry, retain: false });

      if (!stripDuplicateLatest) {
        clientRef.current.publish(`${topic}/latest`, messageJson, { qos: qosTelemetry, retain: true });
      }
    }
  };

  const drainOfflineSpool = () => {
    if (!clientRef.current || !clientRef.current.connected || spooledMessages.length === 0) return;

    console.log(`[Gateway] Replaying ${spooledMessages.length} spooled messages...`);
    spooledMessages.forEach((item) => {
      item.envelope.replayed = true; // Mark as recovered
      clientRef.current.publish(item.topic, JSON.stringify(item.envelope), { qos: 0, retain: false });
    });

    setTotalPublished((prev) => prev + spooledMessages.length);
    setSpooledMessages([]);
  };

  const toggleSimulateCut = () => {
    if (isSimulatedCut) {
      // Reconnected!
      setIsSimulatedCut(false);
      drainOfflineSpool();
    } else {
      // Cut signal
      setIsSimulatedCut(true);
    }
  };

  return (
    <div className="min-h-screen bg-[#080B11] text-slate-100 font-sans p-4 sm:p-8">
      <Head>
        <title>ULTRON Gateway Simulator — Vercel Deployable Publisher</title>
      </Head>

      <div className="max-w-7xl mx-auto space-y-6">
        {/* Navigation / Header */}
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between pb-6 border-b border-slate-800 gap-4">
          <div>
            <div className="flex items-center gap-3">
              <div className="w-3 h-3 rounded-full bg-emerald-400 animate-pulse" />
              <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                ULTRON <span className="text-emerald-400">Gateway Simulator</span>
              </h1>
            </div>
            <p className="text-sm text-slate-400 mt-1">
              Raspberry Pi Hardware Telemetry Publisher (Deployable on Vercel)
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Link
              href="/receiver"
              className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-cyan-300 border border-cyan-500/30 rounded-lg text-sm font-semibold transition"
            >
              ← Open Receiver Console
            </Link>
          </div>
        </div>

        {/* Broker Connection Setup */}
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
                className="flex-1 min-w-[260px] bg-slate-900 border border-slate-700 rounded-lg px-3.5 py-2 text-sm text-white focus:outline-none focus:border-emerald-400"
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
                onClick={connectBroker}
                disabled={connecting}
                className="w-full md:w-auto px-6 py-2.5 bg-emerald-500 hover:bg-emerald-400 disabled:opacity-50 text-slate-950 font-bold rounded-lg text-sm transition shadow-lg shadow-emerald-500/20"
              >
                {connecting ? 'Connecting...' : 'Connect Gateway'}
              </button>
            )}
          </div>
        </div>

        {errorMsg && (
          <div className="p-3.5 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm">
            <strong>Connection Notice:</strong> {errorMsg}
          </div>
        )}

        {/* NETWORK SPEED PROFILE & CADENCE CONTROLS (KBPS TO GBPS) */}
        <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5 space-y-5">
          {/* Header & Live Throughput Badge */}
          <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3 pb-3 border-b border-slate-800">
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-semibold text-white">Network Speed & Sending Cadence</h2>
                <span className="text-[11px] px-2 py-0.5 rounded-full font-bold bg-cyan-500/20 text-cyan-300 border border-cyan-500/30 uppercase">
                  kbps ➔ Gbps
                </span>
              </div>
              <p className="text-xs text-slate-400">Scale network profiles from 2G (120 kbps) up to 10 Gbps and tune sending rate up to 100 Hz</p>
            </div>
            <div className="flex items-center gap-3">
              <div className="text-right">
                <span className="text-[10px] text-slate-400 uppercase tracking-wider block">Live Output Throughput</span>
                <span className="text-base font-mono font-extrabold text-emerald-400">
                  {formatBandwidth(currentKbps)}
                </span>
              </div>
            </div>
          </div>

          {/* 1. Network Profile Selector (kbps to Gbps) */}
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
              Select Network Speed Profile (kbps to Gbps)
            </label>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
              {NETWORK_PROFILES.map((prof) => (
                <button
                  key={prof.id}
                  onClick={() => applyNetworkProfile(prof)}
                  className={`p-2.5 rounded-lg border text-left transition flex flex-col justify-between ${
                    networkProfileId === prof.id
                      ? 'bg-slate-800 border-cyan-400 ring-1 ring-cyan-400 shadow-md'
                      : 'bg-slate-900/60 border-slate-800 hover:border-slate-700 text-slate-400'
                  }`}
                >
                  <span className={`text-xs font-bold ${networkProfileId === prof.id ? 'text-white' : 'text-slate-300'}`}>
                    {prof.name}
                  </span>
                  <span className="text-[11px] font-mono text-cyan-400 mt-1">{prof.speedLabel}</span>
                  <span className="text-[10px] text-slate-500">~{prof.delayMs}ms delay</span>
                </button>
              ))}
            </div>
          </div>

          {/* 2. Cadence & High-Frequency Stream (Hz / ms) */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
            {/* Sending Rate / Interval Slider */}
            <div className="p-4 bg-slate-900/60 rounded-lg border border-slate-800">
              <div className="flex justify-between items-center text-xs mb-2">
                <span className="text-slate-300 font-semibold">Sending Rate (Cadence)</span>
                <span className="text-cyan-400 font-mono font-bold">
                  {publishIntervalMs} ms ({(1000 / publishIntervalMs).toFixed(1)} msg/sec)
                </span>
              </div>
              <input
                type="range"
                min="10"
                max="2000"
                step="10"
                value={publishIntervalMs}
                onChange={(e) => setPublishIntervalMs(Number(e.target.value))}
                className="w-full accent-cyan-400"
              />
              {/* Cadence Presets */}
              <div className="flex flex-wrap gap-1.5 mt-3">
                {[
                  { label: '100 Hz (10ms)', ms: 10 },
                  { label: '50 Hz (20ms)', ms: 20 },
                  { label: '20 Hz (50ms)', ms: 50 },
                  { label: '10 Hz (100ms)', ms: 100 },
                  { label: '5 Hz (200ms)', ms: 200 },
                  { label: '1 Hz (1s)', ms: 1000 },
                ].map((item) => (
                  <button
                    key={item.ms}
                    onClick={() => setPublishIntervalMs(item.ms)}
                    className={`px-2 py-1 text-[11px] rounded border ${
                      publishIntervalMs === item.ms
                        ? 'bg-cyan-500 text-slate-950 font-bold border-cyan-400'
                        : 'bg-slate-800 text-slate-300 border-slate-700 hover:bg-slate-700'
                    }`}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Payload Density (To Saturate High Mbps / Gbps) */}
            <div className="p-4 bg-slate-900/60 rounded-lg border border-slate-800">
              <div className="flex justify-between items-center text-xs mb-2">
                <span className="text-slate-300 font-semibold">Payload Data Density (Throughput Multiplier)</span>
                <span className="text-emerald-400 font-mono font-bold">
                  {payloadDensity}x Density (~{(payloadDensity * 0.4).toFixed(1)} KB/pkt)
                </span>
              </div>
              <p className="text-[11px] text-slate-400 mb-2.5">
                Simulate multi-rack industrial plants to scale data volume up to Gbps link capacity:
              </p>
              {/* Density Presets */}
              <div className="flex flex-wrap gap-1.5">
                {[
                  { label: '1x (~0.4 KB)', factor: 1 },
                  { label: '5x (~2 KB)', factor: 5 },
                  { label: '25x (~10 KB)', factor: 25 },
                  { label: '100x (~40 KB)', factor: 100 },
                  { label: '250x (~100 KB)', factor: 250 },
                ].map((item) => (
                  <button
                    key={item.factor}
                    onClick={() => setPayloadDensity(item.factor)}
                    className={`px-2.5 py-1 text-[11px] rounded border ${
                      payloadDensity === item.factor
                        ? 'bg-emerald-500 text-slate-950 font-bold border-emerald-400'
                        : 'bg-slate-800 text-slate-300 border-slate-700 hover:bg-slate-700'
                    }`}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Artificial Network Delay & QoS Tuning */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 pt-1">
            <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80">
              <div className="flex justify-between text-xs mb-1">
                <span className="text-slate-400">Artificial Wire Delay</span>
                <span className="text-amber-400 font-mono font-bold">{simulatedNetworkDelay} ms</span>
              </div>
              <input
                type="range"
                min="0"
                max="250"
                step="5"
                value={simulatedNetworkDelay}
                onChange={(e) => setSimulatedNetworkDelay(Number(e.target.value))}
                className="w-full accent-amber-400"
              />
            </div>

            <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80">
              <span className="text-xs text-slate-400 block mb-1.5">Telemetry QoS Level</span>
              <div className="flex gap-2">
                <button
                  onClick={() => setQosTelemetry(0)}
                  className={`flex-1 py-1 text-xs font-bold rounded ${qosTelemetry === 0 ? 'bg-emerald-500 text-slate-950' : 'bg-slate-800 text-slate-400'}`}
                >
                  QoS 0 (Zero Overhead)
                </button>
                <button
                  onClick={() => setQosTelemetry(1)}
                  className={`flex-1 py-1 text-xs font-bold rounded ${qosTelemetry === 1 ? 'bg-amber-500 text-slate-950' : 'bg-slate-800 text-slate-400'}`}
                >
                  QoS 1 (ACKed)
                </button>
              </div>
            </div>

            <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80 flex items-center justify-between">
              <div>
                <label className="flex items-center gap-2 cursor-pointer text-xs font-semibold text-slate-300">
                  <input
                    type="checkbox"
                    checked={stripDuplicateLatest}
                    onChange={(e) => setStripDuplicateLatest(e.target.checked)}
                    className="rounded accent-emerald-400"
                  />
                  Compact Wire Topics
                </label>
                <span className="text-[10px] text-slate-500 block">Omits duplicate latest topic to conserve bandwidth</span>
              </div>
            </div>
          </div>
        </div>

        {/* INTERACTIVE MACHINE CONTROLS & PUBLISH MASTER BAR */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Machine Sliders */}
          <div className="lg:col-span-2 bg-[#0F1420] border border-slate-800 rounded-xl p-5 space-y-4">
            <div className="flex justify-between items-center pb-2 border-b border-slate-800">
              <h2 className="text-base font-semibold text-white">Extruder Process Sliders (Live Values)</h2>
              <span className="text-xs text-slate-400">Values update in live envelope</span>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80">
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-slate-400">Zone 1 Feed Temp</span>
                  <span className="text-cyan-400 font-bold">{zone1Temp.toFixed(1)} °C</span>
                </div>
                <input
                  type="range"
                  min="100"
                  max="300"
                  step="0.5"
                  value={zone1Temp}
                  onChange={(e) => setZone1Temp(Number(e.target.value))}
                  className="w-full accent-cyan-400"
                />
              </div>

              <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80">
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-slate-400">Melt Pressure</span>
                  <span className="text-amber-400 font-bold">{meltPressure.toFixed(1)} bar</span>
                </div>
                <input
                  type="range"
                  min="50"
                  max="250"
                  step="0.5"
                  value={meltPressure}
                  onChange={(e) => setMeltPressure(Number(e.target.value))}
                  className="w-full accent-amber-400"
                />
              </div>

              <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80">
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-slate-400">Screw Speed</span>
                  <span className="text-emerald-400 font-bold">{screwRpm} RPM</span>
                </div>
                <input
                  type="range"
                  min="0"
                  max="500"
                  step="5"
                  value={screwRpm}
                  onChange={(e) => setScrewRpm(Number(e.target.value))}
                  className="w-full accent-emerald-400"
                />
              </div>

              <div className="p-3 bg-slate-900/40 rounded-lg border border-slate-800/80">
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-slate-400">Motor Current</span>
                  <span className="text-indigo-400 font-bold">{motorCurrent.toFixed(1)} A</span>
                </div>
                <input
                  type="range"
                  min="10"
                  max="100"
                  step="0.5"
                  value={motorCurrent}
                  onChange={(e) => setMotorCurrent(Number(e.target.value))}
                  className="w-full accent-indigo-400"
                />
              </div>
            </div>

            {/* Master Control Buttons */}
            <div className="pt-3 border-t border-slate-800 flex flex-wrap items-center gap-3">
              <button
                onClick={togglePublishing}
                disabled={!connected}
                className={`px-6 py-2.5 rounded-lg font-bold text-sm transition shadow-lg ${
                  isPublishing
                    ? 'bg-rose-500 hover:bg-rose-400 text-white shadow-rose-500/20'
                    : 'bg-emerald-500 hover:bg-emerald-400 disabled:opacity-50 text-slate-950 shadow-emerald-500/20'
                }`}
              >
                {isPublishing ? '⏹ Stop Telemetry Stream' : '▶ Start Telemetry Stream'}
              </button>

              <button
                onClick={toggleSimulateCut}
                className={`px-4 py-2.5 rounded-lg font-semibold text-xs border transition ${
                  isSimulatedCut
                    ? 'bg-amber-500/20 border-amber-500/50 text-amber-300 hover:bg-amber-500/30'
                    : 'bg-slate-800 border-slate-700 text-slate-300 hover:bg-slate-700'
                }`}
              >
                {isSimulatedCut ? '⚡ Restore Signal & Drain Spool' : '🚫 Simulate Signal Cut (Test Spooling)'}
              </button>
            </div>
          </div>

          {/* Live Publisher Feed Stats */}
          <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5 flex flex-col justify-between space-y-4">
            <div>
              <h2 className="text-base font-semibold text-white mb-2 pb-2 border-b border-slate-800">
                Transmission Statistics
              </h2>

              <div className="space-y-3 text-xs">
                <div className="flex justify-between py-1.5 border-b border-slate-800/60">
                  <span className="text-slate-400">Active Sequence:</span>
                  <span className="font-mono font-bold text-emerald-400">#{currentSequence}</span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-800/60">
                  <span className="text-slate-400">Total Packets Sent:</span>
                  <span className="font-mono font-bold text-white">{totalPublished}</span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-800/60">
                  <span className="text-slate-400">Data Transferred:</span>
                  <span className="font-mono text-cyan-300">{formatBytes(bytesSent)}</span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-800/60">
                  <span className="text-slate-400">Spool Queue Depth:</span>
                  <span className={`font-mono font-bold ${spooledMessages.length > 0 ? 'text-amber-400' : 'text-slate-400'}`}>
                    {spooledMessages.length} msgs
                  </span>
                </div>
                <div className="flex justify-between py-1.5">
                  <span className="text-slate-400">Live Wire Throughput:</span>
                  <span className="font-mono font-bold text-emerald-400">{formatBandwidth(currentKbps)}</span>
                </div>
              </div>
            </div>

            <div className="p-3 bg-slate-950 rounded-lg border border-slate-900 text-[11px] text-slate-400">
              💡 <strong>Deployment Tip:</strong> Deploy this page to Vercel and input your 3rd laptop's local IP or cloud MQTT URL to test live transmission from any device.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
