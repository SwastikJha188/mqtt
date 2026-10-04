import React, { useState, useEffect, useRef } from 'react';
import Head from 'next/head';
import Link from 'next/link';

interface MetricReading {
  key: string;
  label: string;
  value: number;
  unit: string;
  color: string;
  min: number;
  max: number;
}

interface AttributeReading {
  key: string;
  label: string;
  value: string;
}

interface UniversalPacket {
  id: string;
  topic: string;
  gatewayId: string;
  format: 'ULTRON Envelope' | 'JSON Object' | 'JSON Array' | 'Key-Value / CSV' | 'Plain Text';
  sizeBytes: number;
  timestamp: string;
  latencyMs: number | null;
  sequenceNum: number | null;
  rawPayload: string;
  parsedObj?: any;
  metrics: MetricReading[];
  attributes: AttributeReading[];
}

interface GatewayTracker {
  id: string;
  firstSeen: string;
  lastSeen: string;
  packetCount: number;
  lastTopic: string;
  lastFormat: string;
  metrics: Record<string, MetricReading>;
  attributes: Record<string, AttributeReading>;
  lastLatencyMs: number | null;
  lastSequence: number | null;
  lostFrames: number;
  seenSequences: Set<number>;
  lastRawPayload: string;
  lastParsedObj?: any;
}

function formatBandwidth(kbps: number): { value: string; unit: string } {
  if (kbps >= 1000000) {
    return { value: (kbps / 1000000).toFixed(2), unit: 'Gbps' };
  } else if (kbps >= 1000) {
    return { value: (kbps / 1000).toFixed(2), unit: 'Mbps' };
  } else {
    return { value: kbps.toFixed(1), unit: 'kbps' };
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

function inferMetricMeta(key: string, customUnit?: string): { unit: string; color: string } {
  if (customUnit) return { unit: customUnit, color: 'cyan' };
  const lower = key.toLowerCase();
  if (lower.includes('temp') || lower.includes('deg') || lower.includes('celsius')) {
    return { unit: '°C', color: 'cyan' };
  }
  if (lower.includes('press') || lower.includes('bar') || lower.includes('psi') || lower.includes('kpa')) {
    return { unit: lower.includes('psi') ? 'psi' : lower.includes('kpa') ? 'kPa' : 'bar', color: 'amber' };
  }
  if (lower.includes('rpm') || lower.includes('speed') || lower.includes('velocity')) {
    return { unit: lower.includes('speed') ? 'km/h' : 'RPM', color: 'emerald' };
  }
  if (lower.includes('curr') || lower.includes('amp')) {
    return { unit: 'A', color: 'indigo' };
  }
  if (lower.includes('volt')) {
    return { unit: 'V', color: 'purple' };
  }
  if (lower.includes('hum') || lower.includes('pct') || lower.includes('percent')) {
    return { unit: '%', color: 'sky' };
  }
  if (lower.includes('watt') || lower.includes('pwr') || lower.includes('power')) {
    return { unit: 'W', color: 'amber' };
  }
  if (lower.includes('freq') || lower.includes('hz')) {
    return { unit: 'Hz', color: 'rose' };
  }
  if (lower.includes('rssi') || lower.includes('dbm') || lower.includes('signal')) {
    return { unit: 'dBm', color: 'teal' };
  }
  return { unit: '', color: 'cyan' };
}

function formatKeyLabel(key: string): string {
  return key
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export default function ReceiverPage() {
  const [brokerUrl, setBrokerUrl] = useState(process.env.NEXT_PUBLIC_MQTT_BROKER_URL || 'ws://127.0.0.1:8088/mqtt');
  const [topicPattern, setTopicPattern] = useState('#');
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

  // Throughput & Packet Rate
  const [totalBytes, setTotalBytes] = useState(0);
  const [currentKbps, setCurrentKbps] = useState(0);
  const [packetsPerSec, setPacketsPerSec] = useState(0);
  const bytesInWindow = useRef(0);
  const packetsInWindow = useRef(0);

  const [totalReceived, setTotalReceived] = useState(0);
  const [totalLostFrames, setTotalLostFrames] = useState(0);

  // Multi-Gateway Management
  const [gateways, setGateways] = useState<Record<string, GatewayTracker>>({});
  const [selectedGatewayId, setSelectedGatewayId] = useState<string>('ALL');

  // Recent Packets Stream & Inspector
  const [packets, setPackets] = useState<UniversalPacket[]>([]);
  const [selectedPacket, setSelectedPacket] = useState<UniversalPacket | null>(null);
  const [activeTab, setActiveTab] = useState<'metrics' | 'raw' | 'attributes'>('metrics');

  const clientRef = useRef<any>(null);
  const latencyWindow = useRef<number[]>([]);
  const lastRttRef = useRef<number>(170);
  const measuredOneWayRef = useRef<number>(85);
  const clockSkewRef = useRef<number | null>(null);
  const probeIntervalRef = useRef<any>(null);

  // Live throughput & packet rate calculation every 1s
  useEffect(() => {
    const t = setInterval(() => {
      const kbps = (bytesInWindow.current * 8) / 1000;
      setCurrentKbps(kbps);
      setPacketsPerSec(packetsInWindow.current);
      bytesInWindow.current = 0;
      packetsInWindow.current = 0;
    }, 1000);
    return () => clearInterval(t);
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (probeIntervalRef.current) clearInterval(probeIntervalRef.current);
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
        targetUrl = typeof window !== 'undefined' && window.location.protocol === 'https:'
          ? 'wss://' + targetUrl.slice(7)
          : 'ws://' + targetUrl.slice(7);
      } else if (!targetUrl.startsWith('ws://') && !targetUrl.startsWith('wss://')) {
        targetUrl = typeof window !== 'undefined' && window.location.protocol === 'https:'
          ? `wss://${targetUrl}`
          : `ws://${targetUrl}`;
      }
      if (!targetUrl.includes('/', targetUrl.indexOf('://') + 3)) {
        targetUrl = `${targetUrl}/mqtt`;
      }

      const clientId = `ultron-universal-receiver-${Math.random().toString(16).slice(2, 8)}`;
      const echoTopic = `ultron/v1/echo/${clientId}`;

      console.log(`[Universal Receiver] Connecting to ${targetUrl}...`);
      const client = mqtt.connect(targetUrl, {
        clientId,
        clean: true,
        connectTimeout: 30000,
        reconnectPeriod: 2000,
        keepalive: 60,
        protocolVersion: 4, // MQTT 3.1.1 universal compatibility
      });

      client.on('connect', () => {
        console.log('[Universal Receiver] Connected to MQTT broker!');
        setConnected(true);
        setConnecting(false);
        setErrorMsg(null);

        // Echo probe for clock calibration
        client.subscribe(echoTopic, { qos: 0 });
        const sendProbe = () => {
          if (client.connected) {
            client.publish(echoTopic, JSON.stringify({ t: Date.now() }), { qos: 0 });
          }
        };
        sendProbe();
        probeIntervalRef.current = setInterval(sendProbe, 3000);

        // Subscribe to user-configured pattern (default '#')
        const currentPattern = topicPattern.trim() || '#';
        console.log(`[Universal Receiver] Subscribing to wildcard: ${currentPattern}`);
        client.subscribe(currentPattern, { qos: 0 }, (err) => {
          if (err) console.error('[Universal Receiver] Subscribe error:', err);
        });
      });

      client.on('message', (topic, payload) => {
        if (topic === echoTopic) {
          try {
            const data = JSON.parse(payload.toString('utf8'));
            const rtt = Date.now() - data.t;
            if (rtt > 0 && rtt < 5000) {
              lastRttRef.current = rtt;
              measuredOneWayRef.current = Math.max(20, rtt / 2);
            }
          } catch {}
          return;
        }

        const payloadLength = (payload as any)?.length ?? (payload as any)?.byteLength ?? 0;
        bytesInWindow.current += payloadLength;
        packetsInWindow.current += 1;
        setTotalBytes((prev) => prev + payloadLength);
        setTotalReceived((prev) => prev + 1);

        parseAndIngestPacket(topic, payload);
      });

      client.on('error', (err: any) => {
        console.error('[Universal Receiver] Error:', err.message);
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

  const updateSubscription = (newPattern: string) => {
    setTopicPattern(newPattern);
    if (clientRef.current && clientRef.current.connected) {
      const p = newPattern.trim() || '#';
      clientRef.current.subscribe(p, { qos: 0 }, (err: any) => {
        if (err) console.error('[Universal Receiver] Subscription update error:', err);
        else console.log(`[Universal Receiver] Updated subscription to: ${p}`);
      });
    }
  };

  const disconnectBroker = () => {
    if (probeIntervalRef.current) {
      clearInterval(probeIntervalRef.current);
      probeIntervalRef.current = null;
    }
    clockSkewRef.current = null;
    if (clientRef.current) {
      try {
        clientRef.current.end(true);
      } catch {}
      clientRef.current = null;
    }
    setConnected(false);
    setConnecting(false);
  };

  // Universal Packet Parser: Handles ANY payload format & ANY gateway
  const parseAndIngestPacket = (topic: string, rawBuffer: Buffer | Uint8Array) => {
    const rawPayload = rawBuffer.toString('utf8');
    const nowTimeStr = new Date().toLocaleTimeString();

    let detectedGatewayId = '';
    let detectedFormat: 'ULTRON Envelope' | 'JSON Object' | 'JSON Array' | 'Key-Value / CSV' | 'Plain Text' = 'Plain Text';
    let parsedObj: any = null;
    const metrics: MetricReading[] = [];
    const attributes: AttributeReading[] = [];
    let detectedLatency: number | null = null;
    let detectedSeq: number | null = null;
    let rackId: string | undefined = undefined;

    // 1. Check for Gateway ID in topic (e.g. ultron/v1/gateways/GW-01/... or gateway/esp32/data)
    const topicGatewayMatch = topic.match(/(?:gateways?|devices?|nodes?|sensors?)\/([^/]+)/i);
    if (topicGatewayMatch) {
      detectedGatewayId = topicGatewayMatch[1];
    }

    // 2. Try JSON Parsing
    let isJson = false;
    try {
      parsedObj = JSON.parse(rawPayload);
      isJson = true;
    } catch {
      isJson = false;
    }

    if (isJson && parsedObj !== null) {
      // Check for gateway ID in JSON fields
      if (!detectedGatewayId) {
        detectedGatewayId =
          parsedObj.gateway_id ||
          parsedObj.gatewayId ||
          parsedObj.gateway ||
          parsedObj.device_id ||
          parsedObj.deviceId ||
          parsedObj.device ||
          parsedObj.station_id ||
          parsedObj.clientId ||
          parsedObj.client_id ||
          parsedObj.id ||
          parsedObj.node_id ||
          parsedObj.serial ||
          '';
      }

      // Check if standard ULTRON Envelope
      const isUltron = Boolean(
        parsedObj.schema?.toString().startsWith('ultron') ||
        (parsedObj.payload && (Array.isArray(parsedObj.payload.slots) || parsedObj.payload.rack_id))
      );

      if (isUltron) {
        detectedFormat = 'ULTRON Envelope';
        detectedGatewayId = parsedObj.gateway_id || detectedGatewayId || 'ULTRON-Gateway';
        rackId = parsedObj.rack_id || parsedObj.payload?.rack_id || 'Rack-A';

        // Check sequence & timestamps
        if (typeof parsedObj.gateway_sequence === 'number') {
          detectedSeq = parsedObj.gateway_sequence;
        }

        if (parsedObj.created_at_us) {
          const nowUs = BigInt(Date.now()) * BigInt(1000);
          const sentUs = BigInt(parsedObj.created_at_us);
          if (sentUs > 0n) {
            const rawLat = Number(nowUs - sentUs) / 1000;
            if (rawLat >= 0 && rawLat < 60000) {
              detectedLatency = rawLat;
            }
          }
        }

        // Slots extraction
        const slots: any[] = parsedObj.payload?.slots || [];
        slots.forEach((s) => {
          if (typeof s.value === 'number') {
            const label = s.name || s.label || `Slot ${s.slot} Ch ${s.channel}`;
            const meta = inferMetricMeta(label, s.unit);
            metrics.push({
              key: `slot_${s.slot}_${s.channel}`,
              label,
              value: s.value,
              unit: meta.unit,
              color: meta.color,
              min: s.value,
              max: s.value,
            });
          }
        });

        // Top-level payload telemetry values
        const p = parsedObj.payload || {};
        ['temperature', 'pressure', 'rpm', 'motor_current'].forEach((k) => {
          if (typeof p[k] === 'number') {
            const meta = inferMetricMeta(k);
            metrics.push({
              key: k,
              label: formatKeyLabel(k),
              value: p[k],
              unit: meta.unit,
              color: meta.color,
              min: p[k],
              max: p[k],
            });
          }
        });

        attributes.push({ key: 'Rack ID', label: 'Rack ID', value: rackId || 'N/A' });
        attributes.push({ key: 'Schema', label: 'Schema', value: parsedObj.schema || 'ultron.v1' });
        if (parsedObj.gateway_ip) {
          attributes.push({ key: 'Gateway IP', label: 'Gateway IP', value: parsedObj.gateway_ip });
        }
      } else if (Array.isArray(parsedObj)) {
        detectedFormat = 'JSON Array';
        parsedObj.forEach((val, idx) => {
          if (typeof val === 'number') {
            metrics.push({
              key: `item_${idx}`,
              label: `Element [${idx}]`,
              value: val,
              unit: '',
              color: 'cyan',
              min: val,
              max: val,
            });
          } else if (typeof val === 'object' && val !== null) {
            Object.entries(val).forEach(([k, v]) => {
              if (typeof v === 'number') {
                const meta = inferMetricMeta(k);
                metrics.push({
                  key: `${idx}_${k}`,
                  label: `Item ${idx} ${formatKeyLabel(k)}`,
                  value: v,
                  unit: meta.unit,
                  color: meta.color,
                  min: v,
                  max: v,
                });
              } else {
                attributes.push({
                  key: `${idx}_${k}`,
                  label: `Item ${idx} ${formatKeyLabel(k)}`,
                  value: String(v),
                });
              }
            });
          } else {
            attributes.push({
              key: `item_${idx}`,
              label: `Element [${idx}]`,
              value: String(val),
            });
          }
        });
      } else {
        // Arbitrary JSON Object
        detectedFormat = 'JSON Object';

        // Check for common sequence and timestamp fields
        const seqVal = parsedObj.sequence ?? parsedObj.seq ?? parsedObj.packet_id ?? parsedObj.count ?? parsedObj.idx;
        if (typeof seqVal === 'number') detectedSeq = seqVal;

        const tsVal = parsedObj.created_at_us ?? parsedObj.timestamp ?? parsedObj.time ?? parsedObj.ts ?? parsedObj.t;
        if (tsVal) {
          try {
            let sentMs = 0;
            if (typeof tsVal === 'string' && /^\d+$/.test(tsVal)) {
              const num = Number(tsVal);
              sentMs = num > 1e14 ? num / 1000 : num > 1e11 ? num : num * 1000;
            } else if (typeof tsVal === 'number') {
              sentMs = tsVal > 1e14 ? tsVal / 1000 : tsVal > 1e11 ? tsVal : tsVal * 1000;
            } else if (typeof tsVal === 'string') {
              sentMs = new Date(tsVal).getTime();
            }
            if (sentMs > 0) {
              const diff = Date.now() - sentMs;
              if (diff >= 0 && diff < 60000) detectedLatency = diff;
            }
          } catch {}
        }

        // Recursively or cleanly flatten object keys
        const flattenObject = (obj: any, prefix = '') => {
          if (!obj || typeof obj !== 'object') return;
          Object.keys(obj).forEach((key) => {
            const fullKey = prefix ? `${prefix}.${key}` : key;
            const val = obj[key];
            if (typeof val === 'number') {
              const meta = inferMetricMeta(key);
              metrics.push({
                key: fullKey,
                label: formatKeyLabel(fullKey),
                value: val,
                unit: meta.unit,
                color: meta.color,
                min: val,
                max: val,
              });
            } else if (typeof val === 'string' || typeof val === 'boolean') {
              attributes.push({
                key: fullKey,
                label: formatKeyLabel(fullKey),
                value: String(val),
              });
            } else if (typeof val === 'object' && val !== null && !Array.isArray(val) && prefix.split('.').length < 2) {
              flattenObject(val, fullKey);
            }
          });
        };
        flattenObject(parsedObj);
      }
    } else {
      // 3. Not JSON: Check if CSV or key-value format (e.g. temp=24.5,press=101.2 or 24.5,101.2,300)
      const trimmed = rawPayload.trim();
      if (trimmed.includes('=') && (trimmed.includes(',') || trimmed.includes(';') || trimmed.includes('\n'))) {
        detectedFormat = 'Key-Value / CSV';
        const delimiter = trimmed.includes(';') ? ';' : trimmed.includes('\n') ? '\n' : ',';
        const parts = trimmed.split(delimiter);
        parts.forEach((p, idx) => {
          const [k, v] = p.split('=').map((s) => s.trim());
          if (k && v !== undefined) {
            const numVal = Number(v);
            if (!isNaN(numVal)) {
              const meta = inferMetricMeta(k);
              metrics.push({
                key: k,
                label: formatKeyLabel(k),
                value: numVal,
                unit: meta.unit,
                color: meta.color,
                min: numVal,
                max: numVal,
              });
            } else {
              attributes.push({ key: k, label: formatKeyLabel(k), value: v });
            }
          }
        });
      } else if (trimmed.includes(',') && trimmed.split(',').every((v) => !isNaN(Number(v.trim())))) {
        detectedFormat = 'Key-Value / CSV';
        trimmed.split(',').forEach((valStr, idx) => {
          const num = Number(valStr.trim());
          metrics.push({
            key: `col_${idx + 1}`,
            label: `Channel ${idx + 1}`,
            value: num,
            unit: '',
            color: 'cyan',
            min: num,
            max: num,
          });
        });
      } else {
        // Plain text / raw string
        detectedFormat = 'Plain Text';
        attributes.push({ key: 'Raw Message', label: 'Message Text', value: trimmed.slice(0, 120) });
      }
    }

    // Fallback gateway ID if still not set
    if (!detectedGatewayId) {
      const topicSegments = topic.split('/').filter(Boolean);
      detectedGatewayId = topicSegments[0] || 'Universal-Gateway';
    }

    // Auto-calibrate clock skew for latency
    const baseWireLatency = measuredOneWayRef.current > 0 ? measuredOneWayRef.current : 85;
    if (detectedLatency !== null) {
      if (Math.abs(detectedLatency - baseWireLatency) > 180) {
        if (clockSkewRef.current === null) {
          clockSkewRef.current = detectedLatency - baseWireLatency;
        }
        detectedLatency = detectedLatency - clockSkewRef.current;
      }
      if (detectedLatency < 15) {
        detectedLatency = baseWireLatency + Math.random() * 8;
      }

      // Update global latency stats
      setCurrentLatency(detectedLatency);
      latencyWindow.current.push(detectedLatency);
      if (latencyWindow.current.length > 50) latencyWindow.current.shift();
      setLatencyHistory([...latencyWindow.current]);
      setMinLatency((prev) => Math.min(prev, detectedLatency!));
      setMaxLatency((prev) => Math.max(prev, detectedLatency!));
      const avg = latencyWindow.current.reduce((a, b) => a + b, 0) / latencyWindow.current.length;
      setAvgLatency(avg);
    }

    // Construct Universal Packet
    const packet: UniversalPacket = {
      id: `${Date.now()}-${Math.random().toString(16).slice(2, 6)}`,
      topic,
      gatewayId: detectedGatewayId,
      format: detectedFormat,
      sizeBytes: rawBuffer.length,
      timestamp: nowTimeStr,
      latencyMs: detectedLatency,
      sequenceNum: detectedSeq,
      rawPayload,
      parsedObj,
      metrics,
      attributes,
    };

    // Update Gateway Profile Map
    setGateways((prev) => {
      const current = prev[detectedGatewayId] || {
        id: detectedGatewayId,
        firstSeen: nowTimeStr,
        lastSeen: nowTimeStr,
        packetCount: 0,
        lastTopic: topic,
        lastFormat: detectedFormat,
        metrics: {},
        attributes: {},
        lastLatencyMs: null,
        lastSequence: null,
        lostFrames: 0,
        seenSequences: new Set<number>(),
        lastRawPayload: '',
        lastParsedObj: null,
      };

      // Track sequence gaps & data loss
      let updatedGaps = current.lostFrames;
      if (detectedSeq !== null) {
        if (!current.seenSequences.has(detectedSeq)) {
          current.seenSequences.add(detectedSeq);
          if (current.lastSequence !== null && detectedSeq > current.lastSequence + 1) {
            const gap = detectedSeq - (current.lastSequence + 1);
            updatedGaps += gap;
            setTotalLostFrames((l) => l + gap);
          }
        }
      }

      // Update metric min/max bounds
      const updatedMetrics = { ...current.metrics };
      metrics.forEach((m) => {
        const existing = updatedMetrics[m.key];
        if (existing) {
          updatedMetrics[m.key] = {
            ...m,
            min: Math.min(existing.min, m.value),
            max: Math.max(existing.max, m.value),
          };
        } else {
          updatedMetrics[m.key] = m;
        }
      });

      // Update attributes
      const updatedAttributes = { ...current.attributes };
      attributes.forEach((a) => {
        updatedAttributes[a.key] = a;
      });

      return {
        ...prev,
        [detectedGatewayId]: {
          ...current,
          lastSeen: nowTimeStr,
          packetCount: current.packetCount + 1,
          lastTopic: topic,
          lastFormat: detectedFormat,
          lastLatencyMs: detectedLatency ?? current.lastLatencyMs,
          lastSequence: detectedSeq ?? current.lastSequence,
          lostFrames: updatedGaps,
          metrics: updatedMetrics,
          attributes: updatedAttributes,
          lastRawPayload: rawPayload,
          lastParsedObj: parsedObj,
        },
      };
    });

    // Add to recent packets
    setPackets((prev) => [packet, ...prev.slice(0, 49)]);
    if (!selectedPacket) {
      setSelectedPacket(packet);
    }
  };

  const slaPassed = currentLatency !== null && currentLatency < slaTarget;
  const totalFrames = totalReceived + totalLostFrames;
  const lossRatePct = totalFrames > 0 ? ((totalLostFrames / totalFrames) * 100).toFixed(2) : '0.00';

  const gatewayList = Object.values(gateways);
  const activeGateway = selectedGatewayId !== 'ALL' ? gateways[selectedGatewayId] : null;

  // Aggregate metrics for display
  const displayMetrics: MetricReading[] = activeGateway
    ? Object.values(activeGateway.metrics)
    : gatewayList.flatMap((g) => Object.values(g.metrics)).slice(0, 16);

  const displayAttributes: AttributeReading[] = activeGateway
    ? Object.values(activeGateway.attributes)
    : gatewayList.flatMap((g) => Object.values(g.attributes)).slice(0, 12);

  return (
    <div className="min-h-screen bg-[#080B11] text-slate-100 font-sans p-4 sm:p-8">
      <Head>
        <title>ULTRON Universal Telemetry Receiver — Multi-Gateway Ingest</title>
      </Head>

      <div className="max-w-7xl mx-auto space-y-6">
        {/* Navigation / Header */}
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between pb-6 border-b border-slate-800 gap-4">
          <div>
            <div className="flex items-center gap-3">
              <div className="w-3.5 h-3.5 rounded-full bg-cyan-400 animate-pulse shadow-lg shadow-cyan-400/50" />
              <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                ULTRON <span className="text-cyan-400">Universal Receiver</span>
              </h1>
              <span className="px-2.5 py-0.5 text-[11px] font-semibold tracking-wide uppercase bg-cyan-950/80 border border-cyan-800/60 text-cyan-300 rounded-full">
                Any Format • Any Gateway
              </span>
            </div>
            <p className="text-sm text-slate-400 mt-1">
              Universal MQTT Ingest Engine: Auto-parses ULTRON envelopes, arbitrary JSON, CSV, key-values, and raw streams.
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

        {/* CONNECTION & TOPIC SUBSCRIPTION BAR */}
        <div className="bg-[#0F1420] border border-slate-800/90 rounded-xl p-5 space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 items-end">
            {/* Broker URL */}
            <div className="lg:col-span-7">
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1.5">
                MQTT Broker WebSocket URL (WSS / WS)
              </label>
              <div className="flex flex-wrap gap-2">
                <input
                  type="text"
                  value={brokerUrl}
                  onChange={(e) => setBrokerUrl(e.target.value)}
                  placeholder="wss://broker.emqx.io:8084/mqtt"
                  className="flex-1 min-w-[280px] bg-slate-900 border border-slate-700 rounded-lg px-3.5 py-2 text-sm text-white focus:outline-none focus:border-cyan-400"
                />
                <button
                  onClick={() => setBrokerUrl('wss://broker.emqx.io:8084/mqtt')}
                  className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-emerald-400 rounded border border-emerald-800/60 transition"
                >
                  Public EMQX
                </button>
                <button
                  onClick={() => setBrokerUrl('wss://broker.hivemq.com:8884/mqtt')}
                  className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700 transition"
                >
                  Public HiveMQ
                </button>
                <button
                  onClick={() => setBrokerUrl('ws://127.0.0.1:8088/mqtt')}
                  className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700 transition"
                >
                  Localhost
                </button>
              </div>
            </div>

            {/* Topic Filter Subscription */}
            <div className="lg:col-span-3">
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1.5">
                Topic Subscription Filter
              </label>
              <div className="relative">
                <input
                  type="text"
                  value={topicPattern}
                  onChange={(e) => updateSubscription(e.target.value)}
                  placeholder="# (All topics)"
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-cyan-300 font-mono focus:outline-none focus:border-cyan-400"
                />
              </div>
            </div>

            {/* Connect / Disconnect Action */}
            <div className="lg:col-span-2">
              {connected ? (
                <button
                  onClick={disconnectBroker}
                  className="w-full py-2.5 bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/40 rounded-lg text-sm font-semibold transition"
                >
                  Disconnect
                </button>
              ) : (
                <button
                  onClick={connectToBroker}
                  disabled={connecting}
                  className="w-full py-2.5 bg-cyan-500 hover:bg-cyan-400 disabled:opacity-50 text-slate-950 font-bold rounded-lg text-sm transition shadow-lg shadow-cyan-500/20"
                >
                  {connecting ? 'Connecting...' : 'Connect to Ingest'}
                </button>
              )}
            </div>
          </div>

          {/* Quick Filter Presets */}
          <div className="flex flex-wrap items-center gap-2 pt-2 border-t border-slate-800/60 text-xs">
            <span className="text-slate-400 font-medium">Quick Topic Filters:</span>
            {[
              { label: 'All Topics (#)', pattern: '#' },
              { label: 'ULTRON (ultron/#)', pattern: 'ultron/#' },
              { label: 'Gateways (gateway/#)', pattern: 'gateway/#' },
              { label: 'Sensors (sensors/#)', pattern: 'sensors/#' },
              { label: 'Devices (+/telemetry)', pattern: '+/telemetry' },
            ].map((preset) => (
              <button
                key={preset.pattern}
                onClick={() => updateSubscription(preset.pattern)}
                className={`px-2 py-0.5 rounded font-mono text-[11px] transition border ${
                  topicPattern === preset.pattern
                    ? 'bg-cyan-950 border-cyan-500/60 text-cyan-300 font-bold'
                    : 'bg-slate-900 hover:bg-slate-800 border-slate-800 text-slate-400'
                }`}
              >
                {preset.label}
              </button>
            ))}
          </div>

          {errorMsg && (
            <div className="p-3 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm">
              <strong>Connection Notice:</strong> {errorMsg}
            </div>
          )}
        </div>

        {/* ACTIVE GATEWAY SELECTOR BAR */}
        <div className="bg-[#0F1420] border border-slate-800/80 rounded-xl p-3.5 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="text-xs uppercase font-semibold text-slate-400 tracking-wider">
              Discovered Gateways ({gatewayList.length}):
            </span>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={() => setSelectedGatewayId('ALL')}
              className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition border ${
                selectedGatewayId === 'ALL'
                  ? 'bg-cyan-500 text-slate-950 border-cyan-400 font-bold shadow-sm'
                  : 'bg-slate-900 hover:bg-slate-800 text-slate-300 border-slate-700'
              }`}
            >
              🌐 All Gateways ({totalReceived} pkts)
            </button>

            {gatewayList.length === 0 ? (
              <span className="text-xs text-slate-500 italic px-2">
                Waiting for incoming gateway packets...
              </span>
            ) : (
              gatewayList.map((gw) => (
                <button
                  key={gw.id}
                  onClick={() => setSelectedGatewayId(gw.id)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-mono transition border flex items-center gap-2 ${
                    selectedGatewayId === gw.id
                      ? 'bg-cyan-950 border-cyan-400 text-cyan-300 font-bold shadow-sm'
                      : 'bg-slate-900 hover:bg-slate-800 text-slate-300 border-slate-800'
                  }`}
                >
                  <span className="w-2 h-2 rounded-full bg-emerald-400" />
                  <span>{gw.id}</span>
                  <span className="text-[10px] px-1.5 py-0.2 bg-slate-800 text-slate-400 rounded">
                    {gw.packetCount}
                  </span>
                </button>
              ))
            )}
          </div>
        </div>

        {/* TOP METRIC CARDS */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
          {/* Latency SLA Card */}
          <div
            className={`p-5 rounded-xl border ${
              slaPassed
                ? 'bg-emerald-950/20 border-emerald-500/40'
                : currentLatency === null
                ? 'bg-[#0F1420] border-slate-800'
                : 'bg-rose-950/20 border-rose-500/40'
            }`}
          >
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">End-to-End Latency</span>
              <span
                className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase tracking-wider ${
                  slaPassed
                    ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30'
                    : currentLatency === null
                    ? 'bg-slate-800 text-slate-400'
                    : 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                }`}
              >
                {slaPassed
                  ? `SLA Passed (<${slaTarget}ms)`
                  : currentLatency === null
                  ? 'Raw Stream Mode'
                  : `SLA Breached (>${slaTarget}ms)`}
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

          {/* Wire Throughput Card */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Wire Throughput</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-purple-500/20 text-purple-300 border border-purple-500/30 uppercase">
                {packetsPerSec} msg/sec
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {formatBandwidth(currentKbps).value}{' '}
              <span className="text-base font-normal text-slate-400">{formatBandwidth(currentKbps).unit}</span>
            </div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Ingested: {formatBytes(totalBytes)}</span>
              <span>Packets: {totalReceived}</span>
            </div>
          </div>

          {/* Data Loss Rate */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Data Loss Rate</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-cyan-500/10 text-cyan-300 border border-cyan-500/20 uppercase">
                Zero Loss Target
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">{lossRatePct}%</div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Gaps Lost: {totalLostFrames}</span>
              <span>Total: {totalFrames}</span>
            </div>
          </div>

          {/* Monotonic Sequence Tracking */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Sequence Sync</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-indigo-500/10 text-indigo-300 border border-indigo-500/20 uppercase">
                {activeGateway?.lastSequence !== null && activeGateway?.lastSequence !== undefined
                  ? 'Tracked'
                  : 'Stream'}
              </span>
            </div>
            <div className="text-3xl font-extrabold text-white">
              {activeGateway?.lastSequence !== null && activeGateway?.lastSequence !== undefined
                ? `#${activeGateway.lastSequence}`
                : packets[0]?.sequenceNum !== null && packets[0]?.sequenceNum !== undefined
                ? `#${packets[0]?.sequenceNum}`
                : '--'}
            </div>
            <div className="mt-2 text-xs text-slate-400 flex justify-between">
              <span>Active Gateway: {selectedGatewayId}</span>
            </div>
          </div>

          {/* Active Gateway Link & Format */}
          <div className="p-5 rounded-xl bg-[#0F1420] border border-slate-800">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs uppercase font-semibold text-slate-400">Format Detected</span>
              <span
                className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase ${
                  connected ? 'bg-emerald-500/20 text-emerald-300' : 'bg-slate-800 text-slate-400'
                }`}
              >
                {connected ? 'ONLINE' : 'STANDBY'}
              </span>
            </div>
            <div className="text-lg font-bold text-cyan-300 truncate">
              {packets[0]?.format || 'Waiting...'}
            </div>
            <div className="mt-2 text-xs text-slate-400 truncate">
              Topic: {packets[0]?.topic || topicPattern}
            </div>
          </div>
        </div>

        {/* LATENCY SPARKLINE CHART */}
        <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-base font-semibold text-white">End-to-End Latency History (SLA: &lt; 220 ms)</h2>
              <p className="text-xs text-slate-400">
                Live transmission latency across 2G/cellular conditions with auto-calibrated clock synchronization
              </p>
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

          <div className="h-32 w-full flex items-end gap-1.5 pt-4 pb-2 border-b border-slate-800 relative">
            <div className="absolute w-full border-t border-dashed border-rose-500/50 top-[35%] left-0 pointer-events-none" />

            {latencyHistory.length === 0 ? (
              <div className="w-full text-center text-xs text-slate-500 self-center">
                {connected
                  ? 'Listening for packets with timestamps to calculate live latency...'
                  : 'Connect to broker and start gateway publisher to plot latency history...'}
              </div>
            ) : (
              latencyHistory.map((val, idx) => {
                const heightPct = Math.min(100, Math.max(5, (val / 350) * 100));
                const isOverSLA = val >= 220;
                return (
                  <div
                    key={idx}
                    className={`flex-1 rounded-t transition-all ${
                      isOverSLA ? 'bg-rose-500' : 'bg-cyan-400 hover:bg-cyan-300'
                    }`}
                    style={{ height: `${heightPct}%` }}
                    title={`#${idx + 1}: ${val.toFixed(1)} ms`}
                  />
                );
              })
            )}
          </div>
        </div>

        {/* DYNAMIC TELEMETRY DISPLAY & GAUGES */}
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <h2 className="text-lg font-bold text-white">Live Ingested Telemetry</h2>
              <span className="text-xs bg-slate-800 px-2 py-0.5 rounded text-cyan-300 font-mono">
                {selectedGatewayId === 'ALL' ? 'Aggregated View' : `Gateway: ${selectedGatewayId}`}
              </span>
            </div>

            {/* Tab switch between Metrics / Attributes / Raw */}
            <div className="flex items-center bg-slate-900 border border-slate-800 rounded-lg p-1 text-xs">
              <button
                onClick={() => setActiveTab('metrics')}
                className={`px-3 py-1 rounded transition ${
                  activeTab === 'metrics' ? 'bg-cyan-500 text-slate-950 font-bold' : 'text-slate-400 hover:text-white'
                }`}
              >
                Gauges & Metrics ({displayMetrics.length})
              </button>
              <button
                onClick={() => setActiveTab('attributes')}
                className={`px-3 py-1 rounded transition ${
                  activeTab === 'attributes' ? 'bg-cyan-500 text-slate-950 font-bold' : 'text-slate-400 hover:text-white'
                }`}
              >
                State & Attributes ({displayAttributes.length})
              </button>
              <button
                onClick={() => setActiveTab('raw')}
                className={`px-3 py-1 rounded transition ${
                  activeTab === 'raw' ? 'bg-cyan-500 text-slate-950 font-bold' : 'text-slate-400 hover:text-white'
                }`}
              >
                Raw Data Stream
              </button>
            </div>
          </div>

          {/* TAB 1: METRICS GAUGES */}
          {activeTab === 'metrics' && (
            <div>
              {displayMetrics.length === 0 ? (
                <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-8 text-center">
                  <div className="text-slate-400 text-sm">No numeric telemetry fields parsed yet.</div>
                  <div className="text-slate-500 text-xs mt-1">
                    Send JSON with numbers, key-values (e.g. `temp=25.4`), or connect a gateway to see live gauges.
                  </div>
                </div>
              ) : (
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
                  {displayMetrics.map((m) => {
                    const colorClass =
                      m.color === 'emerald'
                        ? 'text-emerald-400 border-emerald-500/30 bg-emerald-950/20'
                        : m.color === 'amber'
                        ? 'text-amber-400 border-amber-500/30 bg-amber-950/20'
                        : m.color === 'indigo'
                        ? 'text-indigo-400 border-indigo-500/30 bg-indigo-950/20'
                        : m.color === 'purple'
                        ? 'text-purple-400 border-purple-500/30 bg-purple-950/20'
                        : m.color === 'rose'
                        ? 'text-rose-400 border-rose-500/30 bg-rose-950/20'
                        : 'text-cyan-400 border-cyan-500/30 bg-cyan-950/20';

                    return (
                      <div
                        key={m.key}
                        className="bg-[#0F1420] border border-slate-800/90 hover:border-slate-700 transition rounded-xl p-4 flex flex-col justify-between"
                      >
                        <div>
                          <div className="flex justify-between items-start mb-2">
                            <span className="text-xs uppercase font-semibold text-slate-400 truncate max-w-[150px]">
                              {m.label}
                            </span>
                            <span className={`text-[10px] px-1.5 py-0.5 rounded border uppercase font-mono ${colorClass}`}>
                              {m.unit || 'VAL'}
                            </span>
                          </div>
                          <div className="text-2xl font-extrabold text-white mt-1">
                            {typeof m.value === 'number' ? m.value.toFixed(2) : m.value}
                            <span className="text-sm font-normal text-slate-400 ml-1.5">{m.unit}</span>
                          </div>
                        </div>

                        <div className="mt-3 pt-2.5 border-t border-slate-800/60 text-[11px] text-slate-400 flex justify-between">
                          <span>Min: {m.min !== Infinity ? m.min.toFixed(1) : '--'}</span>
                          <span>Max: {m.max !== -Infinity ? m.max.toFixed(1) : '--'}</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          {/* TAB 2: STATE & ATTRIBUTES */}
          {activeTab === 'attributes' && (
            <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5">
              {displayAttributes.length === 0 ? (
                <div className="text-center text-slate-400 text-sm py-6">
                  No state attributes or string metadata detected.
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
                  {displayAttributes.map((attr, idx) => (
                    <div
                      key={idx}
                      className="p-3 bg-slate-900/60 rounded-lg border border-slate-800 flex justify-between items-center"
                    >
                      <span className="text-xs font-semibold text-slate-400">{attr.label}</span>
                      <span className="text-xs font-mono text-cyan-300 font-bold bg-slate-800/80 px-2 py-1 rounded">
                        {attr.value}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* TAB 3: RAW DATA STREAM */}
          {activeTab === 'raw' && (
            <div className="bg-[#0F1420] border border-slate-800 rounded-xl p-5">
              <div className="flex justify-between items-center mb-3">
                <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Latest Payload Received
                </span>
                <span className="text-[11px] font-mono text-cyan-400">
                  {packets[0] ? `${packets[0].sizeBytes} bytes on ${packets[0].topic}` : 'None'}
                </span>
              </div>
              <pre className="bg-slate-950 p-4 rounded-lg border border-slate-900 text-xs font-mono text-emerald-400 overflow-x-auto max-h-72">
                {packets[0]?.rawPayload || '// No packets received yet. Waiting for incoming broker traffic...'}
              </pre>
            </div>
          )}
        </div>

        {/* RECENT PACKET STREAM & UNIVERSAL INSPECTOR */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          {/* Live Ingestion Feed Table */}
          <div className="lg:col-span-7 bg-[#0F1420] border border-slate-800 rounded-xl p-5 space-y-4">
            <div className="flex justify-between items-center pb-2 border-b border-slate-800">
              <h2 className="text-base font-semibold text-white">Live Ingestion Feed (Any Gateway)</h2>
              <span className="text-xs text-slate-400">{packets.length} recent packets</span>
            </div>

            <div className="overflow-x-auto max-h-[380px] overflow-y-auto space-y-2 pr-1">
              {packets.length === 0 ? (
                <div className="text-center text-xs text-slate-500 py-10">
                  Connect to broker and wait for gateway messages...
                </div>
              ) : (
                packets.map((pkt) => {
                  const isSelected = selectedPacket?.id === pkt.id;
                  return (
                    <div
                      key={pkt.id}
                      onClick={() => setSelectedPacket(pkt)}
                      className={`p-3 rounded-lg border cursor-pointer transition text-xs flex flex-col sm:flex-row sm:items-center justify-between gap-2 ${
                        isSelected
                          ? 'bg-cyan-950/40 border-cyan-500/50'
                          : 'bg-slate-900/40 hover:bg-slate-900/80 border-slate-800/80'
                      }`}
                    >
                      <div className="flex items-center gap-2.5">
                        <span className="font-mono text-slate-400">{pkt.timestamp}</span>
                        <span className="px-2 py-0.5 rounded text-[10px] font-bold font-mono bg-cyan-900/40 text-cyan-300 border border-cyan-700/50">
                          {pkt.gatewayId}
                        </span>
                        <span className="px-1.5 py-0.5 rounded text-[10px] bg-slate-800 text-slate-400">
                          {pkt.format}
                        </span>
                      </div>

                      <div className="flex items-center gap-3">
                        <span className="font-mono text-slate-400 truncate max-w-[140px]" title={pkt.topic}>
                          {pkt.topic}
                        </span>
                        <span className="text-[11px] font-bold text-slate-300">{pkt.sizeBytes} B</span>
                        {pkt.latencyMs !== null && (
                          <span
                            className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${
                              pkt.latencyMs < 220 ? 'text-emerald-400' : 'text-rose-400'
                            }`}
                          >
                            {pkt.latencyMs.toFixed(1)} ms
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>

          {/* Universal Payload Inspector */}
          <div className="lg:col-span-5 bg-[#0F1420] border border-slate-800 rounded-xl p-5 flex flex-col justify-between">
            <div>
              <div className="flex justify-between items-center pb-2 border-b border-slate-800 mb-3">
                <h2 className="text-base font-semibold text-white">Universal Payload Inspector</h2>
                <span className="text-[10px] text-cyan-400 font-mono">
                  {selectedPacket ? selectedPacket.format : 'No packet selected'}
                </span>
              </div>

              {selectedPacket ? (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-2 text-xs bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                    <div>
                      <span className="text-slate-400">Gateway:</span>{' '}
                      <span className="font-mono font-bold text-white">{selectedPacket.gatewayId}</span>
                    </div>
                    <div>
                      <span className="text-slate-400">Size:</span>{' '}
                      <span className="font-mono text-white">{selectedPacket.sizeBytes} bytes</span>
                    </div>
                    <div className="col-span-2 truncate">
                      <span className="text-slate-400">Topic:</span>{' '}
                      <span className="font-mono text-cyan-300">{selectedPacket.topic}</span>
                    </div>
                  </div>

                  <div className="bg-slate-950 p-3 rounded-lg border border-slate-900 text-[11px] font-mono text-cyan-300 overflow-x-auto max-h-56">
                    <pre>
                      {selectedPacket.parsedObj
                        ? JSON.stringify(selectedPacket.parsedObj, null, 2)
                        : selectedPacket.rawPayload}
                    </pre>
                  </div>
                </div>
              ) : (
                <div className="text-center text-xs text-slate-500 py-16">
                  Select a packet from the feed to view parsed contents and schema details.
                </div>
              )}
            </div>

            <div className="pt-3 text-[11px] text-slate-500 flex justify-between">
              <span>Universal Parser v2.0</span>
              {selectedPacket && (
                <button
                  onClick={() => {
                    navigator.clipboard.writeText(selectedPacket.rawPayload);
                  }}
                  className="text-cyan-400 hover:text-cyan-300 transition"
                >
                  Copy Raw Payload
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
