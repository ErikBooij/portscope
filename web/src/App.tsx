import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import type { ClientTLSOptions, HeaderRule, Interaction, Payload, RuntimeStatus, Upstream } from "./types";

type UpstreamResponse = { items: Upstream[]; statuses: RuntimeStatus[] };
const emptyTLS: ClientTLSOptions = { enabled: false };
const emptyUpstream: Upstream = {
  id: "", name: "", protocol: "http", listenAddr: "127.0.0.1:9000",
  target: "http://127.0.0.1:3000", enabled: true,
  http: { preserveHost: false, requestHeaders: [], responseHeaders: [], upstreamTls: { ...emptyTLS } },
};

export default function App() {
  const [upstreams, setUpstreams] = useState<Upstream[]>([]);
  const [statuses, setStatuses] = useState<RuntimeStatus[]>([]);
  const [interactions, setInteractions] = useState<Interaction[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [upstreamFilter, setUpstreamFilter] = useState("");
  const [protocolFilter, setProtocolFilter] = useState("");
  const [search, setSearch] = useState("");
  const [connected, setConnected] = useState(false);
  const [editor, setEditor] = useState<Upstream | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const loadUpstreams = useCallback(async () => {
    const response = await api<UpstreamResponse>("/api/upstreams");
    setUpstreams(response.items);
    setStatuses(response.statuses);
  }, []);
  const loadInteractions = useCallback(async () => {
    const params = new URLSearchParams({ limit: "500" });
    if (upstreamFilter) params.set("upstream", upstreamFilter);
    if (protocolFilter) params.set("protocol", protocolFilter);
    if (search) params.set("search", search);
    const response = await api<{ items: Interaction[] }>(`/api/interactions?${params}`);
    setInteractions(response.items);
    setSelected(current => current && response.items.some(item => item.id === current) ? current : response.items[0]?.id ?? null);
  }, [upstreamFilter, protocolFilter, search]);

  useEffect(() => { void loadUpstreams().catch(showError); }, [loadUpstreams]);
  useEffect(() => {
    const timer = window.setTimeout(() => void loadInteractions().catch(showError), search ? 180 : 0);
    return () => clearTimeout(timer);
  }, [loadInteractions, search]);
  useEffect(() => {
    const events = new EventSource("/api/events");
    events.addEventListener("ready", () => setConnected(true));
    events.addEventListener("interaction", event => {
      const item = JSON.parse((event as MessageEvent).data) as Interaction;
      setInteractions(current => {
        if ((upstreamFilter && item.upstreamId !== upstreamFilter) || (protocolFilter && item.protocol !== protocolFilter) || (search && !matches(item, search))) return current;
        return [item, ...current].slice(0, 500);
      });
      setSelected(current => current ?? item.id);
    });
    events.onerror = () => setConnected(false);
    return () => events.close();
  }, [upstreamFilter, protocolFilter, search]);
  useEffect(() => {
    const timer = window.setInterval(() => void loadUpstreams().catch(() => {}), 3000);
    return () => clearInterval(timer);
  }, [loadUpstreams]);

  const active = interactions.find(item => item.id === selected) ?? null;
  const counts = useMemo(() => Object.fromEntries(upstreams.map(item => [item.id, interactions.filter(event => event.upstreamId === item.id).length])), [upstreams, interactions]);

  async function saveUpstream(item: Upstream) {
    setBusy(true);
    try {
      await api(item.id ? `/api/upstreams/${item.id}` : "/api/upstreams", { method: item.id ? "PUT" : "POST", body: JSON.stringify(item) });
      setEditor(null);
      await loadUpstreams();
    } catch (reason) { showError(reason); } finally { setBusy(false); }
  }
  async function removeUpstream(item: Upstream) {
    if (!window.confirm(`Remove ${item.name}? Existing captured traffic is kept.`)) return;
    try {
      await api(`/api/upstreams/${item.id}`, { method: "DELETE" });
      if (upstreamFilter === item.id) setUpstreamFilter("");
      setEditor(null);
      await loadUpstreams();
    } catch (reason) { showError(reason); }
  }
  async function clearTraffic() {
    if (!window.confirm("Clear all captured traffic? This cannot be undone.")) return;
    try {
      await api("/api/interactions", { method: "DELETE" });
      setInteractions([]);
      setSelected(null);
    } catch (reason) { showError(reason); }
  }
  async function demo() {
    setBusy(true);
    try {
      await api("/api/demo", { method: "POST" });
      window.setTimeout(() => setBusy(false), 900);
    } catch (reason) { showError(reason); setBusy(false); }
  }
  function showError(reason: unknown) {
    setError(reason instanceof Error ? reason.message : String(reason));
    window.setTimeout(() => setError(""), 5000);
  }

  return <main className="app-shell">
    <header className="app-header">
      <button className="brand" onClick={() => setUpstreamFilter("")}><i>⟲</i><span>PORTSCOPE<small>INSPECTION PROXY</small></span></button>
      <div className="live-status"><i className={connected ? "online" : ""}/><span>{connected ? "LIVE CAPTURE CONNECTED" : "RECONNECTING"}</span></div>
      <div className="header-actions"><button onClick={demo} disabled={busy}>Generate traffic</button><button className="primary" onClick={() => setEditor(structuredClone(emptyUpstream))}>＋ Add upstream</button></div>
    </header>
    <section className="layout">
      <aside className="sidebar">
        <div className="section-title"><span>UPSTREAMS</span><b>{upstreams.length.toString().padStart(2, "0")}</b></div>
        <button className={`all-traffic ${!upstreamFilter ? "selected" : ""}`} onClick={() => setUpstreamFilter("")}><span>All traffic</span><b>{interactions.length}</b></button>
        <div className="upstream-list">{upstreams.map(item => {
          const status = statuses.find(value => value.upstreamId === item.id);
          return <div className="upstream-entry" key={item.id}>
            <button className={`upstream ${upstreamFilter === item.id ? "selected" : ""}`} onClick={() => setUpstreamFilter(item.id)}>
              <i className={`protocol ${item.protocol}`}>{item.protocol === "http" ? "H" : "R"}</i>
              <span><b>{item.name}</b><small>{item.listenAddr}</small></span>
              <em className={`state ${status?.state ?? "starting"}`} title={status?.detail}>{status?.state ?? "starting"}</em>
              <strong>{counts[item.id] ?? 0}</strong>
            </button>
            <button className="upstream-edit" onClick={() => setEditor(structuredClone(item))} aria-label={`Edit ${item.name}`}>•••</button>
          </div>;
        })}</div>
        <button className="sidebar-add" onClick={() => setEditor(structuredClone(emptyUpstream))}>＋ Configure another upstream</button>
        <div className="sidebar-tip"><b>Route through Portscope</b><p>Point your application at an upstream’s listen address. Portscope forwards the traffic and records both directions.</p></div>
      </aside>
      <section className="traffic-pane">
        <div className="toolbar">
          <label className="search"><span>⌕</span><input value={search} onChange={event => setSearch(event.target.value)} placeholder="Filter operation, body, error…"/></label>
          <div className="segmented"><button className={!protocolFilter ? "active" : ""} onClick={() => setProtocolFilter("")}>ALL</button><button className={protocolFilter === "http" ? "active" : ""} onClick={() => setProtocolFilter("http")}>HTTP</button><button className={protocolFilter === "websocket" ? "active" : ""} onClick={() => setProtocolFilter("websocket")}>WS</button><button className={protocolFilter === "redis" ? "active" : ""} onClick={() => setProtocolFilter("redis")}>REDIS</button></div>
          <button className="clear" onClick={clearTraffic}>Clear</button>
        </div>
        <div className="table-head"><span>TIME</span><span>UPSTREAM</span><span>OPERATION</span><span>RESULT</span><span>DURATION</span><span>SIZE</span></div>
        <div className="traffic-list">
          {interactions.map(item => <button key={item.id} className={`traffic-row ${selected === item.id ? "selected" : ""}`} onClick={() => setSelected(item.id)}>
            <time>{new Date(item.startedAt).toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3 })}</time>
            <span className="row-upstream"><i className={`protocol ${item.protocol}`}>{protocolGlyph(item.protocol)}</i>{upstreams.find(value => value.id === item.upstreamId)?.name ?? item.upstreamId}</span>
            <b>{item.operation}</b><span className={`outcome ${item.outcome}`}>{resultLabel(item)}</span><span>{duration(item.durationUs)}</span><span>{bytes(item.request.size + item.response.size)}</span>
          </button>)}
          {interactions.length === 0 && <Empty onDemo={demo} hasDemo={upstreams.some(item => item.id === "demo-http" && item.enabled)}/>}
        </div>
      </section>
      <aside className={`inspector ${active ? "open" : ""}`}>{active ? <Inspector item={active} upstream={upstreams.find(item => item.id === active.upstreamId)} onClose={() => setSelected(null)}/> : <div className="no-selection"><span>↖</span><b>Select an interaction</b><p>Requests and responses appear here without leaving the live stream.</p></div>}</aside>
    </section>
    <footer><span>PORTSCOPE / LOCAL ONLY</span><span>{interactions.length} INTERACTIONS IN VIEW</span><span>RETENTION 5,000</span></footer>
    {editor && <UpstreamEditor value={editor} busy={busy} onSave={saveUpstream} onDelete={editor.id ? () => removeUpstream(editor) : undefined} onClose={() => setEditor(null)}/>}
    {error && <div className="toast">{error}<button onClick={() => setError("")}>×</button></div>}
  </main>;
}

function Inspector({ item, upstream, onClose }: { item: Interaction; upstream?: Upstream; onClose: () => void }) {
  return <><div className="inspector-head"><span className="eyebrow">INTERACTION DETAIL</span><button className="inspector-close" onClick={onClose} aria-label="Close interaction">×</button><i className={`protocol ${item.protocol}`}>{protocolGlyph(item.protocol)}</i><h2>{item.operation}</h2><p>{upstream?.name ?? item.upstreamId} · {new Date(item.startedAt).toLocaleString()}</p></div><div className="metrics"><span><small>OUTCOME</small><b className={item.outcome}>{resultLabel(item)}</b></span><span><small>DURATION</small><b>{duration(item.durationUs)}</b></span><span><small>CONNECTION</small><b title={item.connection}>{shortConnection(item.connection)}</b></span></div>{item.error && <div className="error-box">{item.error}</div>}<PayloadView title="REQUEST" payload={item.request}/><PayloadView title="RESPONSE" payload={item.response}/>{item.attributes && <section className="attributes"><h3>PROTOCOL ATTRIBUTES</h3>{Object.entries(item.attributes).map(([key, value]) => <div key={key}><span>{key}</span><code>{value}</code></div>)}</section>}</>;
}

function PayloadView({ title, payload }: { title: string; payload: Payload }) {
  const text = payload.json ? JSON.stringify(payload.json, null, 2) : payload.text || payload.summary || "<empty>";
  return <section className="payload"><div><h3>{title}</h3><span>{payload.kind} · {bytes(payload.size)}{payload.truncated ? " · truncated" : ""}</span><button onClick={() => void navigator.clipboard.writeText(text)}>COPY</button></div>{payload.headers && payload.headers.length > 0 && <details><summary>HEADERS · {payload.headers.length}</summary>{payload.headers.map(header => <p key={header.name}><b>{header.name}</b><span>{header.value}</span></p>)}</details>}<pre>{text}</pre></section>;
}

function Empty({ onDemo, hasDemo }: { onDemo: () => void; hasDemo: boolean }) {
  return <div className="empty"><div className="empty-graphic"><i/><i/><i/></div><b>Waiting at the wire.</b><p>Traffic appears here the instant a configured proxy sees a complete interaction.</p>{hasDemo && <button onClick={onDemo}>Send three real requests through Echo Lab →</button>}</div>;
}

function UpstreamEditor({ value, busy, onSave, onDelete, onClose }: { value: Upstream; busy: boolean; onSave: (item: Upstream) => void; onDelete?: () => void; onClose: () => void }) {
  const [draft, setDraft] = useState(value);
  const [localError, setLocalError] = useState("");
  function submit(event: FormEvent) {
    event.preventDefault();
    if (!draft.name.trim() || !draft.listenAddr.trim() || !draft.target.trim()) {
      setLocalError("Name, listen address, and target are required.");
      return;
    }
    onSave(draft);
  }
  function selectProtocol(protocol: "http" | "redis") {
    if (protocol === draft.protocol) return;
    setDraft(protocol === "http"
      ? { ...draft, protocol, target: "http://127.0.0.1:3000", redis: undefined, http: { requestHeaders: [], responseHeaders: [], upstreamTls: { ...emptyTLS } } }
      : { ...draft, protocol, target: "127.0.0.1:6379", http: undefined, listenerTls: undefined, redis: { database: 0, tls: { ...emptyTLS } } });
  }
  const httpOptions = draft.http ?? { requestHeaders: [], responseHeaders: [], upstreamTls: { ...emptyTLS } };
  const redisOptions = draft.redis ?? { database: 0, tls: { ...emptyTLS } };

  return <div className="modal-backdrop" onMouseDown={onClose}>
    <form className="editor advanced-editor" onSubmit={submit} onMouseDown={event => event.stopPropagation()}>
      <div className="editor-head"><div><span className="eyebrow">{draft.id ? "EDIT UPSTREAM" : "NEW UPSTREAM"}</span><h2>{draft.id ? draft.name : "Open another inspection port"}</h2></div><button type="button" onClick={onClose}>×</button></div>
      <div className="editor-scroll">
        <div className="protocol-field"><span className="field-caption">PROTOCOL</span><div className="protocol-choice">
          <button type="button" className={draft.protocol === "http" ? "active" : ""} onClick={() => selectProtocol("http")}><i className="protocol http">H</i><span><b>HTTP</b><small>HTTP/1.1 + HTTP/2 + WS</small></span></button>
          <button type="button" className={draft.protocol === "redis" ? "active" : ""} onClick={() => selectProtocol("redis")}><i className="protocol redis">R</i><span><b>REDIS</b><small>RESP2 + RESP3</small></span></button>
        </div></div>
        <label>DISPLAY NAME<input value={draft.name} onChange={event => setDraft({ ...draft, name: event.target.value })} placeholder="Orders API"/></label>
        <div className="field-pair">
          <label>LISTEN ADDRESS<input value={draft.listenAddr} onChange={event => setDraft({ ...draft, listenAddr: event.target.value })} placeholder="127.0.0.1:9000"/><small>Your application connects here</small></label>
          <label>UPSTREAM TARGET<input value={draft.target} onChange={event => setDraft({ ...draft, target: event.target.value })} placeholder={draft.protocol === "http" ? "https://api.internal" : "cache.internal:6379"}/><small>{draft.protocol === "http" ? "http://, https://, or h2c://" : "host:port"}</small></label>
        </div>

        {draft.protocol === "http" && <HTTPSettings value={draft} options={httpOptions} onChange={next => setDraft({ ...draft, ...next })}/>}
        {draft.protocol === "redis" && <RedisSettings value={redisOptions} onChange={redis => setDraft({ ...draft, redis })}/>}

        <label className="enabled"><input type="checkbox" checked={draft.enabled} onChange={event => setDraft({ ...draft, enabled: event.target.checked })}/><span><b>Start this proxy</b><small>Disabled upstreams keep their configuration and history.</small></span></label>
        {localError && <p className="form-error">{localError}</p>}
      </div>
      <div className="editor-actions">{onDelete && <button className="delete" type="button" onClick={onDelete}>Remove upstream</button>}<span/><button type="button" onClick={onClose}>Cancel</button><button className="primary" disabled={busy}>{busy ? "Applying…" : "Apply configuration"}</button></div>
    </form>
  </div>;
}

function HTTPSettings({ value, options, onChange }: { value: Upstream; options: NonNullable<Upstream["http"]>; onChange: (value: Partial<Upstream>) => void }) {
  const update = (http: NonNullable<Upstream["http"]>) => onChange({ http });
  return <div className="advanced-stack">
    <details className="config-section" open>
      <summary><span>HEADER POLICIES</span><small>{(options.requestHeaders?.length ?? 0) + (options.responseHeaders?.length ?? 0)} rules</small></summary>
      <p className="section-help">Rules run in order. Sensitive values are write-only and redacted from captures.</p>
      <HeaderRules title="REQUEST HEADERS" rules={options.requestHeaders ?? []} onChange={requestHeaders => update({ ...options, requestHeaders })}/>
      <HeaderRules title="RESPONSE HEADERS" rules={options.responseHeaders ?? []} onChange={responseHeaders => update({ ...options, responseHeaders })}/>
      <label className="enabled compact"><input type="checkbox" checked={options.preserveHost ?? false} onChange={event => update({ ...options, preserveHost: event.target.checked })}/><span><b>Preserve downstream Host</b><small>Otherwise Host is rewritten to the upstream target.</small></span></label>
    </details>
    <details className="config-section">
      <summary><span>UPSTREAM TLS + HTTP/2</span><small>{options.upstreamTls?.enabled ? "TLS configured" : "automatic"}</small></summary>
      <p className="section-help">HTTPS negotiates HTTP/2 automatically. Use an h2c:// target for cleartext HTTP/2.</p>
      <TLSClientFields value={options.upstreamTls ?? emptyTLS} onChange={upstreamTls => update({ ...options, upstreamTls })}/>
    </details>
    <details className="config-section">
      <summary><span>LISTENER TLS</span><small>{value.listenerTls?.enabled ? "HTTPS + HTTP/2" : "HTTP + h2c"}</small></summary>
      <ListenerTLSFields value={value.listenerTls ?? { enabled: false }} onChange={listenerTls => onChange({ listenerTls })}/>
    </details>
  </div>;
}

function RedisSettings({ value, onChange }: { value: NonNullable<Upstream["redis"]>; onChange: (value: NonNullable<Upstream["redis"]>) => void }) {
  return <div className="advanced-stack">
    <details className="config-section" open>
      <summary><span>AUTHENTICATION + DATABASE</span><small>{value.passwordSet || value.password ? "credentials stored" : "no authentication"}</small></summary>
      <div className="field-pair">
        <label>ACL USERNAME<input value={value.username ?? ""} onChange={event => onChange({ ...value, username: event.target.value })} placeholder="default"/><small>Leave blank for password-only AUTH</small></label>
        <label>PASSWORD<input type="password" autoComplete="new-password" value={value.password ?? ""} onChange={event => onChange({ ...value, password: event.target.value, passwordSet: event.target.value !== "" })} placeholder={value.passwordSet ? "Stored — enter to replace" : "Optional"}/><small>{value.passwordSet && <button className="inline-danger" type="button" onClick={() => onChange({ ...value, password: "", passwordSet: false })}>Clear stored password</button>}</small></label>
      </div>
      <label>DATABASE<input type="number" min="0" value={value.database ?? 0} onChange={event => onChange({ ...value, database: Number(event.target.value) })}/><small>Portscope issues SELECT before forwarding application traffic.</small></label>
    </details>
    <details className="config-section">
      <summary><span>UPSTREAM TLS</span><small>{value.tls?.enabled ? "encrypted" : "plaintext"}</small></summary>
      <TLSClientFields value={value.tls ?? emptyTLS} onChange={tls => onChange({ ...value, tls })}/>
    </details>
  </div>;
}

function HeaderRules({ title, rules, onChange }: { title: string; rules: HeaderRule[]; onChange: (rules: HeaderRule[]) => void }) {
  function update(index: number, patch: Partial<HeaderRule>) {
    onChange(rules.map((rule, position) => position === index ? { ...rule, ...patch } : rule));
  }
  return <div className="rules">
    <div className="rule-title"><span>{title}</span><button type="button" onClick={() => onChange([...rules, { action: "set", name: "", value: "" }])}>＋ Add rule</button></div>
    {rules.length === 0 && <p className="empty-rules">No mutations — forwarded headers pass through unchanged.</p>}
    {rules.map((rule, index) => <div className="header-rule" key={index}>
      <select aria-label="Header action" value={rule.action} onChange={event => update(index, { action: event.target.value as HeaderRule["action"], value: event.target.value === "remove" ? "" : rule.value })}><option value="set">SET</option><option value="add">ADD</option><option value="remove">REMOVE</option></select>
      <input aria-label="Header name" value={rule.name} onChange={event => update(index, { name: event.target.value })} placeholder="X-Header"/>
      <input aria-label="Header value" type={rule.sensitive ? "password" : "text"} disabled={rule.action === "remove"} value={rule.value ?? ""} onChange={event => update(index, { value: event.target.value, valueSet: event.target.value !== "" })} placeholder={rule.valueSet ? "Stored — enter to replace" : "Value"}/>
      <label className="sensitive-check" title="Redact this value"><input type="checkbox" checked={rule.sensitive ?? false} disabled={rule.action === "remove"} onChange={event => update(index, { sensitive: event.target.checked })}/><span>SECRET</span></label>
      <button className="remove-rule" type="button" onClick={() => onChange(rules.filter((_, position) => position !== index))} aria-label="Remove header rule">×</button>
    </div>)}
  </div>;
}

function TLSClientFields({ value, onChange }: { value: ClientTLSOptions; onChange: (value: ClientTLSOptions) => void }) {
  return <div className="tls-fields">
    <label className="enabled compact"><input type="checkbox" checked={value.enabled} onChange={event => onChange(event.target.checked ? { ...value, enabled: true } : { enabled: false })}/><span><b>Use TLS upstream</b><small>TLS 1.2 minimum; system roots are used by default.</small></span></label>
    {value.enabled && <>
      <div className="field-pair"><label>SERVER NAME<input value={value.serverName ?? ""} onChange={event => onChange({ ...value, serverName: event.target.value })} placeholder="Derived from target"/><small>SNI and certificate hostname</small></label><label>CA PEM FILE<input value={value.caFile ?? ""} onChange={event => onChange({ ...value, caFile: event.target.value })} placeholder="/path/to/ca.pem"/><small>Optional private certificate authority</small></label></div>
      <div className="field-pair"><label>CLIENT CERTIFICATE<input value={value.certFile ?? ""} onChange={event => onChange({ ...value, certFile: event.target.value })} placeholder="/path/to/client.pem"/></label><label>CLIENT KEY<input value={value.keyFile ?? ""} onChange={event => onChange({ ...value, keyFile: event.target.value })} placeholder="/path/to/client-key.pem"/></label></div>
      <label className={`enabled compact danger-toggle ${value.insecureSkipVerify ? "active" : ""}`}><input type="checkbox" checked={value.insecureSkipVerify ?? false} onChange={event => onChange({ ...value, insecureSkipVerify: event.target.checked })}/><span><b>Skip certificate verification</b><small>Dangerous. Use only for temporary local diagnosis.</small></span></label>
    </>}
  </div>;
}

function ListenerTLSFields({ value, onChange }: { value: NonNullable<Upstream["listenerTls"]>; onChange: (value: NonNullable<Upstream["listenerTls"]>) => void }) {
  return <div className="tls-fields">
    <label className="enabled compact"><input type="checkbox" checked={value.enabled} onChange={event => onChange(event.target.checked ? { ...value, enabled: true } : { enabled: false })}/><span><b>Serve HTTPS</b><small>HTTP/1.1 and HTTP/2 are negotiated with ALPN.</small></span></label>
    {value.enabled && <>
      <div className="field-pair"><label>CERTIFICATE PEM<input value={value.certFile ?? ""} onChange={event => onChange({ ...value, certFile: event.target.value })} placeholder="/path/to/server.pem"/></label><label>PRIVATE KEY<input value={value.keyFile ?? ""} onChange={event => onChange({ ...value, keyFile: event.target.value })} placeholder="/path/to/server-key.pem"/></label></div>
      <label>CLIENT CA PEM<input value={value.clientCaFile ?? ""} onChange={event => onChange({ ...value, clientCaFile: event.target.value })} placeholder="/path/to/client-ca.pem"/><small>Optional; verifies a client certificate when one is presented.</small></label>
      <label className="enabled compact"><input type="checkbox" checked={value.requireClientCert ?? false} onChange={event => onChange({ ...value, requireClientCert: event.target.checked })}/><span><b>Require client certificates</b><small>Enables mutual TLS and requires a client CA above.</small></span></label>
    </>}
  </div>;
}

async function api<T = void>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { ...init, headers: { "Content-Type": "application/json", ...init?.headers } });
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try { message = (await response.json()).error ?? message; } catch {}
    throw new Error(message);
  }
  if (response.status === 204) return undefined as T;
  return response.json();
}
function duration(value: number) { if (value < 1000) return `${value} µs`; if (value < 1_000_000) return `${(value / 1000).toFixed(value < 10_000 ? 2 : 1)} ms`; return `${(value / 1_000_000).toFixed(2)} s`; }
function bytes(value: number) { if (value < 1024) return `${value} B`; if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`; return `${(value / 1024 / 1024).toFixed(1)} MB`; }
function resultLabel(item: Interaction) { return item.protocol === "http" ? (item.attributes?.status ?? (item.outcome === "ok" ? "OK" : "ERR")) : (item.outcome === "ok" ? "OK" : "ERR"); }
function protocolGlyph(protocol: Interaction["protocol"]) { return protocol === "http" ? "H" : protocol === "websocket" ? "W" : "R"; }
function shortConnection(value?: string) { if (!value) return "—"; return value.length > 18 ? value.slice(0, 15) + "…" : value; }
function matches(item: Interaction, query: string) {
  const text = [item.operation, item.request.summary, item.request.text, item.request.json ? JSON.stringify(item.request.json) : "", item.response.text, item.response.json ? JSON.stringify(item.response.json) : "", item.error, item.attributes ? JSON.stringify(item.attributes) : ""].filter(Boolean).join(" ").toLowerCase();
  return text.includes(query.toLowerCase());
}
