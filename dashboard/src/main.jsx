import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  ArrowDown,
  ArrowRight,
  Check,
  CheckCircle2,
  ChevronRight,
  Circle,
  CircleAlert,
  Clock3,
  Code2,
  Database,
  Download,
  FlaskConical,
  GitBranch,
  Layers3,
  LayoutDashboard,
  LoaderCircle,
  Network,
  Pause,
  Play,
  Radio,
  RefreshCw,
  RotateCcw,
  Search,
  Server,
  ShieldCheck,
  SquareTerminal,
  Unplug,
  X,
  Zap,
} from "lucide-react";
import "./styles.css";

const navigation = [
  ["overview", "Overview", LayoutDashboard],
  ["logs", "Log explorer", Layers3],
  ["chaos", "Chaos lab", Zap],
  ["simulator", "Simulator", FlaskConical],
  ["console", "Client console", SquareTerminal],
];
const blank = {
  nodes: [],
  events: [],
  faults: { isolated: [], delayMs: 0, dropPercent: 0 },
  metrics: {},
  converged: false,
};
const fmt = (v, d = 0) =>
  Number(v || 0).toLocaleString(undefined, { maximumFractionDigits: d });
const shortType = (s) => (s || "").toLowerCase().replaceAll("_", " ");
const time = (s) =>
  s ? new Date(s).toLocaleTimeString("en-GB", { hour12: false }) : "—";
function Pill({ children, tone = "muted" }) {
  return (
    <span className={`pill ${tone}`}>
      <span className="pill-dot" />
      {children}
    </span>
  );
}
function SectionTitle({ eyebrow, title, action }) {
  return (
    <div className="section-title">
      <div>
        {eyebrow && <span className="eyebrow">{eyebrow}</span>}
        <h2>{title}</h2>
      </div>
      {action}
    </div>
  );
}

function App() {
  const [page, setPage] = useState("overview"),
    [state, setState] = useState(blank),
    [connected, setConnected] = useState(false),
    [token, setToken] = useState(""),
    [toast, setToast] = useState(null),
    [busy, setBusy] = useState(""),
    [selected, setSelected] = useState("node-1");
  const toastTimer = useRef();
  useEffect(() => {
    window.scrollTo({ top: 0 });
  }, [page]);
  const notify = (message, error = false) => {
    setToast({ message, error });
    clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(null), 6500);
  };
  useEffect(() => {
    let disposed = false;
    const renewSession = () =>
      fetch("/api/session")
        .then((r) =>
          r.ok
            ? r.json()
            : Promise.reject(new Error("Controller is unavailable")),
        )
        .then((r) => {
          if (!disposed) setToken(r.token);
        })
        .catch((e) => notify(e.message, true));
    const stream = new EventSource("/api/events");
    stream.onopen = renewSession;
    stream.addEventListener("state", (e) => {
      setState(JSON.parse(e.data));
      setConnected(true);
    });
    stream.onerror = () => setConnected(false);
    return () => {
      disposed = true;
      stream.close();
      clearTimeout(toastTimer.current);
    };
  }, []);
  async function request(path, body) {
    if (!token) throw new Error("Waiting for the local controller connection");
    const r = await fetch("/api/" + path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-ShardZero-Control": token,
      },
      body: JSON.stringify(body),
    });
    const text = await r.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      throw new Error(text || "Controller request failed");
    }
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    return data;
  }
  async function act(action, node, extra = {}) {
    setBusy(`${action}:${node || ""}`);
    try {
      await request("chaos", { action, node, ...extra });
      notify(
        action === "heal"
          ? "Network restored. Replicas will catch up."
          : `${shortType(action)} requested${node ? " for " + node : ""}.`,
      );
    } catch (e) {
      notify(e.message, true);
    } finally {
      setBusy("");
    }
  }
  const nodes = state.nodes || [],
    online = nodes.filter((n) => n.online && n.healthy).length,
    leader = nodes
      .filter((n) => n.online && n.role === "leader" && !n.paused)
      .sort((a, b) => b.term - a.term)[0],
    term = Math.max(0, ...nodes.map((n) => n.term || 0)),
    commit = Math.max(0, ...nodes.map((n) => n.commitIndex || 0));
  const titles = {
    overview: [
      "Cluster overview",
      "Watch agreement happen, one committed entry at a time.",
    ],
    logs: [
      "Log explorer",
      "Follow each operation from proposal to applied state.",
    ],
    chaos: ["Chaos lab", "Introduce a failure. Watch the cluster recover."],
    simulator: [
      "Deterministic simulator",
      "The same seed. The same schedule. The same result.",
    ],
    console: [
      "Client console",
      "Send real commands through the leader-aware client.",
    ],
  };
  return (
    <div className="app">
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setPage("overview");
          }}
        >
          <div className="brand-mark">
            <Layers3 size={23} />
          </div>
          <span>
            Shard<span className="brand-zero">Zero</span>
          </span>
        </a>
        <div className="workspace">
          <span className="workspace-avatar">L</span>
          <div>
            <strong>Local cluster</strong>
            <small>Development environment</small>
          </div>
          <ChevronRight size={15} />
        </div>
        <p className="nav-caption">WORKSPACE</p>
        <nav>
          {navigation.map(([id, label, Icon]) => (
            <button
              key={id}
              aria-label={label}
              title={label}
              className={page === id ? "nav-item active" : "nav-item"}
              onClick={() => setPage(id)}
            >
              <Icon size={18} />
              <span>{label}</span>
              {id === "chaos" && state.faults.isolated.length > 0 && (
                <span className="nav-badge">
                  {state.faults.isolated.length}
                </span>
              )}
            </button>
          ))}
        </nav>
        <div className="sidebar-note">
          <GitBranch size={19} />
          <strong>Built on consensus.</strong>
          <p>
            One ordered history.
            <br />
            Independent replicas.
          </p>
          <span className="mono">RAFT · PHASE 08</span>
        </div>
        <div className="sidebar-footer">
          <span className={`connection-dot ${connected ? "on" : ""}`} />
          <div>
            <strong>
              {connected ? "Controller connected" : "Reconnecting…"}
            </strong>
            <small>Local control plane</small>
          </div>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumbs">
            <span>Workspace</span>
            <ChevronRight size={13} />
            <strong>Local cluster</strong>
          </div>
          <div className="topbar-right">
            <span className="mono">HTTP / JSON</span>
            <span className="divider" />
            <span className="environment">
              <Circle size={7} fill="currentColor" /> LOCAL
            </span>
          </div>
        </header>
        <main>
          <div className="page-heading">
            <div>
              <div className="eyebrow heading-eyebrow">
                <span /> DISTRIBUTED SYSTEMS LAB
              </div>
              <h1>{titles[page][0]}</h1>
              <p>{titles[page][1]}</p>
            </div>
            <div className="heading-actions">
              <Pill tone={connected ? "green" : "amber"}>
                {connected ? "Live telemetry" : "Disconnected"}
              </Pill>
              <button
                className="button secondary"
                disabled={!!busy || !connected}
                onClick={() => act("heal")}
              >
                <RefreshCw size={15} /> Heal network
              </button>
            </div>
          </div>
          {!connected && (
            <div className="notice">
              <CircleAlert size={17} /> Waiting for the local lab. Start it with{" "}
              <code>go run ./cmd/lab</code>. Displayed data may be stale.
            </div>
          )}
          {page === "overview" && (
            <>
              <div className="metric-grid">
                <Metric
                  icon={Server}
                  label="Members responding"
                  value={
                    <>
                      {online}
                      <span className="metric-denominator">
                        {" "}
                        / {nodes.length || "—"}
                      </span>
                    </>
                  }
                  detail={`${Math.floor(nodes.length / 2) + 1} required for a majority`}
                  tone="green"
                />
                <Metric
                  icon={GitBranch}
                  label="Current term"
                  value={fmt(term)}
                  detail={
                    leader ? `Leader · ${leader.id}` : "Election in progress"
                  }
                  tone="orange"
                />
                <Metric
                  icon={Layers3}
                  label="Highest commit index"
                  value={fmt(commit)}
                  detail="Durably committed log position"
                />
                <Metric
                  icon={Activity}
                  label="Request throughput"
                  value={
                    <>
                      {fmt(state.metrics.rate, 1)}
                      <span className="metric-unit">ops/s</span>
                    </>
                  }
                  detail="Observed over the last 10 seconds"
                />
              </div>
              <div className="overview-grid">
                <div className="panel topology-panel">
                  <SectionTitle
                    title="Replication topology"
                    action={
                      <span className="tiny-label">
                        <span className="live-dot" /> ACTUAL RPC TRAFFIC
                      </span>
                    }
                  />
                  <Topology
                    nodes={nodes}
                    events={state.events}
                    faults={state.faults}
                    selected={selected}
                    select={setSelected}
                  />
                  <div className="topology-footer">
                    <div className="legend">
                      <span>
                        <i className="leader-dot" />
                        Leader
                      </span>
                      <span>
                        <i className="follower-dot" />
                        Follower
                      </span>
                      <span>
                        <i className="offline-dot" />
                        Unavailable
                      </span>
                    </div>
                    <span className="mono">
                      {nodes.length} independent processes
                    </span>
                  </div>
                </div>
                <div className="panel activity-panel">
                  <SectionTitle
                    title="Cluster activity"
                    action={<Radio size={16} className="muted" />}
                  />
                  <Timeline events={state.timeline || []} compact />
                  <button
                    className="text-button"
                    onClick={() => setPage("chaos")}
                  >
                    Explore the chaos lab <ArrowRight size={14} />
                  </button>
                </div>
              </div>
              <div className="lower-grid">
                <div className="panel">
                  <SectionTitle
                    title="Replicated log"
                    action={
                      <button
                        className="text-button"
                        onClick={() => setPage("logs")}
                      >
                        Open explorer <ArrowRight size={14} />
                      </button>
                    }
                  />
                  <LogTable
                    nodes={nodes}
                    selected={selected}
                    select={setSelected}
                    compact
                  />
                </div>
                <div className="panel integrity-panel">
                  <div
                    className={`integrity-icon ${state.converged ? "good" : ""}`}
                  >
                    <ShieldCheck size={23} />
                  </div>
                  <h2>Replica consistency</h2>
                  <Pill tone={state.converged ? "green" : "amber"}>
                    {state.converged
                      ? "States match"
                      : "Converging / unavailable"}
                  </Pill>
                  <p>{state.consistency || "Waiting for node observations."}</p>
                  <div className="integrity-fact">
                    <span>Scope</span>
                    <strong>Sampled live state</strong>
                  </div>
                  <div className="integrity-fact">
                    <span>Event gaps</span>
                    <strong>{fmt(state.eventGaps)}</strong>
                  </div>
                  <button
                    className="text-button"
                    onClick={() => setPage("simulator")}
                  >
                    Run invariant checks <ArrowRight size={14} />
                  </button>
                </div>
              </div>
            </>
          )}
          {page === "logs" && (
            <div className="panel">
              <SectionTitle
                title="Per-node log inspector"
                action={
                  <span className="muted small">
                    Latest 100 entries · values redacted
                  </span>
                }
              />
              <LogTable
                nodes={nodes}
                selected={selected}
                select={setSelected}
              />
              <div className="panel-note">
                Entry hashes include the full command. A matching index and term
                should have a matching hash on every replica. BARRIER entries
                confirm read authority or a new leadership term.
              </div>
            </div>
          )}
          {page === "chaos" && (
            <Chaos
              state={state}
              request={request}
              act={act}
              busy={busy}
              notify={notify}
            />
          )}
          {page === "simulator" && (
            <Simulator request={request} notify={notify} />
          )}
          {page === "console" && <Console request={request} notify={notify} />}
          <footer className="page-footer">
            <span>
              <span className="footer-square" /> SHARDZERO LAB
            </span>
            <span>Fixed membership · durable journals · majority commits</span>
            <span className="mono">
              {state.time
                ? `UPDATED ${time(state.time)}`
                : "AWAITING TELEMETRY"}
            </span>
          </footer>
        </main>
      </div>
      {toast && (
        <div className={`toast ${toast.error ? "error" : ""}`} role="status">
          {toast.error ? <CircleAlert size={18} /> : <CheckCircle2 size={18} />}
          <span>{toast.message}</span>
          <button
            aria-label="Dismiss notification"
            onClick={() => setToast(null)}
          >
            <X size={15} />
          </button>
        </div>
      )}
    </div>
  );
}
function Metric({ icon: Icon, label, value, detail, tone = "" }) {
  return (
    <div className="metric-card">
      <div className="metric-top">
        <span>{label}</span>
        <Icon size={17} className={tone} />
      </div>
      <div className="metric-value">{value}</div>
      <div className="metric-detail">
        {tone === "green" && <span className="live-dot" />}
        {detail}
      </div>
    </div>
  );
}
function Topology({ nodes, events, faults, selected, select }) {
  const positions =
    nodes.length === 5
      ? [
          [50, 13],
          [81, 38],
          [70, 76],
          [30, 76],
          [19, 38],
        ]
      : [
          [50, 16],
          [24, 72],
          [76, 72],
        ];
  const pairs = [];
  nodes.forEach((n, i) =>
    nodes.slice(i + 1).forEach((other, j) => pairs.push([i, i + j + 1])),
  );
  return (
    <div className={`topology ${nodes.length === 5 ? "five-members" : ""}`}>
      <div className="topology-grid" />
      <svg
        className="connections"
        viewBox="0 0 1000 420"
        preserveAspectRatio="none"
      >
        <defs>
          <linearGradient id="connectionColor">
            <stop stopColor="#d37c44" />
            <stop offset="1" stopColor="#598d9b" />
          </linearGradient>
        </defs>
        {pairs.map(([a, b]) => {
          const p = positions[a],
            q = positions[b];
          const isolated =
            faults.isolated.includes(nodes[a].id) ||
            faults.isolated.includes(nodes[b].id);
          const traffic = events
            .filter(
              (e) =>
                e.type === "APPEND_SENT" &&
                ((e.nodeId === nodes[a].id && e.peer === nodes[b].id) ||
                  (e.nodeId === nodes[b].id && e.peer === nodes[a].id)),
            )
            .at(-1);
          const active =
            traffic &&
            Date.now() - Date.parse(traffic.time) < 1400 &&
            !isolated;
          const from = traffic?.nodeId === nodes[a].id ? p : q;
          const to = traffic?.nodeId === nodes[a].id ? q : p;
          return (
            <g key={a + "-" + b}>
              <line
                x1={p[0] * 10}
                y1={p[1] * 4.2}
                x2={q[0] * 10}
                y2={q[1] * 4.2}
                className={
                  isolated ? "edge isolated" : active ? "edge active" : "edge"
                }
              />
              {active && (
                <circle key={traffic.messageId} r="3.5" fill="#ffad73">
                  <animateMotion
                    dur="1s"
                    repeatCount="1"
                    fill="freeze"
                    path={`M ${from[0] * 10} ${from[1] * 4.2} L ${to[0] * 10} ${to[1] * 4.2}`}
                  />
                </circle>
              )}
            </g>
          );
        })}
      </svg>
      <div className="topology-center">
        <div className="consensus-ring">
          <GitBranch size={22} />
        </div>
        <span>RAFT GROUP</span>
        <small>{Math.floor(nodes.length / 2) + 1}-node majority</small>
      </div>
      {nodes.map((n, i) => {
        const pos = positions[i],
          offline = !n.online || !n.healthy,
          role = offline ? (n.paused ? "paused" : "offline") : n.role;
        return (
          <button
            onClick={() => select(n.id)}
            key={n.id}
            style={{ left: pos[0] + "%", top: pos[1] + "%" }}
            className={`node-card ${role} ${selected === n.id ? "selected" : ""}`}
            aria-label={`Inspect ${n.id}, ${role}`}
          >
            <div className="node-card-top">
              <span className="node-icon">
                <Database size={18} />
              </span>
              <strong>{n.id}</strong>
              <span className="node-status-dot" />
            </div>
            <div className="node-card-meta">
              <span>{role || "starting"}</span>
              <span>term {n.term ?? "—"}</span>
            </div>
            <div className="node-index">
              <span>COMMIT</span>
              <b>{n.commitIndex ?? "—"}</b>
              <span>APPLIED</span>
              <b>{n.lastApplied ?? "—"}</b>
            </div>
            {faults.isolated.includes(n.id) && (
              <span className="isolated-label">ISOLATED</span>
            )}
          </button>
        );
      })}
      {nodes.length === 0 && (
        <div className="empty topology-empty">
          <LoaderCircle className="spinning" />
          Waiting for cluster…
        </div>
      )}
    </div>
  );
}
function Timeline({ events, compact = false }) {
  const meaningful = events.filter(
    (e) =>
      ![
        "APPEND_SENT",
        "APPEND_RECEIVED",
        "APPEND_RESPONSE",
        "APPLIED",
        "PROPOSED",
        "COMMITTED",
      ].includes(e.type),
  );
  const shown = [...meaningful]
    .sort((a, b) => Date.parse(b.time) - Date.parse(a.time) || b.id - a.id)
    .slice(0, compact ? 6 : 30);
  return (
    <div className={`timeline ${compact ? "compact" : ""}`}>
      {shown.length ? (
        shown.map((e) => (
          <div className="timeline-item" key={e.id}>
            <span
              className={`timeline-dot ${e.type.includes("ELECTED") ? "orange-bg" : e.type.includes("DROP") || e.type.includes("EXIT") ? "red-bg" : ""}`}
            />
            <div>
              <div className="timeline-row">
                <strong>{shortType(e.type)}</strong>
                <time>{time(e.time)}</time>
              </div>
              <p>
                {e.nodeId || "Network"}
                {e.peer ? " → " + e.peer : ""}
                {e.term ? " · term " + e.term : ""}
              </p>
              {!compact && e.detail && <small>{e.detail}</small>}
            </div>
          </div>
        ))
      ) : (
        <div className="empty">
          <Radio size={22} />
          <p>Waiting for protocol events</p>
        </div>
      )}
    </div>
  );
}
function LogTable({ nodes, selected, select, compact = false }) {
  const node = nodes.find((n) => n.id === selected) || nodes[0];
  const rows = [...(node?.log || [])].reverse().slice(0, compact ? 5 : 100);
  return (
    <>
      <div className="node-tabs" role="tablist" aria-label="Replica log">
        {nodes.map((n) => (
          <button
            role="tab"
            aria-selected={n.id === node?.id}
            className={n.id === node?.id ? "chosen" : ""}
            key={n.id}
            onClick={() => select(n.id)}
          >
            <span className={`tab-dot ${n.online ? "on" : ""}`} />
            {n.id}
            {n.role === "leader" && <span className="tab-leader">L</span>}
          </button>
        ))}
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>INDEX</th>
              <th>TERM</th>
              <th>OPERATION</th>
              <th>KEY</th>
              {!compact && <th>ENTRY HASH</th>}
              <th>STATE</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.index}>
                <td className="mono index-cell">#{row.index}</td>
                <td className="mono muted">{row.term}</td>
                <td>
                  <span className={`operation ${row.operation.toLowerCase()}`}>
                    {row.operation}
                  </span>
                </td>
                <td className="mono key-cell">{row.key || "—"}</td>
                {!compact && (
                  <td className="mono muted" title={row.hash}>
                    {row.hash.slice(0, 14)}…
                  </td>
                )}
                <td>
                  <span
                    className={`entry-state ${row.applied ? "applied" : ""}`}
                  >
                    {row.applied ? <Check size={12} /> : <Clock3 size={12} />}{" "}
                    {row.applied
                      ? "Applied"
                      : row.committed
                        ? "Committed"
                        : "Uncommitted"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!rows.length && (
          <div className="empty">No observed log entries yet</div>
        )}
      </div>
    </>
  );
}
function Chaos({ state, request, act, busy, notify }) {
  const [delay, setDelay] = useState(120),
    [drop, setDrop] = useState(10),
    [working, setWorking] = useState(false);
  const toggleWork = async () => {
    setWorking(true);
    try {
      await request("workload", { enabled: !state.metrics.workload });
    } catch (e) {
      notify(e.message, true);
    } finally {
      setWorking(false);
    }
  };
  return (
    <>
      <div className="chaos-banner">
        <div className="chaos-banner-icon">
          <Zap size={26} />
        </div>
        <div>
          <h2>Make failure visible.</h2>
          <p>
            These controls affect the lab’s real node processes and peer
            messages. Durable data is retained.
          </p>
        </div>
        <button
          className="button secondary"
          disabled={!!busy}
          onClick={() => act("reset")}
        >
          <RotateCcw size={15} />
          Reset faults
        </button>
      </div>
      <div className="node-control-grid">
        {state.nodes.map((n) => (
          <div className="panel node-control" key={n.id}>
            <div className="node-control-heading">
              <Database size={21} />
              <h2>{n.id}</h2>
              <Pill tone={n.online && n.healthy ? "green" : "amber"}>
                {n.paused ? "paused" : n.online ? n.role : "offline"}
              </Pill>
            </div>
            <div className="control-details">
              <span>
                Term <b>{n.term ?? "—"}</b>
              </span>
              <span>
                Commit <b>{n.commitIndex ?? "—"}</b>
              </span>
            </div>
            <div className="control-buttons">
              <button
                className="button secondary"
                disabled={!!busy || !n.running}
                onClick={() => act(n.paused ? "resume" : "pause", n.id)}
              >
                {n.paused ? <Play size={14} /> : <Pause size={14} />}{" "}
                {n.paused ? "Resume" : "Pause"}
              </button>
              <button
                className="button danger"
                disabled={!!busy || !n.running}
                onClick={() => act("crash", n.id)}
              >
                <Unplug size={14} />
                Crash
              </button>
              <button
                className="button secondary"
                disabled={!!busy || n.running}
                onClick={() => act("restart", n.id)}
              >
                <RefreshCw size={14} />
                Restart
              </button>
              <button
                className={`button ${state.faults.isolated.includes(n.id) ? "orange-button" : "secondary"}`}
                disabled={!!busy}
                onClick={() =>
                  act("network", "", {
                    isolated: state.faults.isolated.includes(n.id)
                      ? state.faults.isolated.filter((id) => id !== n.id)
                      : [...state.faults.isolated, n.id],
                    delayMs: state.faults.delayMs,
                    dropPercent: state.faults.dropPercent,
                  })
                }
              >
                <GitBranch size={14} />
                {state.faults.isolated.includes(n.id) ? "Reconnect" : "Isolate"}
              </button>
            </div>
          </div>
        ))}
      </div>
      <div className="lower-grid">
        <div className="panel fault-settings">
          <SectionTitle eyebrow="PEER TRANSPORT" title="Network conditions" />
          <label className="range-label">
            Message delay <span className="mono">{delay} ms</span>
            <input
              aria-label="Message delay"
              type="range"
              min="0"
              max="900"
              step="10"
              value={delay}
              onChange={(e) => setDelay(+e.target.value)}
            />
          </label>
          <label className="range-label">
            Message loss <span className="mono">{drop}%</span>
            <input
              aria-label="Message loss"
              type="range"
              min="0"
              max="100"
              step="1"
              value={drop}
              onChange={(e) => setDrop(+e.target.value)}
            />
          </label>
          <div className="settings-bottom">
            <span className="small muted">
              Active: {state.faults.delayMs} ms / {state.faults.dropPercent}%
              loss
            </span>
            <button
              className="button primary"
              disabled={!!busy}
              onClick={() =>
                act("network", "", {
                  isolated: state.faults.isolated,
                  delayMs: delay,
                  dropPercent: drop,
                })
              }
            >
              Apply conditions <ArrowRight size={15} />
            </button>
          </div>
        </div>
        <div className="panel workload-panel">
          <SectionTitle
            eyebrow="CONTINUOUS WRITES"
            title="Workload generator"
          />
          <p>
            Write a counter through the real client at up to 5 requests per
            second. Retries retain the same request identity.
          </p>
          <div className="workload-stat">
            <span>
              {fmt(state.metrics.p95Ms, 1)}
              <small>ms</small>
            </span>
            <label>
              p95 request latency
              <br />
              <small>Last 300 observed requests</small>
            </label>
          </div>
          <button
            className={`button ${state.metrics.workload ? "secondary" : "primary"}`}
            disabled={working}
            onClick={toggleWork}
          >
            {state.metrics.workload ? <Pause size={16} /> : <Play size={16} />}{" "}
            {state.metrics.workload ? "Stop workload" : "Start workload"}
          </button>
        </div>
      </div>
      <div className="panel">
        <SectionTitle
          title="Before / after fault"
          action={
            <Pill tone={state.observed?.matched ? "green" : "amber"}>
              {state.observed?.matched
                ? "Replicas agree now"
                : "Replicas unavailable / catching up"}
            </Pill>
          }
        />
        {state.beforeFault ? (
          <>
            <div className="panel-note">
              Before the latest injected fault ({time(state.beforeFault.time)}):{" "}
              {state.beforeFault.matched
                ? "replicas agreed"
                : "replicas differed or were unavailable"}
              . Current observation: {time(state.observed?.time)}.
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>REPLICA</th>
                    <th>BEFORE · APPLIED / STATE HASH</th>
                    <th>NOW · APPLIED / STATE HASH</th>
                  </tr>
                </thead>
                <tbody>
                  {state.observed?.replicas.map((node) => {
                    const prior = state.beforeFault.replicas.find(
                      (n) => n.id === node.id,
                    );
                    const label = (n) =>
                      n?.online && n?.healthy
                        ? `${n.applied} / ${n.hash.slice(0, 14)}…`
                        : "Unavailable";
                    return (
                      <tr key={node.id}>
                        <td>{node.id}</td>
                        <td className="mono muted">{label(prior)}</td>
                        <td className="mono">{label(node)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div className="panel-note">
              Each comparison checks agreement across replicas at that
              observation. Writes can change the hash over time; this sampled
              view is not a linearizability proof.
            </div>
          </>
        ) : (
          <div className="empty">
            Inject a fault to capture the current replica state, then heal to
            compare recovery.
          </div>
        )}
      </div>
      <div className="panel">
        <SectionTitle title="Fault and election timeline" />
        <Timeline events={state.timeline || []} />
      </div>
    </>
  );
}
function Simulator({ request, notify }) {
  const [seed, setSeed] = useState("728391"),
    [nodes, setNodes] = useState(3),
    [operations, setOperations] = useState(60),
    [result, setResult] = useState(null),
    [running, setRunning] = useState(false),
    [match, setMatch] = useState(null);
  async function run(replay = false) {
    setRunning(true);
    try {
      const body =
        replay && result
          ? { scenario: result.scenario }
          : { seed: Number(seed), nodes, operations };
      const next = await request("simulate", body);
      if (replay && result) setMatch(next.traceHash === result.traceHash);
      else setMatch(null);
      setResult(next);
      if (!next.passed) notify(next.error || "Invariant check failed", true);
    } catch (e) {
      notify(e.message, true);
    } finally {
      setRunning(false);
    }
  }
  function download() {
    const blob = new Blob([JSON.stringify(result.scenario, null, 2)], {
      type: "application/json",
    });
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = href;
    a.download = `seed-${result.scenario.seed}.scenario.json`;
    a.click();
    URL.revokeObjectURL(href);
  }
  return (
    <div className="sim-grid">
      <div className="panel sim-config">
        <div className="sim-icon">
          <FlaskConical size={30} />
        </div>
        <h2>Reproduce the unexpected.</h2>
        <p>
          A virtual clock and seeded event queue drive the same Raft protocol
          used by your live cluster.
        </p>
        <label className="field">
          Random seed
          <input
            type="number"
            min="0"
            max="9007199254740991"
            value={seed}
            onChange={(e) => setSeed(e.target.value)}
          />
        </label>
        <div className="form-row">
          <label className="field">
            Members
            <select value={nodes} onChange={(e) => setNodes(+e.target.value)}>
              <option value={3}>3 nodes</option>
              <option value={5}>5 nodes</option>
            </select>
          </label>
          <label className="field">
            Operations
            <input
              type="number"
              min="1"
              max="500"
              value={operations}
              onChange={(e) => setOperations(+e.target.value)}
            />
          </label>
        </div>
        <div className="scenario-description">
          <span className="eyebrow">FAULT SCHEDULE</span>
          <p>
            <Unplug size={14} /> Crash → restart
          </p>
          <p>
            <GitBranch size={14} /> Partition → heal
          </p>
          <p>
            <Pause size={14} /> Pause → resume
          </p>
          <p>
            <Database size={14} /> Persistence failure → restart
          </p>
          <p>
            <Network size={14} /> Delay, drop, and duplicate messages
          </p>
        </div>
        <button
          className="button primary full-width"
          disabled={running || !seed || operations < 1 || operations > 500}
          onClick={() => run()}
        >
          {running ? (
            <LoaderCircle size={16} className="spinning" />
          ) : (
            <Play size={16} />
          )}{" "}
          {running ? "Running virtual schedule…" : "Run simulation"}
        </button>
        <p className="small subtle">
          Simulation is isolated from the live cluster. This checks modeled
          schedules; it is not a universal proof.
        </p>
      </div>
      <div className="panel sim-result">
        <SectionTitle
          title="Verification report"
          action={
            result && (
              <Pill tone={result.passed ? "green" : "red"}>
                {result.passed ? "Passed" : "Failed"}
              </Pill>
            )
          }
        />
        {result ? (
          <>
            <div className="result-metrics">
              <div>
                <strong>
                  {result.acknowledged}
                  <small>/{result.scenario.operations}</small>
                </strong>
                <span>Writes acknowledged</span>
              </div>
              <div>
                <strong>{fmt(result.steps)}</strong>
                <span>Scheduled events</span>
              </div>
              <div>
                <strong>
                  {(result.virtualMs / 1000).toFixed(1)}
                  <small>s</small>
                </strong>
                <span>Virtual time</span>
              </div>
            </div>
            <div className="checks">
              {result.checks.map((check) => (
                <div key={check}>
                  {result.passed ? (
                    <CheckCircle2 size={17} />
                  ) : (
                    <CircleAlert size={17} />
                  )}
                  <span>{check}</span>
                  <span className="mono">
                    {result.passed ? "PASS" : "SEE FAILURE"}
                  </span>
                </div>
              ))}
            </div>
            {result.error && <div className="notice error">{result.error}</div>}
            <div className="trace-hash">
              <span className="eyebrow">FULL EXECUTION TRACE · SHA-256</span>
              <code>{result.traceHash}</code>
            </div>
            {match !== null && (
              <div className={`replay-result ${match ? "success" : "error"}`}>
                {match ? <CheckCircle2 size={17} /> : <CircleAlert size={17} />}{" "}
                {match
                  ? "Replay matched the original trace exactly."
                  : "Replay produced a different trace."}
              </div>
            )}
            <div className="result-actions">
              <button
                className="button secondary"
                disabled={running}
                onClick={() => run(true)}
              >
                <RotateCcw size={15} />
                Replay same scenario
              </button>
              <button className="button secondary" onClick={download}>
                <Download size={15} />
                Save scenario
              </button>
            </div>
            <div className="panel-note">
              Replay in your terminal:{" "}
              <code>
                go run ./cmd/simulator --scenario=path/to/scenario.json
              </code>
            </div>
          </>
        ) : (
          <div className="sim-empty">
            <div className="orbit">
              <ShieldCheck size={38} />
            </div>
            <h3>Every failure has a sequence.</h3>
            <p>
              Run a seeded scenario to check safety invariants,
              <br />
              recovery, and agreement across replicas.
            </p>
            <div className="empty-chips">
              <span>VIRTUAL CLOCK</span>
              <span>REAL RAFT CORE</span>
              <span>REPEATABLE</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
function Console({ request, notify }) {
  const [operation, setOperation] = useState("SET"),
    [key, setKey] = useState("score"),
    [value, setValue] = useState("950"),
    [expected, setExpected] = useState("950"),
    [busy, setBusy] = useState(false),
    [history, setHistory] = useState([]),
    [last, setLast] = useState(null);
  async function send(retry = false) {
    const command =
      retry && last
        ? last
        : {
            operation,
            key,
            value: ["SET", "CAS"].includes(operation) ? value : "",
            expected: operation === "CAS" ? expected : "",
            clientId: crypto.randomUUID(),
            requestId: 1,
          };
    setLast(command);
    setBusy(true);
    try {
      const response = await request("command", command);
      setHistory((h) =>
        [{ command, response, at: new Date().toISOString() }, ...h].slice(
          0,
          12,
        ),
      );
      if (response.status >= 500)
        notify(
          "Outcome may be unknown. Retry with the same request identity.",
          true,
        );
    } catch (e) {
      notify(e.message, true);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="console-grid">
      <div className="panel console-form">
        <SectionTitle
          eyebrow="LEADER-AWARE HTTP CLIENT"
          title="Send a command"
        />
        <div className="operation-picker">
          {["SET", "GET", "CAS", "DELETE"].map((op) => (
            <button
              key={op}
              className={operation === op ? "chosen" : ""}
              onClick={() => setOperation(op)}
            >
              {op}
            </button>
          ))}
        </div>
        <label className="field">
          Key
          <input
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder="e.g. score"
          />
        </label>
        {operation === "CAS" && (
          <label className="field">
            Expected value
            <input
              value={expected}
              onChange={(e) => setExpected(e.target.value)}
            />
          </label>
        )}
        {["SET", "CAS"].includes(operation) && (
          <label className="field">
            {operation === "CAS" ? "New value" : "Value"}
            <textarea
              rows="4"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
          </label>
        )}
        <button
          className="button primary full-width"
          disabled={busy || !key}
          onClick={() => send()}
        >
          {busy ? (
            <LoaderCircle size={16} className="spinning" />
          ) : (
            <ArrowRight size={16} />
          )}{" "}
          {busy ? "Waiting for a committed result…" : "Execute " + operation}
        </button>
        {last && (
          <button
            className="button secondary full-width"
            disabled={busy}
            onClick={() => send(true)}
          >
            <RotateCcw size={14} />
            Retry previous request
          </button>
        )}
        <div className="console-note">
          <ShieldCheck size={18} />
          <p>
            Writes wait for a majority commit. Reads wait for a new quorum
            barrier. Retries keep the original client and request IDs.
          </p>
        </div>
      </div>
      <div className="panel console-output">
        <SectionTitle
          title="Response history"
          action={<span className="mono small muted">LIVE REQUESTS</span>}
        />
        {history.length ? (
          history.map((h, i) => (
            <div className="response-item" key={h.at}>
              <div className="response-header">
                <span
                  className={`operation ${h.command.operation.toLowerCase()}`}
                >
                  {h.command.operation}
                </span>
                <code>{h.command.key}</code>
                <span className={h.response.status < 300 ? "green" : "amber"}>
                  HTTP {h.response.status}
                </span>
                <time>{fmt(h.response.latencyMs, 1)} ms</time>
              </div>
              <pre>{JSON.stringify(h.response.body, null, 2)}</pre>
              <div className="response-meta">
                <span>{h.response.server || "No leader response"}</span>
                <span>{time(h.at)}</span>
              </div>
            </div>
          ))
        ) : (
          <div className="sim-empty">
            <SquareTerminal size={38} />
            <h3>Your first command starts here.</h3>
            <p>Set a value, compare-and-set it, then read it back.</p>
          </div>
        )}
      </div>
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
