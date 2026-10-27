import { html, render, useState, useEffect, useRef, useCallback, useMemo } from './vendor/htm-preact.mjs';
import { t, getLang, setLang, locales } from './i18n.js';

const TOKEN_KEY = 'cd.token';
const INTERVALS = [2, 5, 10, 30];
const MAX_WORKERS = 256;
const MAX_PEEK = 100;

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

async function api(method, path, body) {
  const headers = {};
  const token = sessionStorage.getItem(TOKEN_KEY);
  if (token) headers.Authorization = 'Bearer ' + token;
  const init = { method, headers, cache: 'no-store' };
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const res = await fetch(path, init);
  const text = await res.text();
  if (!res.ok) throw new ApiError(res.status, text.trim() || res.statusText);
  return text ? JSON.parse(text) : null;
}

const jobPath = (id, suffix = '') => '/api/jobs/' + encodeURIComponent(id) + suffix;

const locale = () => locales[getLang()];

const fmt = (n) => (n == null ? '–' : n.toLocaleString(locale()));

function fmtRate(r) {
  if (r == null) return '';
  if (r === 0) return '0/s';
  return (r < 10 ? r.toFixed(1) : Math.round(r).toLocaleString(locale())) + '/s';
}

function pretty(body) {
  try {
    return JSON.stringify(JSON.parse(body), null, 2);
  } catch {
    return body;
  }
}

function clampInt(v, min, max) {
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) return min;
  return Math.min(max, Math.max(min, n));
}

// ---------- toasts ----------

function useToasts() {
  const [toasts, setToasts] = useState([]);
  const seq = useRef(0);
  const push = useCallback((kind, text) => {
    const id = ++seq.current;
    setToasts((ts) => [...ts, { id, kind, text }]);
    setTimeout(() => setToasts((ts) => ts.filter((x) => x.id !== id)), kind === 'error' ? 8000 : 4000);
  }, []);
  return [toasts, push];
}

function Toasts({ toasts }) {
  return html`<div class="toasts">
    ${toasts.map((x) => html`<div key=${x.id} class=${'toast ' + x.kind}>${x.text}</div>`)}
  </div>`;
}

// ---------- modal ----------

function Modal({ title, onClose, children, wide }) {
  useEffect(() => {
    const onKey = (e) => e.key === 'Escape' && onClose();
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);
  return html`<div class="backdrop" onClick=${(e) => e.target === e.currentTarget && onClose()}>
    <div class=${'modal' + (wide ? ' wide' : '')} role="dialog" aria-modal="true">
      <div class="modal-head">
        <h3>${title}</h3>
        <button class="icon" onClick=${onClose} aria-label=${t('common.close')}>×</button>
      </div>
      <div class="modal-body">${children}</div>
    </div>
  </div>`;
}

// confirm = { title, message, typed?, danger?, action }. Title and message
// are functions so they follow a language switch while the dialog is open.
// action rejects to keep the dialog open; errors are reported by the caller.
function ConfirmDialog({ confirm, onClose }) {
  const [typed, setTyped] = useState('');
  const [busy, setBusy] = useState(false);
  const ok = !confirm.typed || typed === confirm.typed;
  const run = async () => {
    setBusy(true);
    try {
      await confirm.action();
      onClose();
    } catch {
      setBusy(false);
    }
  };
  return html`<${Modal} title=${confirm.title()} onClose=${onClose}>
    <p class="confirm-msg">${confirm.message()}</p>
    ${confirm.typed && html`<label class="field">
      <span>${t('confirm.typeBefore')} <code>${confirm.typed}</code> ${t('confirm.typeAfter')}</span>
      <input value=${typed} onInput=${(e) => setTyped(e.target.value)} autofocus />
    </label>`}
    <div class="actions">
      <button onClick=${onClose}>${t('common.cancel')}</button>
      <button class=${confirm.danger ? 'danger' : 'primary'} disabled=${!ok || busy} onClick=${run}>
        ${busy ? t('common.running') : t('common.confirm')}
      </button>
    </div>
  <//>`;
}

function TokenDialog({ onClose, onSaved }) {
  const [value, setValue] = useState(sessionStorage.getItem(TOKEN_KEY) || '');
  const save = (e) => {
    e.preventDefault();
    if (value.trim()) sessionStorage.setItem(TOKEN_KEY, value.trim());
    else sessionStorage.removeItem(TOKEN_KEY);
    onSaved();
  };
  return html`<${Modal} title="Admin Token" onClose=${onClose}>
    <form onSubmit=${save}>
      <label class="field">
        <span>${t('token.label')}</span>
        <input type="password" value=${value} onInput=${(e) => setValue(e.target.value)} autofocus autocomplete="off" />
      </label>
      <p class="hint">${t('token.hint')}</p>
      <div class="actions">
        <button type="button" onClick=${() => { setValue(''); sessionStorage.removeItem(TOKEN_KEY); onSaved(); }}>${t('token.clear')}</button>
        <button type="submit" class="primary">${t('common.save')}</button>
      </div>
    </form>
  <//>`;
}

function ChangeList({ label, items }) {
  if (!items || items.length === 0) return null;
  return html`<div class="change"><b>${label}</b> ${items.map((i) => html`<code key=${i}>${i}</code>`)}</div>`;
}

function ReloadDialog({ onClose, notify, onDone }) {
  const [preview, setPreview] = useState(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api('POST', '/api/reload?dry_run=true').then(setPreview, (e) => setError(e.message));
  }, []);

  const apply = async () => {
    setBusy(true);
    try {
      await api('POST', '/api/reload');
      notify('ok', t('reload.done'));
      onDone();
      onClose();
    } catch (e) {
      setError(e.message);
      setBusy(false);
    }
  };

  const j = preview?.jobs;
  const nothing = j && !j.added?.length && !j.removed?.length && !j.updated?.length &&
    !j.scaled?.length && !j.recreated?.length && !preview.settings?.length && !preview.restart_required?.length;

  return html`<${Modal} title=${t('reload.title')} onClose=${onClose}>
    ${error && html`<div class="alert error"><pre>${error}</pre></div>`}
    ${!preview && !error && html`<p class="muted">${t('reload.previewing')}</p>`}
    ${preview && html`<div class="changes">
      <p class="muted">${t('reload.diff')}</p>
      <${ChangeList} label=${t('reload.added')} items=${j.added} />
      <${ChangeList} label=${t('reload.removed')} items=${j.removed} />
      <${ChangeList} label=${t('reload.updated')} items=${j.updated} />
      <${ChangeList} label=${t('reload.scaled')} items=${j.scaled} />
      <${ChangeList} label=${t('reload.recreated')} items=${j.recreated} />
      <${ChangeList} label=${t('reload.settings')} items=${preview.settings} />
      <${ChangeList} label=${t('reload.restartRequired')} items=${preview.restart_required} />
      <div class="change"><b>${t('reload.unchanged')}</b> ${t('reload.unchangedCount', { n: j.unchanged })}</div>
      ${nothing && html`<p>${t('reload.nothing')}</p>`}
      ${j.recreated?.length > 0 && html`<div class="alert warn">${t('reload.recreatedWarn')}</div>`}
    </div>`}
    <div class="actions">
      <button onClick=${onClose}>${t('common.cancel')}</button>
      <button class="primary" disabled=${!preview || busy} onClick=${apply}>${busy ? t('reload.applying') : t('reload.apply')}</button>
    </div>
  <//>`;
}

// ---------- job detail ----------

function Section({ title, children }) {
  return html`<section class="section"><h4>${title}</h4>${children}</section>`;
}

function SettingsTab({ job, act }) {
  const [workers, setWorkers] = useState(job.workers);
  const [url, setUrl] = useState(job.url);
  const [patterns, setPatterns] = useState(job.block_patterns);
  const [discard, setDiscard] = useState(job.discard_match_jobs);

  // Resync the form when another change (or reload) updates the job.
  useEffect(() => setWorkers(job.workers), [job.workers]);
  useEffect(() => setUrl(job.url), [job.url]);
  useEffect(() => setPatterns(job.block_patterns), [job.block_patterns]);
  useEffect(() => setDiscard(job.discard_match_jobs), [job.discard_match_jobs]);

  const id = job.logic_name;
  const auto = job.autoscale.enabled;
  const setCount = (n) => act(auto ? t('settings.workersDoneAuto', { id, n }) : t('settings.workersDone', { id, n }),
    () => api('POST', jobPath(id, '/workers'), { count: n }));

  return html`<div>
    <${Section} title="Workers">
      <div class="row">
        <input type="number" min="0" max=${MAX_WORKERS} value=${workers}
          onInput=${(e) => setWorkers(e.target.value)} class="narrow" />
        <button class="primary" onClick=${() => setCount(clampInt(workers, 0, MAX_WORKERS))}>${t('settings.set')}</button>
        ${job.workers > 0
          ? html`<button onClick=${() => setCount(0)}>${t('settings.pause')}</button>`
          : !auto && html`<span class="badge warn">${t('badge.paused')}</span>`}
      </div>
      <p class="hint">${t('settings.workersHint', { max: MAX_WORKERS })}${auto ? ' ' + t('settings.workersHintAuto') : ''}</p>
    <//>
    <${AutoscaleSection} job=${job} act=${act} />
    <${Section} title="Consumer URL">
      <div class="row">
        <input value=${url} onInput=${(e) => setUrl(e.target.value)} class="grow" />
        <button class="primary" disabled=${url === job.url || !url}
          onClick=${() => act(t('settings.urlDone', { id }), () => api('POST', jobPath(id, '/url'), { url }))}>${t('common.save')}</button>
      </div>
    <//>
    <${Section} title="Block patterns">
      <div class="row">
        <input value=${patterns} onInput=${(e) => setPatterns(e.target.value)} class="grow mono" placeholder=${t('settings.patternsPlaceholder')} />
        <button class="primary" disabled=${patterns === job.block_patterns}
          onClick=${() => act(t('settings.patternsDone', { id }), () => api('POST', jobPath(id, '/block-patterns'), { patterns }))}>${t('common.save')}</button>
      </div>
      <label class="check">
        <input type="checkbox" checked=${discard} onChange=${(e) => setDiscard(e.target.checked)} />
        ${t('settings.discard')}
        <button class="primary small" disabled=${discard === job.discard_match_jobs}
          onClick=${() => act(t('settings.discardDone', { id }), () => api('POST', jobPath(id, '/discard-match'), { discard }))}>${t('common.save')}</button>
      </label>
    <//>
    <p class="hint">${t('settings.memoryNote')}</p>
  </div>`;
}

const AUTOSCALE_FIELDS = [
  ['min', 0, MAX_WORKERS],
  ['max', 0, MAX_WORKERS],
  ['messages_per_worker', 1, 1e9],
  ['interval_seconds', 1, 86400],
  ['scale_down_delay_seconds', 0, 86400],
  ['max_step_up', 0, MAX_WORKERS],
];

function AutoscaleSection({ job, act }) {
  const [form, setForm] = useState(job.autoscale);
  // Resync when the job's settings change elsewhere (manual workers, reload).
  const key = JSON.stringify(job.autoscale);
  useEffect(() => setForm(job.autoscale), [key]);

  const id = job.logic_name;
  const set = (k, v) => setForm({ ...form, [k]: v });
  const dirty = JSON.stringify(form) !== key;
  const save = () => {
    const body = { enabled: form.enabled, max_failure_ratio: Number(form.max_failure_ratio) };
    for (const [k, lo, hi] of AUTOSCALE_FIELDS) body[k] = clampInt(form[k], lo, hi);
    act(t('autoscale.done', { id }), () => api('POST', jobPath(id, '/autoscale'), body));
  };

  return html`<${Section} title="Autoscale">
    <label class="check">
      <input type="checkbox" checked=${form.enabled} onChange=${(e) => set('enabled', e.target.checked)} />
      ${t('autoscale.enabled')}
    </label>
    <div class="row wrap">
      ${AUTOSCALE_FIELDS.map(([k, lo, hi]) => html`<label key=${k} class="inline" title=${t('autoscale.help.' + k)}>
        ${t('autoscale.' + k)}
        <input type="number" min=${lo} max=${hi} value=${form[k]} onInput=${(e) => set(k, e.target.value)} class="narrow" />
      </label>`)}
      <label class="inline" title=${t('autoscale.help.max_failure_ratio')}>
        ${t('autoscale.max_failure_ratio')}
        <input type="number" min="0.01" max="1" step="0.05" value=${form.max_failure_ratio}
          onInput=${(e) => set('max_failure_ratio', e.target.value)} class="narrow" />
      </label>
    </div>
    <div class="row">
      <button class="primary" disabled=${!dirty} onClick=${save}>${t('common.save')}</button>
    </div>
    <p class="hint">${t('autoscale.hint')}</p>
  <//>`;
}

function MessageList({ result }) {
  if (!result) return null;
  return html`<div class="messages">
    <div class="muted">${t('msg.resultCount', { label: t(result.labelKey), n: result.messages.length })}</div>
    ${result.messages.length === 0 && html`<div class="empty">${t('msg.empty')}</div>`}
    ${result.messages.map((m, i) => html`<details key=${i} open=${result.messages.length <= 3}>
      <summary>#${i + 1} <span class="mono preview">${m.slice(0, 120)}</span></summary>
      <pre>${pretty(m)}</pre>
    </details>`)}
  </div>`;
}

function MessagesTab({ job, act, ask }) {
  const [count, setCount] = useState(10);
  const [result, setResult] = useState(null);
  const id = job.logic_name;
  const n = () => clampInt(count, 1, MAX_PEEK);

  const read = async (labelKey, method, suffix) => {
    const res = await api(method, jobPath(id, suffix + '?count=' + n()));
    setResult({ labelKey, messages: res.messages || [] });
  };

  const d = job.depth;
  const { queue, dlq, workers } = job;
  return html`<div>
    <div class="depth-cards">
      <div class="card"><span>${t('msg.mainQueue')} <code>${queue}</code></span><b>${fmt(d?.queue.messages)}</b>
        ${d?.queue.error && html`<small class="err" title=${d.queue.error}>${t('common.queryFailed')}</small>`}</div>
      <div class="card"><span>DLQ <code>${dlq}</code></span><b class=${d?.dlq.messages > 0 ? 'warn' : ''}>${fmt(d?.dlq.messages)}</b>
        ${d?.dlq.error && html`<small class="err" title=${d.dlq.error}>${t('common.queryFailed')}</small>`}</div>
    </div>
    <${Section} title=${t('msg.inspect')}>
      <div class="row">
        <label class="inline">${t('msg.count')} <input type="number" min="1" max=${MAX_PEEK} value=${count}
          onInput=${(e) => setCount(e.target.value)} class="narrow" /></label>
      </div>
      <div class="row wrap">
        <button onClick=${() => act(null, () => read('msg.peekMainLabel', 'GET', '/peek'))}>${t('msg.peekMain')}</button>
        <button onClick=${() => act(null, () => read('msg.peekDlqLabel', 'GET', '/dlq'))}>${t('msg.peekDlq')}</button>
        <button class="warn" onClick=${() => {
          const k = n();
          ask({
            title: () => t('msg.takeTitle'), danger: true,
            message: () => t('msg.takeConfirm', { queue, n: k }),
            action: () => read('msg.takenLabel', 'POST', '/take'),
          });
        }}>${t('msg.take')}</button>
        <button class="warn" onClick=${() => {
          const k = n();
          ask({
            title: () => t('msg.replay'),
            message: () => t('msg.replayConfirm', { dlq, queue, n: k }),
            action: async () => {
              const r = await api('POST', jobPath(id, '/replay-dlq'), { count: k });
              return t('msg.replayDone', { n: r.replayed });
            },
          });
        }}>${t('msg.replay')}</button>
      </div>
      <p class="hint">${t('msg.peekHint', { max: MAX_PEEK })}</p>
    <//>
    <${Section} title=${t('msg.purge')}>
      <div class="row wrap">
        <button class="danger" onClick=${() => ask({
          title: () => t('msg.purgeMain'), danger: true, typed: id,
          message: () => t('msg.purgeMainConfirm', { queue }) + (workers > 0 ? t('msg.stillConsuming', { n: workers }) : ''),
          action: async () => {
            const r = await api('POST', jobPath(id, '/purge'));
            return t('msg.purgeMainDone', { n: r.purged });
          },
        })}>${t('msg.purgeMain')}</button>
        <button class="danger" onClick=${() => ask({
          title: () => t('msg.purgeDlq'), danger: true, typed: id,
          message: () => t('msg.purgeDlqConfirm', { dlq }),
          action: async () => {
            const r = await api('POST', jobPath(id, '/purge-dlq'));
            return t('msg.purgeDlqDone', { n: r.purged });
          },
        })}>${t('msg.purgeDlq')}</button>
      </div>
      <p class="hint">${t('msg.purgeHint')}</p>
    <//>
    <${MessageList} result=${result} />
  </div>`;
}

function TryTab({ job, act }) {
  const [body, setBody] = useState('');
  const [result, setResult] = useState(null);
  const send = () => act(null, async () => {
    setResult(null);
    const r = await api('POST', jobPath(job.logic_name, '/try'), { body });
    setResult(r);
  });
  return html`<div>
    <${Section} title=${t('try.title')}>
      <p class="hint">${t('try.hintBefore')}<code>${job.url}</code>${t('try.hintAfter')}</p>
      <textarea rows="10" class="mono" value=${body} onInput=${(e) => setBody(e.target.value)} placeholder=${t('try.placeholder')}></textarea>
      <div class="row"><button class="primary" onClick=${send}>${t('try.send')}</button></div>
    <//>
    ${result && html`<${Section} title=${result.error ? t('try.failed') : t('try.ok')}>
      ${result.error && html`<div class="alert error">${result.error}</div>`}
      <pre>${pretty(result.result ?? '')}</pre>
    <//>`}
  </div>`;
}

function JobDetail({ job, onClose, act, ask }) {
  const [tab, setTab] = useState('settings');
  const tabs = ['settings', 'messages', 'try'];
  return html`<aside class="drawer">
    <div class="drawer-head">
      <div>
        <h2>${job.logic_name}</h2>
        <div class="muted">server <code>${job.server}</code> · queue <code>${job.queue}</code></div>
      </div>
      <button class="icon" onClick=${onClose} aria-label=${t('common.close')}>×</button>
    </div>
    <dl class="facts">
      <div><dt>Workers</dt><dd>${job.workers}</dd></div>
      <div><dt>Prefetch</dt><dd>${job.prefetch}</dd></div>
      <div><dt>Retry limit</dt><dd>${job.retry_limit}</dd></div>
      <div><dt>Completed</dt><dd>${fmt(job.completed)}</dd></div>
      <div><dt>Failed</dt><dd class=${job.failed > 0 ? 'warn' : ''}>${fmt(job.failed)}</dd></div>
      <div><dt>Consumers</dt><dd>${fmt(job.depth?.queue.consumers)}</dd></div>
      <div><dt>DLQ TTL</dt><dd>${job.dlq_ttl_days > 0 ? t('facts.dlqTTLDays', { n: job.dlq_ttl_days }) : t('facts.dlqTTLNever')}</dd></div>
    </dl>
    <nav class="tabs">
      ${tabs.map((k) => html`<button key=${k} class=${tab === k ? 'active' : ''} onClick=${() => setTab(k)}>${t('tab.' + k)}</button>`)}
    </nav>
    <div class="drawer-body">
      ${tab === 'settings' && html`<${SettingsTab} job=${job} act=${act} />`}
      ${tab === 'messages' && html`<${MessagesTab} job=${job} act=${act} ask=${ask} />`}
      ${tab === 'try' && html`<${TryTab} job=${job} act=${act} />`}
    </div>
  </aside>`;
}

// ---------- jobs table ----------

function DepthCell({ stats, warnIfPositive }) {
  if (!stats) return html`<td class="num muted">–</td>`;
  if (stats.error) return html`<td class="num"><span class="err" title=${stats.error}>${t('common.error')}</span></td>`;
  return html`<td class=${'num' + (warnIfPositive && stats.messages > 0 ? ' warn' : '')}>${fmt(stats.messages)}</td>`;
}

function JobsTable({ jobs, rates, selected, onSelect }) {
  if (jobs.length === 0) return html`<div class="empty">${t('table.empty')}</div>`;
  return html`<div class="table-wrap"><table>
    <thead><tr>
      <th>Job</th><th>Server</th><th class="num">Workers</th><th class="num">${t('table.backlog')}</th>
      <th class="num">Consumers</th><th class="num">DLQ</th><th class="num">Completed</th><th class="num">Failed</th><th>URL</th>
    </tr></thead>
    <tbody>
      ${jobs.map((j) => {
        const r = rates[j.logic_name];
        return html`<tr key=${j.logic_name} class=${selected === j.logic_name ? 'selected' : ''} onClick=${() => onSelect(j.logic_name)}>
          <td>
            <div class="name">${j.logic_name}</div>
            <div class="sub">
              ${j.queue}
              ${j.workers === 0 && !j.autoscale.enabled && html` <span class="badge warn">${t('badge.paused')}</span>`}
              ${j.autoscale.enabled && html` <span class="badge" title=${t('badge.autoTitle')}>${t('badge.auto', { min: j.autoscale.min, max: j.autoscale.max })}</span>`}
              ${j.block_patterns && html` <span class="badge" title=${j.block_patterns}>${j.discard_match_jobs ? t('badge.blockDiscard') : t('badge.block')}</span>`}
            </div>
          </td>
          <td>${j.server}</td>
          <td class="num">${j.workers}</td>
          <${DepthCell} stats=${j.depth?.queue} />
          <td class="num">${j.depth && !j.depth.queue.error ? j.depth.queue.consumers : '–'}</td>
          <${DepthCell} stats=${j.depth?.dlq} warnIfPositive />
          <td class="num">${fmt(j.completed)}<div class="sub">${fmtRate(r?.completed)}</div></td>
          <td class=${'num' + (r?.failed > 0 ? ' warn' : '')}>${fmt(j.failed)}<div class="sub">${fmtRate(r?.failed)}</div></td>
          <td class="url" title=${j.url}>${j.url}</td>
        </tr>`;
      })}
    </tbody>
  </table></div>`;
}

function Summary({ jobs }) {
  const sum = (f) => jobs.reduce((a, j) => a + (f(j) || 0), 0);
  // Queues that could not be inspected are left out; null when none could.
  const sumQueue = (f) => {
    const ok = jobs.map((j) => j.depth && f(j.depth)).filter((s) => s && !s.error);
    return ok.length ? ok.reduce((a, s) => a + s.messages, 0) : null;
  };
  const depthErrors = jobs.filter((j) => j.depth?.queue.error || j.depth?.dlq.error).length;
  const cards = [
    ['summary.jobs', jobs.length],
    ['summary.workers', sum((j) => j.workers)],
    ['summary.backlog', sumQueue((d) => d.queue)],
    ['summary.dlq', sumQueue((d) => d.dlq), 'warn'],
    ['summary.failed', sum((j) => j.failed), 'warn'],
  ];
  return html`<div class="summary">
    ${cards.map(([key, v, cls]) => html`<div class="card" key=${key}>
      <span>${t(key)}</span><b class=${cls && v > 0 ? cls : ''}>${fmt(v)}</b>
    </div>`)}
    ${depthErrors > 0 && html`<div class="card"><span>${t('summary.depthErrors')}</span><b class="err">${depthErrors}</b></div>`}
  </div>`;
}

// ---------- app ----------

function App() {
  const [lang, setLangState] = useState(getLang());
  const [jobs, setJobs] = useState(null);
  const [rates, setRates] = useState({});
  const [error, setError] = useState(null);
  const [ready, setReady] = useState(null);
  const [updated, setUpdated] = useState(null);
  const [interval, setIntervalSec] = useState(() => Number(localStorage.getItem('cd.interval')) || 5);
  const [paused, setPaused] = useState(false);
  const [filter, setFilter] = useState('');
  const [selected, setSelected] = useState(null);
  const [confirm, setConfirm] = useState(null);
  const [dialog, setDialog] = useState(null); // 'token' | 'reload'
  const [toasts, notify] = useToasts();
  const prev = useRef(null);
  const inFlight = useRef(false);

  const switchLang = () => {
    const next = lang === 'zh' ? 'en' : 'zh';
    setLang(next);
    setLangState(next);
  };

  const refresh = useCallback(async () => {
    if (inFlight.current) return;
    inFlight.current = true;
    try {
      const [list, readyz] = await Promise.all([
        api('GET', '/api/jobs?depth=true'),
        fetch('/readyz', { cache: 'no-store' }).then((r) => r.ok, () => false),
      ]);
      const now = performance.now();
      if (prev.current) {
        const dt = (now - prev.current.t) / 1000;
        const next = {};
        for (const j of list) {
          const p = prev.current.jobs[j.logic_name];
          if (p && dt > 0) {
            next[j.logic_name] = {
              completed: Math.max(0, j.completed - p.completed) / dt,
              failed: Math.max(0, j.failed - p.failed) / dt,
            };
          }
        }
        setRates(next);
      }
      prev.current = { t: now, jobs: Object.fromEntries(list.map((j) => [j.logic_name, j])) };
      setJobs(list);
      setReady(readyz);
      setError(null);
      setUpdated(new Date());
    } catch (e) {
      setError(e);
      if (e.status === 401) setDialog((d) => d || 'token');
    } finally {
      inFlight.current = false;
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  useEffect(() => {
    localStorage.setItem('cd.interval', String(interval));
    if (paused) return undefined;
    const timer = setInterval(() => { if (!document.hidden) refresh(); }, interval * 1000);
    const onVisible = () => { if (!document.hidden) refresh(); };
    document.addEventListener('visibilitychange', onVisible);
    return () => { clearInterval(timer); document.removeEventListener('visibilitychange', onVisible); };
  }, [interval, paused, refresh]);

  // act runs an API call, reports the result and refreshes the list.
  // The action may return a message that replaces okText.
  const act = useCallback(async (okText, fn) => {
    try {
      const msg = await fn();
      const text = typeof msg === 'string' ? msg : okText;
      if (text) notify('ok', text);
      refresh();
    } catch (e) {
      notify('error', e.message);
      if (e.status === 401) setDialog('token');
      throw e;
    }
  }, [notify, refresh]);

  // Swallow errors already reported by act, so event handlers don't log them.
  const safeAct = useCallback((okText, fn) => act(okText, fn).catch(() => {}), [act]);

  const shown = useMemo(() => {
    if (!jobs) return [];
    const q = filter.trim().toLowerCase();
    if (!q) return jobs;
    return jobs.filter((j) => [j.logic_name, j.queue, j.server, j.url].some((s) => s.toLowerCase().includes(q)));
  }, [jobs, filter]);

  const job = jobs?.find((j) => j.logic_name === selected);

  return html`<div class=${'layout' + (job ? ' with-drawer' : '')}>
    <header>
      <h1>Consumer Dispatcher</h1>
      <span class=${'badge ' + (ready == null ? '' : ready ? 'ok' : 'err')}>${ready == null ? '…' : ready ? 'Ready' : 'Not ready'}</span>
      <div class="spacer"></div>
      <input class="search" placeholder=${t('header.filter')} value=${filter} onInput=${(e) => setFilter(e.target.value)} />
      <select value=${interval} onChange=${(e) => setIntervalSec(Number(e.target.value))} title=${t('header.interval')}>
        ${INTERVALS.map((s) => html`<option value=${s}>${s}s</option>`)}
      </select>
      <button onClick=${() => setPaused(!paused)}>${paused ? t('header.resume') : t('header.pause')}</button>
      <button onClick=${refresh}>${t('header.refresh')}</button>
      <button onClick=${() => setDialog('reload')}>${t('header.reload')}</button>
      <button onClick=${() => setDialog('token')}>${t('header.token')}</button>
      <button onClick=${switchLang} title=${t('lang.switchTitle')}>${t('lang.switch')}</button>
    </header>
    <main>
      ${error && html`<div class="alert error">
        ${error.status === 401 ? t('app.authFailed') + ' ' : t('app.loadFailed')}
        <code>${error.message}</code>
      </div>`}
      ${jobs && html`<${Summary} jobs=${jobs} />`}
      ${jobs && html`<${JobsTable} jobs=${shown} rates=${rates} selected=${selected} onSelect=${setSelected} />`}
      ${!jobs && !error && html`<div class="empty">${t('app.loading')}</div>`}
      <footer class="muted">
        ${updated ? t('app.updatedAt', { time: updated.toLocaleTimeString(locale()) }) : ''}
        ${paused ? t('app.refreshPaused') : ''}
        ${t('app.rateNote')}
      </footer>
    </main>
    ${job && html`<${JobDetail} key=${job.logic_name} job=${job} onClose=${() => setSelected(null)} act=${safeAct} ask=${setConfirm} />`}
    ${confirm && html`<${ConfirmDialog} confirm=${{ ...confirm, action: () => act(null, confirm.action) }} onClose=${() => setConfirm(null)} />`}
    ${dialog === 'token' && html`<${TokenDialog} onClose=${() => setDialog(null)} onSaved=${() => { setDialog(null); refresh(); }} />`}
    ${dialog === 'reload' && html`<${ReloadDialog} onClose=${() => setDialog(null)} notify=${notify} onDone=${refresh} />`}
    <${Toasts} toasts=${toasts} />
  </div>`;
}

render(html`<${App} />`, document.getElementById('app'));
