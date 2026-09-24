// Orbix Web Control Plane Frontend Application
(function () {
  'use strict';

  // --- State ---
  let state = {
    nodeId: '',
    status: null,
    jobs: [],
    nodes: [],
    queue: { pending: [], in_flight: [] },
    runs: [],
    searchQuery: '',
    editingJobId: null,
    theme: localStorage.getItem('orbix_theme') || 'light'
  };

  // --- DOM Elements ---
  const el = {
    themeBtn: document.getElementById('btn-theme'),
    nodeBadge: document.getElementById('node-badge'),
    leaderChip: document.getElementById('leader-chip'),
    clock: document.getElementById('clock'),
    nodeBanner: document.getElementById('node-banner'),
    
    // Telemetry Stats
    sMembers: document.getElementById('s-members'),
    sPending: document.getElementById('s-pending'),
    sFlight: document.getElementById('s-flight'),
    sRuns: document.getElementById('s-runs'),
    sOk: document.getElementById('s-ok'),

    // Jobs Section
    jobsBody: document.getElementById('jobs-body'),
    jobsCount: document.getElementById('jobs-count'),
    jobsSearch: document.getElementById('jobs-search'),
    btnNew: document.getElementById('btn-new'),
    btnHeroNew: document.getElementById('btn-hero-new'),

    // Cluster Section
    clusterBody: document.getElementById('cluster-body'),
    clusterCount: document.getElementById('cluster-count'),
    btnAddNode: document.getElementById('btn-add-node'),

    // Queue Section
    queueBody: document.getElementById('queue-body'),

    // Runs Section
    runsBody: document.getElementById('runs-body'),
    runsCount: document.getElementById('runs-count'),
    btnRunsRefresh: document.getElementById('btn-runs-refresh'),

    // Footer
    footVer: document.getElementById('foot-ver'),

    // Modal Sheet (Job Form)
    scrim: document.getElementById('scrim'),
    sheet: document.getElementById('sheet'),
    sheetTitle: document.getElementById('sheet-title'),
    btnSheetX: document.getElementById('btn-sheet-x'),
    btnCancel: document.getElementById('btn-cancel'),
    form: document.getElementById('form'),
    fName: document.getElementById('f-name'),
    fCron: document.getElementById('f-cron'),
    fCronHint: document.getElementById('f-cron-hint'),
    fTz: document.getElementById('f-tz'),
    fCommand: document.getElementById('f-command'),
    fTimeout: document.getElementById('f-timeout'),
    fRetries: document.getElementById('f-retries'),
    fCatchup: document.getElementById('f-catchup'),
    fOverlap: document.getElementById('f-overlap'),
    fEnabled: document.getElementById('f-enabled'),

    // Drawer (Run Details)
    drawer: document.getElementById('drawer'),
    btnDrawerX: document.getElementById('btn-drawer-x'),
    drawerBody: document.getElementById('drawer-body'),

    // Toasts
    toast: document.getElementById('toast')
  };

  // --- Theme Initializer ---
  function initTheme() {
    document.documentElement.setAttribute('data-theme', state.theme);
    if (el.themeBtn) {
      el.themeBtn.textContent = state.theme === 'dark' ? 'Light' : 'Dark';
    }
  }

  function toggleTheme() {
    state.theme = state.theme === 'dark' ? 'light' : 'dark';
    localStorage.setItem('orbix_theme', state.theme);
    initTheme();
  }

  // --- Clock ---
  function updateClock() {
    if (!el.clock) return;
    const now = new Date();
    const iso = now.toISOString();
    const timeStr = iso.substring(11, 19) + ' UTC';
    el.clock.textContent = timeStr;
  }

  // --- Toast Notification ---
  function showToast(msg, isError = false) {
    if (!el.toast) return;
    const t = document.createElement('div');
    t.className = `toast ${isError ? 'err' : 'ok'}`;
    t.textContent = msg;
    el.toast.appendChild(t);
    setTimeout(() => {
      t.style.opacity = '0';
      setTimeout(() => t.remove(), 300);
    }, 3500);
  }

  // --- API Utilities ---
  async function apiFetch(url, options = {}) {
    try {
      const res = await fetch(url, options);
      if (res.status === 204) return null; // No Content
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      return data;
    } catch (err) {
      console.warn(`API Error [${url}]:`, err.message);
      throw err;
    }
  }

  // --- Data Fetchers ---
  async function fetchStatus() {
    try {
      const data = await apiFetch('/api/status');
      if (!data) return;
      state.status = data;
      state.nodeId = data.node_id || '';

      if (el.nodeBadge) el.nodeBadge.textContent = state.nodeId || 'unknown';
      if (el.footVer) el.footVer.textContent = `v${data.version || '0.1.0'}`;

      if (el.leaderChip) {
        if (data.is_leader) {
          el.leaderChip.hidden = false;
          el.leaderChip.className = 'badge leader';
          el.leaderChip.textContent = '★ LEADER';
        } else if (data.leader && data.leader.node_id) {
          el.leaderChip.hidden = false;
          el.leaderChip.className = 'badge follower';
          el.leaderChip.textContent = `follower (leader: ${data.leader.node_id})`;
        } else {
          el.leaderChip.hidden = true;
        }
      }

      // Helper to safely convert numbers, arrays, or objects to counts
      function getCount(val) {
        if (typeof val === 'number') return val;
        if (Array.isArray(val)) return val.length;
        if (val && typeof val === 'object') return Object.keys(val).length;
        return 0;
      }

      // Render Telemetry
      if (el.sMembers) el.sMembers.textContent = getCount(data.members);
      if (el.sPending) el.sPending.textContent = getCount(data.pending);
      if (el.sFlight) el.sFlight.textContent = getCount(data.in_flight);

      if (data.run_counts) {
        const okCount = data.run_counts.success || 0;
        const failCount = data.run_counts.failed || 0;
        const timeoutCount = data.run_counts.timeout || 0;
        const totalRuns = okCount + failCount + timeoutCount;

        if (el.sRuns) el.sRuns.textContent = totalRuns;
        if (el.sOk) {
          if (totalRuns > 0) {
            const pct = ((okCount / totalRuns) * 100).toFixed(1);
            el.sOk.textContent = `${pct}%`;
          } else {
            el.sOk.textContent = '100.0%';
          }
        }
      }
    } catch (err) {
      if (el.nodeBadge) el.nodeBadge.textContent = 'offline';
    }
  }

  async function fetchJobs() {
    try {
      const list = await apiFetch('/api/jobs');
      state.jobs = list || [];
      renderJobs();
    } catch (err) {
      state.jobs = [];
      renderJobs();
    }
  }

  async function fetchNodes() {
    try {
      const res = await apiFetch('/api/nodes');
      state.nodes = res ? (res.nodes || []) : [];
      renderNodes(res ? res.docker : false);
    } catch (err) {
      state.nodes = [];
      renderNodes(false);
    }
  }

  async function fetchQueue() {
    try {
      const res = await apiFetch('/api/queue');
      state.queue = res || { pending: [], in_flight: [] };
      renderQueue();
    } catch (err) {
      state.queue = { pending: [], in_flight: [] };
      renderQueue();
    }
  }

  async function fetchRuns() {
    try {
      const res = await apiFetch('/api/runs?limit=50');
      state.runs = res || [];
      renderRuns();
    } catch (err) {
      state.runs = [];
      renderRuns();
    }
  }

  // --- Render Functions ---

  function renderJobs() {
    if (!el.jobsBody) return;
    const q = state.searchQuery.toLowerCase().trim();
    const filtered = state.jobs.filter(j => 
      j.name.toLowerCase().includes(q) || j.command.toLowerCase().includes(q)
    );

    if (el.jobsCount) el.jobsCount.textContent = `(${filtered.length})`;

    if (filtered.length === 0) {
      el.jobsBody.innerHTML = `
        <div class="empty">
          <p>${q ? 'No jobs match your filter query.' : 'No scheduled jobs yet.'}</p>
        </div>`;
      return;
    }

    el.jobsBody.innerHTML = filtered.map(j => {
      const nextStr = j.enabled && j.next_fire ? formatTime(j.next_fire) : null;
      const timeoutVal = j.timeout_s != null ? j.timeout_s : (j.timeout_sec != null ? j.timeout_sec : 60);

      return `
        <div class="jrow ${j.enabled ? '' : 'paused'}" data-id="${j.id}">
          <div class="jtop">
            <span class="jname">${escapeHtml(j.name)}</span>
            <code class="mono dim">${escapeHtml(j.cron)}</code>
            ${nextStr ? `<span class="next">Next: ${nextStr}</span>` : '<span class="chip">disabled</span>'}
            <div class="acts">
              <button class="btn sm primary btn-trigger" data-id="${j.id}">▶ Run</button>
              <button class="btn sm btn-toggle" data-id="${j.id}">${j.enabled ? 'Pause' : 'Enable'}</button>
              <button class="btn sm btn-edit" data-id="${j.id}">Edit</button>
              <button class="btn sm danger btn-delete" data-id="${j.id}">&times;</button>
            </div>
          </div>
          <div class="jmeta">
            <span class="chip">timeout: ${timeoutVal}s</span>
            <span class="chip">retries: ${j.max_retries ?? 2}</span>
            <span class="chip">catchup: ${escapeHtml(j.catch_up || 'run_latest')}</span>
            ${j.no_overlap ? '<span class="chip amber">no-overlap</span>' : ''}
          </div>
          <div class="cmd"><code>${escapeHtml(j.command)}</code></div>
        </div>
      `;
    }).join('');
  }

  function renderNodes(dockerAvailable) {
    if (!el.clusterBody) return;
    if (el.clusterCount) el.clusterCount.textContent = `(${state.nodes.length})`;

    if (el.btnAddNode) {
      el.btnAddNode.disabled = !dockerAvailable;
    }

    if (state.nodes.length === 0) {
      el.clusterBody.innerHTML = `<div class="empty"><p>No active cluster nodes found.</p></div>`;
      return;
    }

    el.clusterBody.innerHTML = state.nodes.map(n => {
      const isSelf = n.node_id === state.nodeId;
      const isLeader = n.leader;
      const stateBadge = n.state === 'running' || n.member ? 'online' : n.state;
      return `
        <div class="mem">
          <span class="dot ${isLeader ? 'lead' : ''}"></span>
          <b>${escapeHtml(n.node_id)}</b>
          ${isSelf ? '<span class="chip">this node</span>' : ''}
          ${isLeader ? '<span class="chip amber">★ Leader</span>' : ''}
          <span class="addr">${escapeHtml(n.addr || n.host_port || ':8080')}</span>
          <span class="ago state-${stateBadge}">${stateBadge}</span>
          ${n.manageable && n.state === 'exited' ? `
            <button class="btn sm primary btn-start-node" data-id="${n.node_id}">Start</button>
          ` : ''}
          ${n.manageable && n.state === 'running' ? `
            <button class="btn sm danger btn-kill-node" data-id="${n.node_id}">Kill</button>
          ` : ''}
        </div>
      `;
    }).join('');
  }

  function renderQueue() {
    if (!el.queueBody) return;
    const pending = state.queue.pending || [];
    const inFlight = state.queue.in_flight || [];

    if (pending.length === 0 && inFlight.length === 0) {
      el.queueBody.innerHTML = `<div class="empty"><p>Queue is empty &mdash; no pending or running fires.</p></div>`;
      return;
    }

    let html = '';

    if (inFlight.length > 0) {
      html += inFlight.map(c => `
        <div class="qrow run">
          <span class="tag">RUNNING</span>
          <b>${escapeHtml(c.job_name || 'job #' + c.job_id)}</b>
          <span class="who">attempt ${c.attempt} on <code>${escapeHtml(c.claimed_by || c.node_id || 'worker')}</code></span>
        </div>
      `).join('');
    }

    if (pending.length > 0) {
      html += pending.map(t => `
        <div class="qrow">
          <span class="tag">QUEUED</span>
          <b>${escapeHtml(t.name || 'job #' + t.job_id)}</b>
          <span class="who">sched ${formatTimeShort(t.sched_at)} (attempt ${t.attempt})</span>
        </div>
      `).join('');
    }

    el.queueBody.innerHTML = html;
  }

  function renderRuns() {
    if (!el.runsBody) return;
    if (el.runsCount) el.runsCount.textContent = `(${state.runs.length})`;

    if (state.runs.length === 0) {
      el.runsBody.innerHTML = `<div class="empty"><p>No run execution logs recorded yet.</p></div>`;
      return;
    }

    el.runsBody.innerHTML = state.runs.map((r, idx) => {
      const dur = r.duration_ms != null ? `${r.duration_ms}ms` : '-';
      const whenStr = formatTime(r.started_at || r.finished_at || r.sched_at);
      const jobName = getJobName(r.job_id);

      return `
        <div class="rrow" data-idx="${idx}">
          <span class="st ${r.status}"><i></i>${r.status}</span>
          <span class="r-job"><strong>${escapeHtml(jobName)}</strong></span>
          <span class="rc-fire mono">${formatTimeShort(r.sched_at)}</span>
          <span class="mono">#${r.attempt}</span>
          <span class="rc-node mono">${escapeHtml(r.node_id || 'node1')}</span>
          <span class="rc-dur mono">${dur}</span>
          <span class="mono">${whenStr}</span>
        </div>
      `;
    }).join('');
  }

  // --- Form & Sheet Controls ---
  function openSheet(job = null) {
    state.editingJobId = job ? job.id : null;
    if (el.sheetTitle) el.sheetTitle.textContent = job ? `Edit Job #${job.id}` : 'New job';

    if (job) {
      el.fName.value = job.name || '';
      el.fCron.value = job.cron || '';
      el.fTz.value = job.tz || job.timezone || '';
      el.fCommand.value = job.command || '';
      el.fTimeout.value = job.timeout_s ?? (job.timeout_sec ?? 60);
      el.fRetries.value = job.max_retries ?? 2;
      el.fCatchup.value = job.catch_up || 'run_latest';
      el.fOverlap.checked = !!job.no_overlap;
      el.fEnabled.checked = job.enabled !== false;
    } else {
      el.form.reset();
      el.fTimeout.value = 60;
      el.fRetries.value = 2;
      el.fCatchup.value = 'run_latest';
      el.fEnabled.checked = true;
    }

    if (el.fCronHint) el.fCronHint.textContent = '';
    previewCron();

    el.scrim.hidden = false;
    el.scrim.classList.add('on');
    el.sheet.setAttribute('aria-hidden', 'false');
    el.sheet.classList.add('open');
    el.fName.focus();
  }

  function closeSheet() {
    el.scrim.hidden = true;
    el.scrim.classList.remove('on');
    el.sheet.setAttribute('aria-hidden', 'true');
    el.sheet.classList.remove('open');
    state.editingJobId = null;
  }

  async function previewCron() {
    if (!el.fCronHint || !el.fCron) return;
    const expr = el.fCron.value.trim();
    if (!expr) {
      el.fCronHint.textContent = '';
      return;
    }
    try {
      const data = await apiFetch(`/api/cron/preview?expr=${encodeURIComponent(expr)}&n=2`);
      if (data && data.next && data.next.length > 0) {
        el.fCronHint.textContent = `Next fire: ${data.next[0]}`;
        el.fCronHint.className = 'hint firehint ok';
      } else if (data && data.error) {
        el.fCronHint.textContent = `Invalid: ${data.error}`;
        el.fCronHint.className = 'hint firehint err';
      }
    } catch (e) {
      el.fCronHint.textContent = '';
    }
  }

  async function handleFormSubmit(e) {
    e.preventDefault();
    const payload = {
      name: el.fName.value.trim(),
      cron: el.fCron.value.trim(),
      tz: el.fTz.value.trim(),
      command: el.fCommand.value.trim(),
      timeout_s: parseInt(el.fTimeout.value, 10) || 60,
      max_retries: parseInt(el.fRetries.value, 10) || 0,
      catch_up: el.fCatchup.value,
      no_overlap: el.fOverlap.checked,
      enabled: el.fEnabled.checked
    };

    if (!payload.name || !payload.cron || !payload.command) {
      showToast('Name, Cron expression, and Command are required.', true);
      return;
    }

    try {
      if (state.editingJobId) {
        await apiFetch(`/api/jobs/${state.editingJobId}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        showToast(`Job "${payload.name}" updated successfully.`);
      } else {
        await apiFetch('/api/jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        showToast(`Job "${payload.name}" scheduled.`);
      }
      closeSheet();
      fetchJobs();
    } catch (err) {
      showToast(`Failed: ${err.message}`, true);
    }
  }

  // --- Run Detail Drawer ---
  function openDrawer(run) {
    if (!el.drawerBody) return;
    const jobName = getJobName(run.job_id);

    el.drawerBody.innerHTML = `
      <div class="kv">
        <dt>Job</dt><dd>${escapeHtml(jobName)} (#${run.job_id})</dd>
        <dt>Run Key</dt><dd>${escapeHtml(run.key)}</dd>
        <dt>Status</dt><dd><span class="st ${run.status}"><i></i>${run.status}</span> (exit: ${run.exit_code ?? 0})</dd>
        <dt>Attempt</dt><dd>#${run.attempt} on <code>${escapeHtml(run.node_id || 'node1')}</code></dd>
        <dt>Scheduled</dt><dd>${formatTime(run.sched_at)}</dd>
        <dt>Started</dt><dd>${formatTime(run.started_at)}</dd>
        <dt>Finished</dt><dd>${formatTime(run.finished_at)}</dd>
        <dt>Duration</dt><dd>${run.duration_ms != null ? run.duration_ms + ' ms' : '-'}</dd>
      </div>

      <div class="well">
        <h4>Stdout</h4>
        <pre><code>${escapeHtml(run.stdout || '(no stdout output)')}</code></pre>
      </div>

      <div class="well">
        <h4>Stderr</h4>
        <pre><code>${escapeHtml(run.stderr || '(no stderr output)')}</code></pre>
      </div>
    `;

    el.drawer.setAttribute('aria-hidden', 'false');
    el.drawer.classList.add('open');
    el.scrim.hidden = false;
    el.scrim.classList.add('on');
  }

  function closeDrawer() {
    el.drawer.setAttribute('aria-hidden', 'true');
    el.drawer.classList.remove('open');
    if (!el.sheet.classList.contains('open')) {
      el.scrim.hidden = true;
      el.scrim.classList.remove('on');
    }
  }

  // --- Event Listeners ---
  function bindEvents() {
    if (el.themeBtn) el.themeBtn.addEventListener('click', toggleTheme);
    if (el.btnNew) el.btnNew.addEventListener('click', () => openSheet());
    if (el.btnHeroNew) el.btnHeroNew.addEventListener('click', () => openSheet());
    if (el.btnSheetX) el.btnSheetX.addEventListener('click', closeSheet);
    if (el.btnCancel) el.btnCancel.addEventListener('click', closeSheet);
    if (el.scrim) el.scrim.addEventListener('click', () => { closeSheet(); closeDrawer(); });
    if (el.form) el.form.addEventListener('submit', handleFormSubmit);

    if (el.fCron) {
      let timer;
      el.fCron.addEventListener('input', () => {
        clearTimeout(timer);
        timer = setTimeout(previewCron, 300);
      });
    }

    if (el.jobsSearch) {
      el.jobsSearch.addEventListener('input', (e) => {
        state.searchQuery = e.target.value;
        renderJobs();
      });
    }

    if (el.jobsBody) {
      el.jobsBody.addEventListener('click', async (e) => {
        const trg = e.target.closest('button');
        if (!trg) return;
        const jobId = trg.dataset.id;
        if (!jobId) return;

        if (trg.classList.contains('btn-trigger')) {
          try {
            await apiFetch(`/api/jobs/${jobId}/trigger`, { method: 'POST' });
            showToast(`Job #${jobId} triggered.`);
            fetchQueue();
          } catch (err) {
            showToast(`Trigger failed: ${err.message}`, true);
          }
        } else if (trg.classList.contains('btn-toggle')) {
          const job = state.jobs.find(j => j.id == jobId);
          if (!job) return;
          const action = job.enabled ? 'disable' : 'enable';
          try {
            await apiFetch(`/api/jobs/${jobId}/${action}`, { method: 'POST' });
            showToast(`Job #${jobId} ${action}d.`);
            fetchJobs();
          } catch (err) {
            showToast(`Action failed: ${err.message}`, true);
          }
        } else if (trg.classList.contains('btn-edit')) {
          const job = state.jobs.find(j => j.id == jobId);
          if (job) openSheet(job);
        } else if (trg.classList.contains('btn-delete')) {
          if (confirm(`Delete job #${jobId}?`)) {
            try {
              await apiFetch(`/api/jobs/${jobId}`, { method: 'DELETE' });
              showToast(`Job #${jobId} deleted.`);
              fetchJobs();
            } catch (err) {
              showToast(`Delete failed: ${err.message}`, true);
            }
          }
        }
      });
    }

    if (el.btnAddNode) {
      el.btnAddNode.addEventListener('click', async () => {
        try {
          const res = await apiFetch('/api/nodes', { method: 'POST', body: JSON.stringify({}) });
          showToast(`Spawned node "${res.node_id}" on port ${res.host_port}`);
          fetchNodes();
        } catch (err) {
          showToast(`Spawn node failed: ${err.message}`, true);
        }
      });
    }

    if (el.clusterBody) {
      el.clusterBody.addEventListener('click', async (e) => {
        const trg = e.target.closest('button');
        if (!trg) return;
        const nodeId = trg.dataset.id;
        if (!nodeId) return;

        if (trg.classList.contains('btn-kill-node')) {
          try {
            await apiFetch(`/api/nodes/${nodeId}/kill`, { method: 'POST' });
            showToast(`Node "${nodeId}" killed.`);
            fetchNodes();
          } catch (err) {
            showToast(`Kill node failed: ${err.message}`, true);
          }
        } else if (trg.classList.contains('btn-start-node')) {
          try {
            await apiFetch(`/api/nodes/${nodeId}/start`, { method: 'POST' });
            showToast(`Node "${nodeId}" started.`);
            fetchNodes();
          } catch (err) {
            showToast(`Start node failed: ${err.message}`, true);
          }
        }
      });
    }

    if (el.runsBody) {
      el.runsBody.addEventListener('click', (e) => {
        const row = e.target.closest('.rrow');
        if (!row) return;
        const idx = parseInt(row.dataset.idx, 10);
        if (!isNaN(idx) && state.runs[idx]) {
          openDrawer(state.runs[idx]);
        }
      });
    }

    if (el.btnRunsRefresh) {
      el.btnRunsRefresh.addEventListener('click', () => {
        fetchRuns();
        showToast('Runs refreshed.');
      });
    }

    if (el.btnDrawerX) {
      el.btnDrawerX.addEventListener('click', closeDrawer);
    }
  }

  // --- Helpers ---
  function getJobName(jobId) {
    const j = state.jobs.find(item => item.id === jobId);
    return j ? j.name : `job #${jobId}`;
  }

  function formatTime(isoStr) {
    if (!isoStr) return '-';
    try {
      const d = new Date(isoStr);
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
    } catch (e) {
      return isoStr;
    }
  }

  function formatTimeShort(isoStr) {
    if (!isoStr) return '-';
    try {
      const d = new Date(isoStr);
      return d.toTimeString().substring(0, 8);
    } catch (e) {
      return isoStr;
    }
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // --- Main Loop ---
  function pollAll() {
    fetchStatus();
    fetchJobs();
    fetchNodes();
    fetchQueue();
    fetchRuns();
  }

  function init() {
    initTheme();
    bindEvents();
    updateClock();
    setInterval(updateClock, 1000);

    pollAll();
    setInterval(pollAll, 1500);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
