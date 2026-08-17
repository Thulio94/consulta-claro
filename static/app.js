const state = {
  settings: { threads: 2, requests_per_second: 2, per_robot_delay_ms: 1000, adaptive: true, confirmation_text: "CONSULTAR API REAL" },
  jobId: null,
  recordPage: 1,
  recordPages: 1,
  eventSource: null,
};

const $ = (id) => document.getElementById(id);
const fmt = (value) => new Intl.NumberFormat("pt-BR").format(Number(value || 0));
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));

async function api(url, options = {}) {
  const response = await fetch(url, options);
  let body = null;
  try { body = await response.json(); } catch { body = {}; }
  if (!response.ok) throw new Error(body.detail || `Erro HTTP ${response.status}`);
  return body;
}

function toast(message, error = false) {
  const el = $("toast");
  el.textContent = message;
  el.className = `toast show${error ? " error" : ""}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.className = "toast"; }, 3200);
}

function duration(seconds) {
  if (seconds == null || !Number.isFinite(Number(seconds))) return "—";
  const value = Math.max(0, Math.round(Number(seconds)));
  const h = Math.floor(value / 3600);
  const m = Math.floor((value % 3600) / 60);
  const s = value % 60;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}

document.querySelectorAll(".tab").forEach(button => {
  button.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(item => item.classList.toggle("active", item === button));
    document.querySelectorAll(".view").forEach(view => view.classList.toggle("active", view.id === button.dataset.target));
    if (button.dataset.target === "dashboard") loadDashboard();
    if (button.dataset.target === "results") loadRecords();
    if (button.dataset.target === "export") loadExportOptions();
  });
});

async function health() {
  try {
    const result = await api("/api/health");
    $("serverDot").className = "status-dot ok";
    $("serverStatus").textContent = result.database ? "Servidor pronto" : "Banco ausente";
  } catch {
    $("serverDot").className = "status-dot error";
    $("serverStatus").textContent = "Servidor indisponível";
  }
}

async function loadDashboard() {
  try {
    const data = await api("/api/dashboard/summary");
    const c = data.counts || {};
    $("mTotal").textContent = fmt(data.total);
    $("mConsulted").textContent = fmt(c.CONSULTADO);
    $("mPending").textContent = fmt(c.PENDENTE);
    $("mRetry").textContent = fmt(c.AGUARDANDO_RETRY);
    $("mErrors").textContent = fmt((c.ERRO_PERMANENTE || 0) + (c.ERRO || 0));
    $("mTech").textContent = fmt(data.technologies);
    $("mStructured").textContent = fmt(data.structured);
    if (data.latest_job) {
      $("lastJobStatus").textContent = `${data.latest_job.status} · ${fmt(data.latest_job.processados)} / ${fmt(data.latest_job.total)}`;
      $("lastJobMessage").textContent = data.latest_job.mensagem || "Sem mensagem.";
    }
  } catch (error) { toast(error.message, true); }
}

function updateRpsEstimate() {
  const threads = Number($("massThreads").value);
  const rps = Math.max(0.1, Number($("massRps").value || 0.1));
  const robotDelay = Math.max(0, Number($("massRobotDelay").value || 0));
  $("massThreadsValue").textContent = threads;
  $("estimatedRps").textContent = `${rps.toLocaleString("pt-BR", {maximumFractionDigits: 2})} req/s`;
  $("riskNotice").textContent = robotDelay
    ? `Cada robô aguardará pelo menos ${robotDelay.toLocaleString("pt-BR")} ms antes da próxima consulta.`
    : rps > 10
      ? "Taxa alta definida pelo usuário. Mantenha a proteção adaptativa ativa."
      : "A proteção adaptativa reduzirá este teto se ocorrer HTTP 429.";
  if (!state.jobId) renderRobotPlaceholders(threads);
}

function renderRobotPlaceholders(count) {
  $("robotGrid").innerHTML = Array.from({length: count}, (_, index) => robotCard({
    id: index + 1, name: `Robô ${index + 1}`, state: "DISPONÍVEL", cep: "", numero: "",
    summary: "Aguardando início", successes: 0, errors: 0, http_429: 0,
    elapsed_seconds: 0, http_status: null, key: ""
  })).join("");
}

function robotCard(robot) {
  const className = String(robot.state || "").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  const target = robot.cep ? `${escapeHtml(robot.cep)} · Nº ${escapeHtml(robot.numero)}` : "Nenhum endereço atribuído";
  return `<article class="robot-card ${className}">
    <div class="robot-top"><span class="robot-name">${escapeHtml(robot.name)}</span><span class="robot-state">${escapeHtml(robot.state)}</span></div>
    <div class="robot-target">${target}</div>
    <div class="robot-summary">${escapeHtml(robot.summary || "Aguardando")}</div>
    <div class="robot-technology">Última tecnologia: <strong>${escapeHtml(robot.technology || "—")}</strong></div>
    <div class="robot-stats">
      <div><span>Sucessos</span><strong>${fmt(robot.successes)}</strong></div>
      <div><span>Erros</span><strong>${fmt(robot.errors)}</strong></div>
      <div><span>429</span><strong>${fmt(robot.http_429)}</strong></div>
    </div>
    <div class="robot-foot">
      <span>${robot.http_status ? `HTTP ${robot.http_status}` : "SEM HTTP"}</span>
      <span>${duration(robot.elapsed_seconds)}</span>
      <span>${escapeHtml(robot.key || "chave —")}</span>
    </div>
  </article>`;
}

function applySnapshot(snapshot) {
  const job = snapshot.job || null;
  const robots = snapshot.robots || [];
  if (job) {
    state.jobId = job.id;
    $("jobBadge").textContent = job.status;
    $("jProcessed").textContent = fmt(job.processados);
    $("jTotal").textContent = `de ${fmt(job.total)}`;
    $("jSuccess").textContent = fmt(job.sucessos);
    $("jErrors").textContent = fmt(job.erros);
    $("j429").textContent = fmt(job.http_429);
    const rate = job.processados ? (job.sucessos / job.processados * 100) : 0;
    $("jSuccessRate").textContent = `${rate.toLocaleString("pt-BR", {maximumFractionDigits: 1})}%`;
    const active = ["EXECUTANDO", "PAUSADO", "CRIADO"].includes(job.status);
    $("startJob").disabled = active;
    $("pauseJob").disabled = job.status !== "EXECUTANDO";
    $("resumeJob").disabled = job.status !== "PAUSADO";
    $("cancelJob").disabled = !active;
  }
  if (robots.length) $("robotGrid").innerHTML = robots.map(robotCard).join("");
  $("jRps").textContent = snapshot.runtime?.rps ?? 0;
  $("jRemaining").textContent = fmt(snapshot.runtime?.remaining ?? 0);
  $("jAverage").textContent = `${fmt(snapshot.runtime?.average_latency_ms ?? 0)} ms`;
  $("jEta").textContent = duration(snapshot.runtime?.eta_seconds);
  const limiter = snapshot.api?.limiter;
  $("jCooldown").textContent = limiter?.cooldown_seconds ? `${limiter.cooldown_seconds}s de resfriamento` : "sem resfriamento";
  $("adaptiveBanner").classList.toggle("hidden", !limiter?.adaptive_reduced);
  if (limiter?.adaptive_reduced) {
    $("adaptiveBanner").textContent =
      `Proteção adaptativa ativa: ${Number(limiter.effective_rps).toLocaleString("pt-BR")} req/s ` +
      `de ${Number(limiter.ceiling_rps).toLocaleString("pt-BR")} req/s configuradas.`;
  }
}

function connectEvents() {
  if (state.eventSource) state.eventSource.close();
  state.eventSource = new EventSource("/api/events");
  state.eventSource.onmessage = event => {
    try { applySnapshot(JSON.parse(event.data)); } catch {}
  };
  state.eventSource.onerror = () => {
    $("serverDot").className = "status-dot error";
    $("serverStatus").textContent = "Reconectando";
  };
  state.eventSource.onopen = () => {
    $("serverDot").className = "status-dot ok";
    $("serverStatus").textContent = "Servidor pronto";
  };
}

async function loadSettings() {
  try {
    const data = await api("/api/settings");
    state.settings = data;
    $("massThreads").value = data.threads;
    $("massRps").value = data.requests_per_second;
    $("massRobotDelay").value = data.per_robot_delay_ms;
    $("massAdaptive").checked = data.adaptive;
    $("settingsThreads").value = data.threads;
    $("settingsRps").value = data.requests_per_second;
    $("settingsRobotDelay").value = data.per_robot_delay_ms;
    $("settingsAdaptive").checked = data.adaptive;
    $("keyList").innerHTML = data.keys.map(key => `<code>${escapeHtml(key)}</code>`).join("");
    updateRpsEstimate();
  } catch (error) { toast(error.message, true); }
}

$("massThreads").addEventListener("input", updateRpsEstimate);
$("massRps").addEventListener("input", updateRpsEstimate);
$("massRobotDelay").addEventListener("input", updateRpsEstimate);
$("refreshDashboard").addEventListener("click", loadDashboard);

$("startJob").addEventListener("click", async () => {
  if (!confirm("Esta ação utilizará a API real. Confirma o início da consulta massiva?")) return;
  try {
    const job = await api("/api/jobs", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        threads: Number($("massThreads").value),
        requests_per_second: Number($("massRps").value),
        per_robot_delay_ms: Number($("massRobotDelay").value),
        adaptive: $("massAdaptive").checked,
        include_retry: $("includeRetry").checked,
        confirmation: state.settings.confirmation_text,
      })
    });
    state.jobId = job.id;
    toast("Consulta massiva real iniciada.");
  } catch (error) { toast(error.message, true); }
});

for (const [id, action] of [["pauseJob","pause"],["resumeJob","resume"],["cancelJob","cancel"]]) {
  $(id).addEventListener("click", async () => {
    if (!state.jobId) return;
    if (action === "cancel" && !confirm("Cancelar imediatamente? Consultas que ainda não foram enviadas serão interrompidas.")) return;
    try {
      await api(`/api/jobs/${state.jobId}/${action}`, {method: "POST"});
      toast(action === "pause" ? "Pausa solicitada." : action === "resume" ? "Consulta retomada." : "Consulta cancelada.");
    } catch (error) { toast(error.message, true); }
  });
}

$("manualButton").addEventListener("click", async () => {
  const cep = $("manualCep").value.trim();
  const numero = $("manualNumero").value.trim();
  if (!cep || !numero) return toast("Informe CEP e número.", true);
  if (!confirm("Executar uma consulta real na API para este endereço?")) return;
  $("manualButton").disabled = true;
  $("manualResult").textContent = "Consultando API real...";
  try {
    const result = await api("/api/manual-query", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({cep, numero, confirmation: state.settings.confirmation_text})
    });
    $("manualResult").textContent = JSON.stringify(result, null, 2);
  } catch (error) {
    $("manualResult").textContent = `Erro: ${error.message}`;
  } finally { $("manualButton").disabled = false; }
});

$("csvFile").addEventListener("change", async event => {
  const file = event.target.files[0];
  if (!file) return;
  const text = await file.slice(0, 8192).text();
  const first = text.split(/\r?\n/, 1)[0];
  const delimiter = [";", ",", "\t", "|"].sort((a,b) => first.split(b).length - first.split(a).length)[0];
  const columns = first.split(delimiter).map(value => value.replace(/^"|"$/g, "").trim()).filter(Boolean);
  for (const [selectId, token] of [["cepColumn","cep"],["numeroColumn","num"]]) {
    const select = $(selectId);
    select.innerHTML = columns.map(column => `<option value="${escapeHtml(column)}">${escapeHtml(column)}</option>`).join("");
    const index = columns.findIndex(column => column.toLowerCase().includes(token));
    if (index >= 0) select.selectedIndex = index;
  }
  $("importButton").disabled = !columns.length;
});

$("importButton").addEventListener("click", async () => {
  const file = $("csvFile").files[0];
  if (!file) return;
  const form = new FormData();
  form.append("file", file);
  form.append("cep_column", $("cepColumn").value);
  form.append("numero_column", $("numeroColumn").value);
  $("importButton").disabled = true;
  $("importResult").textContent = "Importando em blocos...";
  try {
    const result = await api("/api/imports", {method: "POST", body: form});
    $("importResult").className = "inline-result success";
    $("importResult").textContent = `${fmt(result.inserted)} inseridos · ${fmt(result.ignored)} ignorados/duplicados`;
    loadDashboard();
  } catch (error) {
    $("importResult").className = "inline-result error";
    $("importResult").textContent = error.message;
  } finally { $("importButton").disabled = false; }
});

async function loadRecords() {
  const params = new URLSearchParams({
    status: $("recordStatus").value,
    search: $("recordSearch").value.trim(),
    page: state.recordPage,
    page_size: 100,
  });
  try {
    const data = await api(`/api/records?${params}`);
    state.recordPages = data.pages;
    $("recordRows").innerHTML = data.items.map(row => `<tr>
      <td>${row.id}</td><td>${escapeHtml(row.cep)}</td><td>${escapeHtml(row.numero)}</td>
      <td class="status-cell">${escapeHtml(row.status)}</td><td>${escapeHtml(row.uf || "")}</td>
      <td>${escapeHtml(row.cidade || "")}</td><td>${escapeHtml(row.status_resultado || row.ultimo_erro || "")}</td>
      <td>${row.ultimo_http || ""}</td></tr>`).join("");
    $("pageLabel").textContent = `Página ${data.page} de ${data.pages} · ${fmt(data.total)} registros`;
    $("prevPage").disabled = data.page <= 1;
    $("nextPage").disabled = data.page >= data.pages;
  } catch (error) { toast(error.message, true); }
}
$("searchRecords").addEventListener("click", () => { state.recordPage = 1; loadRecords(); });
$("prevPage").addEventListener("click", () => { if (state.recordPage > 1) { state.recordPage--; loadRecords(); } });
$("nextPage").addEventListener("click", () => { if (state.recordPage < state.recordPages) { state.recordPage++; loadRecords(); } });

async function loadExportOptions() {
  try {
    const data = await api("/api/results/options");
    const fill = (id, values, first) => {
      $(id).innerHTML = `<option value="">${first}</option>` + values.map(value => `<option>${escapeHtml(value)}</option>`).join("");
    };
    fill("exportUf", data.ufs, "Todas");
    fill("exportCidade", data.cidades, "Todas");
    fill("exportTech", data.tecnologias, "Todas");
    fill("exportStatus", data.status_resultados, "Todos");
  } catch (error) { toast(error.message, true); }
}

$("exportButton").addEventListener("click", async () => {
  $("exportButton").disabled = true;
  $("exportResult").textContent = "Preparando exportação...";
  try {
    const created = await api("/api/exports", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        uf: $("exportUf").value || null, cidade: $("exportCidade").value || null,
        tecnologia: $("exportTech").value || null, status_resultado: $("exportStatus").value || null,
      })
    });
    const poll = setInterval(async () => {
      try {
        const result = await api(`/api/exports/${created.id}`);
        $("exportResult").textContent = `${result.status} · ${fmt(result.linhas)} linhas`;
        if (result.status === "CONCLUIDO") {
          clearInterval(poll);
          $("exportResult").innerHTML = `Concluído: ${fmt(result.linhas)} linhas · <a href="${result.download_url}">Baixar CSV</a>`;
          $("exportButton").disabled = false;
        } else if (result.status === "FALHOU") {
          clearInterval(poll); $("exportButton").disabled = false;
          $("exportResult").textContent = result.erro || "Falha na exportação.";
        }
      } catch {}
    }, 1000);
  } catch (error) {
    $("exportResult").textContent = error.message;
    $("exportButton").disabled = false;
  }
});

$("saveSettings").addEventListener("click", async () => {
  try {
    const result = await api("/api/settings", {
      method: "PUT", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        threads: Number($("settingsThreads").value),
        requests_per_second: Number($("settingsRps").value),
        per_robot_delay_ms: Number($("settingsRobotDelay").value),
        adaptive: $("settingsAdaptive").checked,
      })
    });
    state.settings = {...state.settings, ...result};
    $("settingsResult").className = "inline-result success";
    $("settingsResult").textContent = "Configurações salvas.";
    await loadSettings();
  } catch (error) {
    $("settingsResult").className = "inline-result error";
    $("settingsResult").textContent = error.message;
  }
});

async function boot() {
  await health();
  await loadSettings();
  await loadDashboard();
  connectEvents();
  renderRobotPlaceholders(state.settings.threads);
}
boot();
