// Drives the demo page: creates jobs, follows each job's SSE timeline, and
// animates the idle meter (time since the upstream last sent anything).

const scenariosEl = document.getElementById("scenarios");
const EDGE_TIMEOUT = Number(scenariosEl.dataset.edgeTimeout);

// Timeline kinds that mean "bytes just arrived from the upstream", so the
// gateway's idle timer starts over.
const UPSTREAM_ACTIVITY = new Set(["info", "step", "heartbeat", "progress", "result"]);
const TIMELINE_KINDS = [
  "status", "info", "step", "heartbeat", "progress", "result", "error", "callback",
];

class ScenarioCard {
  /** @param {HTMLElement} root */
  constructor(root) {
    this.root = root;
    this.mode = root.dataset.mode;
    this.el = Object.fromEntries(
      [...root.querySelectorAll("[data-role]")].map((node) => [node.dataset.role, node]),
    );
    this.source = null;
    this.timer = null;
    this.el["run-one"].addEventListener("click", () => this.start(readForm()));
  }

  /** Reset the card, create a job, and start following its events. */
  async start(request) {
    this.reset();
    this.setStatus("queued");

    let accepted;
    try {
      const response = await fetch("/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...request, mode: this.mode }),
      });
      if (response.status !== 202) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      accepted = await response.json();
    } catch (error) {
      this.setStatus("failed");
      this.showOutcome(false, `Could not create the job: ${error.message}`);
      return;
    }

    this.startedAt = performance.now();
    this.el["job-id"].textContent = accepted.job_id;
    this.append({ elapsed_seconds: 0, kind: "accepted", message: `202 Accepted, job ${accepted.job_id}` });
    this.follow(accepted.events_url);
    // setInterval rather than requestAnimationFrame: rAF pauses in background tabs.
    this.timer = setInterval(() => this.tick(), 100);
  }

  reset() {
    this.source?.close();
    clearInterval(this.timer);
    this.pings = 0;
    this.progress = 0;
    // Times below are in seconds since the job was accepted, as reported by the server.
    this.lastActivity = 0;
    this.lastEvent = 0;
    this.longestSilence = 0;
    this.el["meter-label"].textContent = "Idle time since the last byte";
    this.el.timeline.replaceChildren();
    this.el.outcome.hidden = true;
    this.el.pings.textContent = "0";
    this.el.progress.textContent = "0";
    this.el.elapsed.textContent = "–";
    this.el["job-id"].textContent = "–";
    this.setIdle(0);
  }

  /** Subscribe to the job's timeline over SSE. */
  follow(eventsUrl) {
    const source = new EventSource(eventsUrl);
    this.source = source;

    for (const kind of TIMELINE_KINDS) {
      source.addEventListener(kind, (message) => this.onEvent(JSON.parse(message.data)));
    }
    source.addEventListener("end", () => this.finish());
  }

  onEvent(event) {
    const at = event.elapsed_seconds;
    this.lastEvent = Math.max(this.lastEvent, at);

    // The job failed because the upstream went quiet: that silence lasted until this moment.
    if (event.kind === "error") this.noteSilence(at);
    if (UPSTREAM_ACTIVITY.has(event.kind)) {
      this.noteSilence(at);
      this.lastActivity = Math.max(this.lastActivity, at);
    }
    if (event.kind === "heartbeat") this.el.pings.textContent = String(++this.pings);
    if (event.kind === "progress") this.el.progress.textContent = String(++this.progress);
    if (event.kind === "status") this.setStatus(event.data.status);
    if (event.kind === "result") this.showOutcome(true, event.data.answer, event.data);
    if (event.kind === "error") this.showOutcome(false, event.message);
    if (event.kind === "callback") refreshInbox();

    this.append(event);
  }

  append(event) {
    const row = document.createElement("li");
    row.className = `row row-${event.kind}`;

    const time = document.createElement("span");
    time.className = "row-time";
    time.textContent = `+${event.elapsed_seconds.toFixed(1)}s`;

    const kind = document.createElement("span");
    kind.className = "row-kind";
    kind.textContent = event.kind;

    const message = document.createElement("span");
    message.className = "row-message";
    message.textContent = event.message;

    row.append(time, kind, message);
    const list = this.el.timeline;
    const pinnedToBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 24;
    list.append(row);
    if (pinnedToBottom) list.scrollTop = list.scrollHeight;
  }

  setStatus(status) {
    this.root.dataset.status = status;
    this.el.status.textContent = status;
  }

  showOutcome(ok, text, result) {
    const box = this.el.outcome;
    box.hidden = false;
    box.className = `outcome ${ok ? "outcome-ok" : "outcome-fail"}`;
    box.replaceChildren();

    const title = document.createElement("strong");
    title.textContent = ok ? "Final answer received" : "No answer";
    const body = document.createElement("p");
    body.textContent = text;
    box.append(title, body);

    if (result?.elapsed_seconds !== undefined) {
      const meta = document.createElement("p");
      meta.className = "outcome-meta";
      meta.textContent = `Upstream run ${result.run_id} took ${result.elapsed_seconds}s`;
      box.append(meta);
    }
  }

  setIdle(seconds) {
    const ratio = Math.min(seconds / EDGE_TIMEOUT, 1);
    this.el.idle.textContent = seconds.toFixed(1);
    this.el.fill.style.transform = `scaleX(${ratio})`;
    this.el.meter.dataset.level = ratio >= 1 ? "over" : ratio >= 0.66 ? "high" : "ok";
  }

  noteSilence(at) {
    this.longestSilence = Math.max(this.longestSilence, at - this.lastActivity);
  }

  /** Update the elapsed time and idle meter while the job runs. */
  tick() {
    const elapsed = (performance.now() - this.startedAt) / 1000;
    this.el.elapsed.textContent = `${elapsed.toFixed(1)}s`;
    this.setIdle(Math.max(elapsed - this.lastActivity, 0));
  }

  /** Stop the clocks and show final numbers based on the server's timestamps. */
  finish() {
    this.source?.close();
    clearInterval(this.timer);
    this.el.elapsed.textContent = `${this.lastEvent.toFixed(1)}s`;
    this.el["meter-label"].textContent = "Longest silence from the upstream";
    this.setIdle(this.longestSilence);
  }
}

function readForm() {
  const form = new FormData(document.getElementById("controls"));
  const request = {
    query: form.get("query"),
    duration_seconds: Number(form.get("duration_seconds")),
  };
  if (form.get("use_callback")) request.callback_url = form.get("callback_url");
  return request;
}

async function refreshInbox() {
  const list = document.getElementById("inbox-list");
  let deliveries;
  try {
    deliveries = await (await fetch("/webhook-sink")).json();
  } catch {
    return;
  }
  if (deliveries.length === 0) return;

  list.replaceChildren(
    ...deliveries.map(({ received_at, payload }) => {
      const item = document.createElement("li");
      item.dataset.status = payload.status;

      const head = document.createElement("div");
      head.className = "inbox-meta";
      head.textContent =
        `${new Date(received_at).toLocaleTimeString()} · job ${payload.job_id} · ${payload.mode} · ${payload.status}`;

      const body = document.createElement("pre");
      body.textContent = JSON.stringify(payload.result ?? { error: payload.error }, null, 2);

      item.append(head, body);
      return item;
    }),
  );
}

const cards = [...document.querySelectorAll(".card")].map((node) => new ScenarioCard(node));

document.getElementById("controls").addEventListener("submit", (event) => {
  event.preventDefault();
  const request = readForm();
  for (const card of cards) card.start(request);
});

document.getElementById("refresh-inbox").addEventListener("click", refreshInbox);
refreshInbox();
