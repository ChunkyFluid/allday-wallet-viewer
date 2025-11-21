// public/app.js

const state = {
  wallet: null,
  allRows: [],
  filteredRows: [],
  sort: { key: "last_event_ts", dir: "desc" },
  filters: {
    team: "ALL",
    player: "ALL",
    series: "ALL",
    set: "ALL",
    tier: "ALL",
    position: "ALL",
    locked: "ALL", // ALL | LOCKED | UNLOCKED
  },
};

const sortColumns = {
  playerName: { type: "string" },
  team_name: { type: "string" },
  position: { type: "string" },
  tier: { type: "string" },
  serial_number: { type: "number" },
  max_mint_size: { type: "number" },
  series_name: { type: "string" },
  set_name: { type: "string" },
  is_locked: { type: "boolean" },
  last_event_ts: { type: "date" },
};

function $(id) {
  return document.getElementById(id);
}

function escapeCsv(val) {
  if (val == null) return "";
  const s = String(val);
  if (s.includes('"') || s.includes(",") || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function formatDate(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  return d.toLocaleString();
}

function buildPlayerName(row) {
  const first = row.first_name || "";
  const last = row.last_name || "";
  const combined = `${first} ${last}`.trim();
  if (combined) return combined;
  return row.playerName || ""; // fallback if already set
}

async function fetchJson(url) {
  const res = await fetch(url);
  return res.json();
}

// -------- Wallet summary card --------

async function refreshWalletSummary(wallet) {
  const card = $("wallet-summary-card");
  if (!card) return;

  try {
    const data = await fetchJson(
      "/api/wallet-summary?wallet=" + encodeURIComponent(wallet)
    );

    if (!data.ok) {
      console.error("wallet-summary error:", data.error);
      card.style.display = "none";
      return;
    }

    const s = data.stats || {};
    const byTier = s.byTier || {};

    const title = data.displayName || wallet;
    const short = wallet.slice(0, 6) + "…" + wallet.slice(-4);

    card.innerHTML = `
      <div class="wallet-summary-main">
        <div class="wallet-summary-label">${short}</div>
        <div class="wallet-summary-address">${title}</div>
      </div>
      <div class="wallet-summary-chips">
        <span class="chip">Total: ${s.momentsTotal ?? 0}</span>
        <span class="chip">Unlocked: ${s.unlockedCount ?? 0}</span>
        <span class="chip">Locked: ${s.lockedCount ?? 0}</span>
        <span class="chip chip-pill-tier chip-common">Common: ${
          byTier.Common ?? 0
        }</span>
        <span class="chip chip-pill-tier chip-uncommon">Uncommon: ${
          byTier.Uncommon ?? 0
        }</span>
        <span class="chip chip-pill-tier chip-rare">Rare: ${
          byTier.Rare ?? 0
        }</span>
        <span class="chip chip-pill-tier chip-legendary">Legendary: ${
          byTier.Legendary ?? 0
        }</span>
        <span class="chip chip-pill-tier chip-ultimate">Ultimate: ${
          byTier.Ultimate ?? 0
        }</span>
      </div>
    `;

    card.style.display = "flex";
  } catch (err) {
    console.error("wallet-summary fetch failed:", err);
    card.style.display = "none";
  }
}

// -------- Filters ----------

function initFilterSelect(select, label, values) {
  select.innerHTML = "";

  const firstOption = document.createElement("option");
  firstOption.value = "ALL";
  firstOption.textContent = label;
  select.appendChild(firstOption);

  values.forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    select.appendChild(opt);
  });
}

function populateFilters(rows) {
  const teams = new Set();
  const players = new Set();
  const series = new Set();
  const sets = new Set();
  const tiers = new Set();
  const positions = new Set();

  rows.forEach((r) => {
    if (r.team_name) teams.add(r.team_name);
    const p = buildPlayerName(r);
    if (p) players.add(p);
    if (r.series_name) series.add(r.series_name);
    if (r.set_name) sets.add(r.set_name);
    if (r.tier) tiers.add(r.tier);
    if (r.position) positions.add(r.position);
  });

  initFilterSelect($("filter-team"), "All teams", Array.from(teams).sort());
  initFilterSelect(
    $("filter-player"),
    "All players",
    Array.from(players).sort()
  );
  initFilterSelect(
    $("filter-series"),
    "All series",
    Array.from(series).sort()
  );
  initFilterSelect($("filter-set"), "All sets", Array.from(sets).sort());
  initFilterSelect($("filter-tier"), "All tiers", Array.from(tiers).sort());
  initFilterSelect(
    $("filter-position"),
    "All positions",
    Array.from(positions).sort()
  );

  const lockedSelect = $("filter-locked");
  lockedSelect.innerHTML = "";
  [
    { value: "ALL", label: "Locked + Unlocked" },
    { value: "LOCKED", label: "Locked only" },
    { value: "UNLOCKED", label: "Unlocked only" },
  ].forEach((optDef) => {
    const opt = document.createElement("option");
    opt.value = optDef.value;
    opt.textContent = optDef.label;
    lockedSelect.appendChild(opt);
  });
}

function readFiltersFromUI() {
  state.filters.team = $("filter-team").value;
  state.filters.player = $("filter-player").value;
  state.filters.series = $("filter-series").value;
  state.filters.set = $("filter-set").value;
  state.filters.tier = $("filter-tier").value;
  state.filters.position = $("filter-position").value;
  state.filters.locked = $("filter-locked").value;
}

function resetFilters() {
  $("filter-team").value = "ALL";
  $("filter-player").value = "ALL";
  $("filter-series").value = "ALL";
  $("filter-set").value = "ALL";
  $("filter-tier").value = "ALL";
  $("filter-position").value = "ALL";
  $("filter-locked").value = "ALL";

  readFiltersFromUI();
  applyFiltersSortRender();
}

// -------- Sorting ----------

function setSort(key) {
  if (state.sort.key === key) {
    state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc";
  } else {
    state.sort.key = key;
    state.sort.dir = key === "last_event_ts" ? "desc" : "asc";
  }

  // update header indicators
  const ths = document.querySelectorAll(
    "#wallet-table thead th.sortable"
  );
  ths.forEach((th) => {
    th.classList.remove("sort-asc", "sort-desc");
    if (th.dataset.sortKey === state.sort.key) {
      th.classList.add(
        state.sort.dir === "asc" ? "sort-asc" : "sort-desc"
      );
    }
  });

  applyFiltersSortRender();
}

function compareValues(a, b, cfg, dir) {
  const direction = dir === "asc" ? 1 : -1;

  if (a == null && b == null) return 0;
  if (a == null) return 1 * direction;
  if (b == null) return -1 * direction;

  switch (cfg.type) {
    case "number":
      return (Number(a) - Number(b)) * direction;
    case "boolean":
      return (Boolean(a) === Boolean(b) ? 0 : Boolean(a) ? -1 : 1) * direction;
    case "date":
      return (new Date(a) - new Date(b)) * direction;
    default:
      return String(a).localeCompare(String(b)) * direction;
  }
}

// -------- Rendering ----------

function applyFiltersSortRender() {
  readFiltersFromUI();

  // filter
  const f = state.filters;
  let rows = state.allRows.filter((r) => {
    if (f.team !== "ALL" && r.team_name !== f.team) return false;
    const pName = buildPlayerName(r);
    if (f.player !== "ALL" && pName !== f.player) return false;
    if (f.series !== "ALL" && r.series_name !== f.series) return false;
    if (f.set !== "ALL" && r.set_name !== f.set) return false;
    if (f.tier !== "ALL" && r.tier !== f.tier) return false;
    if (f.position !== "ALL" && r.position !== f.position) return false;
    if (f.locked === "LOCKED" && !r.is_locked) return false;
    if (f.locked === "UNLOCKED" && r.is_locked) return false;
    return true;
  });

  // sort
  const sortKey = state.sort.key;
  const cfg = sortColumns[sortKey] || { type: "string" };

  rows.sort((a, b) =>
    compareValues(a[sortKey], b[sortKey], cfg, state.sort.dir)
  );

  state.filteredRows = rows;
  renderTable();
}

function renderTable() {
  const tbody = $("wallet-tbody");
  tbody.innerHTML = "";

  const rows = state.filteredRows;
  $("moments-title").textContent = `Moments (${rows.length} moments)`;

  for (const r of rows) {
    const tr = document.createElement("tr");

    const playerName = buildPlayerName(r) || "—";
    const team = r.team_name || "";
    const pos = r.position || "";
    const tier = r.tier || "";
    const serial = r.serial_number ?? "";
    const maxMint = r.max_mint_size ?? "";
    const series = r.series_name || "";
    const set = r.set_name || "";
    const locked = r.is_locked;
    const lastEvent = formatDate(r.last_event_ts);

    const momentUrl = `https://nflallday.com/moments/${r.nft_id}`;
    const listingUrl = r.edition_id
      ? `https://nflallday.com/listing/moment/${r.edition_id}`
      : null;

    tr.innerHTML = `
      <td>${playerName}</td>
      <td>${team}</td>
      <td>${pos}</td>
      <td>${tier}</td>
      <td>${serial}</td>
      <td>${maxMint}</td>
      <td>${series}</td>
      <td>${set}</td>
      <td>
        <span class="locked-pill ${
          locked ? "locked" : "unlocked"
        }">${locked ? "Locked" : "Unlocked"}</span>
      </td>
      <td>${lastEvent}</td>
      <td>
        <div class="link-group">
          <a href="${momentUrl}" target="_blank" rel="noopener noreferrer">Moment</a>
          ${
            listingUrl
              ? `<a href="${listingUrl}" target="_blank" rel="noopener noreferrer">Listing</a>`
              : ""
          }
        </div>
      </td>
    `;

    tbody.appendChild(tr);
  }
}

// -------- CSV Export ----------

function exportCsv() {
  const rows = state.filteredRows;
  if (!rows.length) return;

  const header = [
    "wallet_address",
    "nft_id",
    "edition_id",
    "player",
    "team",
    "position",
    "tier",
    "serial_number",
    "max_mint_size",
    "series_name",
    "set_name",
    "is_locked",
    "last_event_ts",
  ];

  const lines = [];
  lines.push(header.join(","));

  rows.forEach((r) => {
    const line = [
      escapeCsv(r.wallet_address),
      escapeCsv(r.nft_id),
      escapeCsv(r.edition_id),
      escapeCsv(buildPlayerName(r)),
      escapeCsv(r.team_name),
      escapeCsv(r.position),
      escapeCsv(r.tier),
      escapeCsv(r.serial_number),
      escapeCsv(r.max_mint_size),
      escapeCsv(r.series_name),
      escapeCsv(r.set_name),
      escapeCsv(r.is_locked ? "Locked" : "Unlocked"),
      escapeCsv(r.last_event_ts),
    ].join(",");
    lines.push(line);
  });

  const blob = new Blob([lines.join("\n")], {
    type: "text/csv;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${state.wallet || "wallet"}_moments.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// -------- Load wallet ----------

async function loadWallet(wallet) {
  if (!wallet) return;

  state.wallet = wallet.toLowerCase();
  $("wallet-input").value = wallet;

  const url = new URL(window.location.href);
  url.searchParams.set("wallet", wallet);
  window.history.replaceState({}, "", url.toString());

  const data = await fetchJson(
    "/api/query?wallet=" + encodeURIComponent(wallet)
  );
  if (!data.ok) {
    alert(data.error || "Error loading wallet");
    return;
  }

  // normalise rows
  const rows = data.rows.map((r) => ({
    ...r,
    playerName: buildPlayerName(r),
  }));

  state.allRows = rows;
  populateFilters(rows);
  resetFilters(); // also renders table
  await refreshWalletSummary(wallet);
}

// -------- Event wiring ----------

function init() {
  const form = $("wallet-form");
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const wallet = $("wallet-input").value.trim();
    if (!wallet) return;
    loadWallet(wallet);
  });

  $("reset-filters").addEventListener("click", (e) => {
    e.preventDefault();
    resetFilters();
  });

  $("export-csv").addEventListener("click", exportCsv);

  document
    .querySelectorAll("#wallet-table thead th.sortable")
    .forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.sortKey;
        setSort(key);
      });
    });

  // when any filter changes, re-apply
  document
    .querySelectorAll(".filters-panel select")
    .forEach((sel) => {
      sel.addEventListener("change", () => {
        applyFiltersSortRender();
      });
    });

  // initial wallet from URL
  const params = new URLSearchParams(window.location.search);
  const walletParam = params.get("wallet");
  if (walletParam) {
    $("wallet-input").value = walletParam;
    loadWallet(walletParam);
  }
}

document.addEventListener("DOMContentLoaded", init);
