// public/app.js

const PAGE_SIZE = 200;

let allMoments = [];
let filteredMoments = [];
let currentSortKey = "last_event_ts";
let currentSortDir = "desc";
let currentPage = 1;

// Generate badges for special moments
function getMomentBadges(moment) {
    const badges = [];
    // Parse as integers for proper comparison (API may return strings)
    const serial = parseInt(moment.serial_number, 10) || 0;
    const max = parseInt(moment.max_mint_size, 10) || 0;
    const jersey = parseInt(moment.jersey_number, 10) || 0;

    // Serial number badges only (removed Challenge, Crafted, Debut, Dynamic)
    if (serial === 1) {
        badges.push({ icon: "🥇", label: "#1", color: "#fbbf24", bg: "rgba(251, 191, 36, 0.2)" });
    }
    if (serial > 0 && max > 0 && serial === max) {
        badges.push({ icon: "🎯", label: "Perfect", color: "#ec4899", bg: "rgba(236, 72, 153, 0.2)" });
    }
    if (serial > 0 && jersey > 0 && serial === jersey) {
        badges.push({ icon: "🏈", label: "Jersey", color: "#22c55e", bg: "rgba(34, 197, 94, 0.2)" });
    }
    if (serial === 69) {
        badges.push({ icon: "😏", label: "69", color: "#f97316", bg: "rgba(249, 115, 22, 0.2)" });
    }
    if (serial === 420) {
        badges.push({ icon: "🌿", label: "420", color: "#22c55e", bg: "rgba(34, 197, 94, 0.2)" });
    }

    return badges.map(b =>
        `<span class="moment-badge" style="display: inline-flex; align-items: center; gap: 2px; padding: 1px 5px; border-radius: 4px; font-size: 0.6rem; font-weight: 600; background: ${b.bg}; color: ${b.color}; border: 1px solid ${b.color}30; margin-left: 3px;" title="${b.label}">${b.icon}</span>`
    ).join("");
}

// Get badge count for sorting (higher = more special)
function getBadgeCount(moment) {
    let count = 0;
    // Parse as integers for proper comparison
    const serial = parseInt(moment.serial_number, 10) || 0;
    const max = parseInt(moment.max_mint_size, 10) || 0;
    const jersey = parseInt(moment.jersey_number, 10) || 0;

    if (serial === 1) count += 10; // #1 is most valuable
    if (serial > 0 && max > 0 && serial === max) count += 5; // Perfect
    if (serial > 0 && jersey > 0 && serial === jersey) count += 5; // Jersey match
    if (serial === 69) count += 2;
    if (serial === 420) count += 2;

    return count;
}

// Detect parallel variant based on set_name and max_mint_size
// Sapphire = /25, Emerald = /50, Ruby = /299 (or null), Standard = non-parallel
function getParallelVariant(moment) {
    const setName = (moment.set_name || "").toLowerCase();
    const maxMint = parseInt(moment.max_mint_size, 10) || 0;

    // Check if this is a parallel set
    if (!setName.includes("parallel")) {
        return "standard"; // Not a parallel set
    }

    // Determine variant by mint size
    // Note: Ruby (/299) moments often have max_mint_size as null in the database
    if (maxMint === 25) return "sapphire";
    if (maxMint === 50) return "emerald";
    if (maxMint === 299) return "ruby";

    // If max_mint is 0/null but it's a parallel set, assume Ruby (/299)
    if (maxMint === 0) return "ruby";

    // Unknown parallel variant with different mint size
    return "parallel";
}

function getEls() {
    return {
        tbody: document.getElementById("wallet-tbody"),
        title: document.getElementById("moments-title"),
        exportBtn: document.getElementById("export-csv"),
        pagerPrev: document.getElementById("wallet-page-prev"),
        pagerNext: document.getElementById("wallet-page-next"),
        pagerInfo: document.getElementById("wallet-page-info"),
        summaryCard: document.getElementById("wallet-summary-card"),
        filterTeam: document.getElementById("filter-team"),
        filterPlayer: document.getElementById("filter-player"),
        filterSeries: document.getElementById("filter-series"),
        filterSet: document.getElementById("filter-set"),
        filterTier: document.getElementById("filter-tier"),
        filterPosition: document.getElementById("filter-position"),
        filterLocked: document.getElementById("filter-locked"),
        resetFilters: document.getElementById("reset-filters"),
        table: document.getElementById("wallet-table"),
        mobileCards: document.getElementById("wallet-mobile-cards")
    };
}

function showLoadingAnimation() {
    const els = getEls();

    // Show loading in table
    if (els.tbody) {
        els.tbody.innerHTML = `
            <tr class="skeleton-row">
                <td class="skeleton-cell" colspan="15">
                    <div class="wallet-loading">
                        <div class="loading-spinner"></div>
                        <div class="loading-text">
                            Loading wallet data
                            <span class="loading-dots">
                                <span></span>
                                <span></span>
                                <span></span>
                            </span>
                        </div>
                    </div>
                </td>
            </tr>
        `;
    }

    // Show loading in mobile cards
    if (els.mobileCards) {
        els.mobileCards.innerHTML = `
            <div class="wallet-loading">
                <div class="loading-spinner"></div>
                <div class="loading-text">
                    Loading wallet data
                    <span class="loading-dots">
                        <span></span>
                        <span></span>
                        <span></span>
                    </span>
                </div>
            </div>
        `;
    }

    // Update title
    if (els.title) {
        els.title.innerHTML = `
            <span style="display: inline-flex; align-items: center; gap: 0.5rem;">
                <span class="loading-spinner" style="width: 20px; height: 20px; border-width: 2px; margin: 0;"></span>
                Loading wallet...
            </span>
        `;
    }
}

function hideLoadingAnimation() {
    const els = getEls();

    // Title will be updated by renderPage, so just clear loading state
    if (els.title && els.title.textContent.includes("Loading")) {
        els.title.textContent = "Moments";
    }
}

function tierToClass(tier) {
    const t = (tier || "").toLowerCase();
    if (t === "common") return "chip-common";
    if (t === "uncommon") return "chip-uncommon";
    if (t === "rare") return "chip-rare";
    if (t === "legendary") return "chip-legendary";
    if (t === "ultimate") return "chip-ultimate";
    return "";
}

function formatDate(ts) {
    if (!ts) return "";
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return ts;
    return d.toLocaleString();
}

function formatPrice(v) {
    if (v == null) return "";
    const num = Number(v);
    if (Number.isNaN(num)) return "";
    return `$${num.toFixed(2)}`;
}

function formatUsd(value) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) return "$0.00";
    return num.toLocaleString(undefined, {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function computeTopSets(moments, limit = 5) {
    const setCounts = new Map();
    for (const m of moments) {
        const setName = m.set_name;
        if (!setName) continue;
        setCounts.set(setName, (setCounts.get(setName) || 0) + 1);
    }
    return Array.from(setCounts.entries())
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}

function computeTopPlayers(moments, limit = 5) {
    const playerCounts = new Map();
    for (const m of moments) {
        const playerName = [m.first_name, m.last_name].filter(Boolean).join(" ");
        if (!playerName) continue;
        playerCounts.set(playerName, (playerCounts.get(playerName) || 0) + 1);
    }
    return Array.from(playerCounts.entries())
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}

// Filter by tier (called from tier chip onclick handlers)
window.filterByTier = function (tier) {
    const els = getEls();
    if (!els || !els.filterTier) return;

    // Find the actual tier value in the dropdown options (case-insensitive match)
    const tierLower = tier.toLowerCase();
    let actualTierValue = null;

    for (const option of els.filterTier.options) {
        if (option.value && option.value.toLowerCase() === tierLower) {
            actualTierValue = option.value;
            break;
        }
    }

    // If we found a matching option, use it; otherwise try the original value
    const targetValue = actualTierValue || tier;

    // If clicking the same tier that's already selected, clear the filter
    if (els.filterTier.value === targetValue) {
        els.filterTier.value = "";
    } else {
        els.filterTier.value = targetValue;
    }

    applyFilters();
    applySort();
    renderPage(1);
};

function buildFilterOptions() {
    const els = getEls();
    const teams = new Set();
    const players = new Set();
    const series = new Set();
    const sets = new Set();
    const positions = new Set();
    const tiers = new Set();

    for (const r of allMoments) {
        if (r.team_name) teams.add(r.team_name);
        const playerName = [r.first_name, r.last_name].filter(Boolean).join(" ");
        if (playerName) players.add(playerName);
        if (r.series_name) series.add(r.series_name);
        if (r.set_name) sets.add(r.set_name);
        if (r.position) positions.add(r.position);
        if (r.tier) tiers.add(r.tier);
    }

    function fillSelect(select, values, { includeAllLabel = "All" } = {}) {
        if (!select) return;
        select.innerHTML = "";
        const optAll = document.createElement("option");
        optAll.value = "";
        optAll.textContent = includeAllLabel;
        select.appendChild(optAll);

        [...values]
            .sort((a, b) => String(a).localeCompare(String(b)))
            .forEach((v) => {
                const opt = document.createElement("option");
                opt.value = v;
                opt.textContent = v;
                select.appendChild(opt);
            });
    }

    fillSelect(els.filterTeam, teams, { includeAllLabel: "All teams" });
    fillSelect(els.filterPlayer, players, { includeAllLabel: "All players" });
    fillSelect(els.filterSeries, series, { includeAllLabel: "All series" });
    fillSelect(els.filterSet, sets, { includeAllLabel: "All sets" });
    fillSelect(els.filterTier, tiers, { includeAllLabel: "All tiers" });
    fillSelect(els.filterPosition, positions, { includeAllLabel: "All positions" });

    if (els.filterLocked) {
        els.filterLocked.innerHTML = "";
        const optAll = document.createElement("option");
        optAll.value = "";
        optAll.textContent = "All";
        els.filterLocked.appendChild(optAll);

        const optUnlocked = document.createElement("option");
        optUnlocked.value = "unlocked";
        optUnlocked.textContent = "Unlocked only";
        els.filterLocked.appendChild(optUnlocked);

        const optLocked = document.createElement("option");
        optLocked.value = "locked";
        optLocked.textContent = "Locked only";
        els.filterLocked.appendChild(optLocked);
    }
}

function applyFilters() {
    const els = getEls();
    const team = els.filterTeam?.value || "";
    const player = els.filterPlayer?.value || "";
    const series = els.filterSeries?.value || "";
    const setName = els.filterSet?.value || "";
    const tier = els.filterTier?.value || "";
    const position = els.filterPosition?.value || "";
    const lockedVal = els.filterLocked?.value || "";

    // Get selected badge filters
    const selectedBadges = [];
    document.querySelectorAll('.badge-filter-list input[type="checkbox"]:checked').forEach(cb => {
        selectedBadges.push(cb.value);
    });

    // Get selected parallel variant filters
    const selectedParallels = [];
    document.querySelectorAll('.parallel-filter-list input[type="checkbox"]:checked').forEach(cb => {
        selectedParallels.push(cb.value);
    });

    filteredMoments = allMoments.filter((r) => {
        if (team && r.team_name !== team) return false;

        if (player) {
            const name = [r.first_name, r.last_name].filter(Boolean).join(" ");
            if (name !== player) return false;
        }

        if (series && r.series_name !== series) return false;
        if (setName && r.set_name !== setName) return false;
        if (tier && (r.tier || "").toLowerCase() !== tier.toLowerCase()) return false;
        if (position && r.position !== position) return false;

        if (lockedVal === "locked" && !r.is_locked) return false;
        if (lockedVal === "unlocked" && r.is_locked) return false;

        // Parallel variant filtering - moment must match ANY selected variant
        if (selectedParallels.length > 0) {
            const variant = getParallelVariant(r);
            if (!selectedParallels.includes(variant)) return false;
        }

        // Badge filtering - moment must have ALL selected badges
        if (selectedBadges.length > 0) {
            for (const badge of selectedBadges) {
                if (!hasBadge(r, badge)) return false;
            }
        }

        return true;
    });
}

// Check if a moment has a specific badge
function hasBadge(moment, badgeType) {
    // Parse as integers for proper comparison
    const serial = parseInt(moment.serial_number, 10) || 0;
    const max = parseInt(moment.max_mint_size, 10) || 0;
    const jersey = parseInt(moment.jersey_number, 10) || 0;

    switch (badgeType) {
        case "serial1":
            return serial === 1;
        case "perfect":
            return serial > 0 && max > 0 && serial === max;
        case "jersey":
            return serial > 0 && jersey > 0 && serial === jersey;
        case "69":
            return serial === 69;
        case "420":
            return serial === 420;
        default:
            return false;
    }
}

function applySort() {
    const key = currentSortKey;
    const dir = currentSortDir;

    const getValue = (row) => {
        if (key === "playerName") {
            return [row.first_name, row.last_name].filter(Boolean).join(" ");
        }
        if (key === "badge_count") {
            return getBadgeCount(row);
        }
        return row[key];
    };

    filteredMoments.sort((a, b) => {
        let va = getValue(a);
        let vb = getValue(b);

        if (va == null && vb == null) return 0;
        if (va == null) return dir === "asc" ? 1 : -1;
        if (vb == null) return dir === "asc" ? -1 : 1;

        if (
            key === "serial_number" ||
            key === "max_mint_size" ||
            key === "low_ask_usd" ||
            key === "avg_sale_usd" ||
            key === "top_sale_usd" ||
            key === "owned_count" ||
            key === "badge_count"
        ) {
            va = Number(va) || 0;
            vb = Number(vb) || 0;
        } else if (key === "is_locked") {
            va = !!va;
            vb = !!vb;
            if (va === vb) return 0;
            return dir === "asc" ? (va ? 1 : -1) : va ? -1 : 1;
        } else if (key === "last_event_ts") {
            va = new Date(va).getTime() || 0;
            vb = new Date(vb).getTime() || 0;
        } else {
            va = String(va).toLowerCase();
            vb = String(vb).toLowerCase();
        }

        if (va < vb) return dir === "asc" ? -1 : 1;
        if (va > vb) return dir === "asc" ? 1 : -1;
        return 0;
    });
}

function getTeamAbbrev(teamName) {
    const abbrevs = {
        "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL", "Buffalo Bills": "BUF",
        "Carolina Panthers": "CAR", "Chicago Bears": "CHI", "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE",
        "Dallas Cowboys": "DAL", "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
        "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX", "Kansas City Chiefs": "KC",
        "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC", "Los Angeles Rams": "LAR", "Miami Dolphins": "MIA",
        "Minnesota Vikings": "MIN", "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
        "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT", "San Francisco 49ers": "SF",
        "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB", "Tennessee Titans": "TEN", "Washington Commanders": "WAS"
    };
    return abbrevs[teamName] || teamName?.substring(0, 3).toUpperCase() || '???';
}

function abbreviateSeries(seriesName) {
    if (!seriesName) return "";
    // Common abbreviations
    const abbrevs = {
        "2025 Season": "2025",
        "2024 Season": "2024",
        "2023 Season": "2023",
        "2022 Season": "2022",
        "2021 Season": "2021",
        "Series 1": "S1",
        "Series 2": "S2",
        "Series 3": "S3",
        "Historical": "Hist",
        "Historical 24": "Hist 24",
        "Historical 25": "Hist 25"
    };
    if (abbrevs[seriesName]) return abbrevs[seriesName];
    // If longer than 12 chars, truncate
    if (seriesName.length > 12) {
        return seriesName.substring(0, 10) + "..";
    }
    return seriesName;
}

function abbreviateSet(setName) {
    if (!setName) return "";
    // Common long set names that can be abbreviated
    const abbrevs = {
        "Move the Chains": "MTC",
        "Make the Stop": "MTS",
        "Rookie Debut": "Rookie",
        "Game Changers": "GC",
        "Gridiron": "Grid",
        "Showtime": "Show",
        "Locked In": "Locked",
        "Draw it Up": "Draw",
        "Highwire": "Wire"
    };
    if (abbrevs[setName]) return abbrevs[setName];
    // If longer than 15 chars, truncate
    if (setName.length > 15) {
        return setName.substring(0, 13) + "..";
    }
    return setName;
}

function countDuplicatesByEdition(moments) {
    // Count how many times each edition_id appears
    const editionCounts = {};
    for (const moment of moments) {
        const editionId = moment.edition_id;
        if (editionId) {
            editionCounts[editionId] = (editionCounts[editionId] || 0) + 1;
        }
    }
    return editionCounts;
}

function renderPage(page) {
    const els = getEls();
    if (!els.tbody) return;

    const total = filteredMoments.length;
    const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

    currentPage = Math.max(1, Math.min(page, totalPages));

    const start = (currentPage - 1) * PAGE_SIZE;
    const end = Math.min(start + PAGE_SIZE, total);
    const slice = filteredMoments.slice(start, end);

    // Count duplicates by edition_id across all moments in the wallet (not just filtered)
    const editionCounts = countDuplicatesByEdition(allMoments);

    els.tbody.innerHTML = "";

    // Also render mobile cards
    const mobileCards = document.getElementById("wallet-mobile-cards");
    if (mobileCards) {
        mobileCards.innerHTML = "";
    }

    for (const r of slice) {
        const tr = document.createElement("tr");
        // For team plays (like Make the Stop, NFL Draft), first/last name may be null
        // Fall back to team name so it shows "Detroit Lions" instead of "(unknown)"
        const playerNameRaw = [r.first_name, r.last_name].filter(Boolean).join(" ");
        const playerName = playerNameRaw || r.team_name || "(unknown)";
        const tierClass = tierToClass(r.tier);
        const tierHtml = r.tier ? `<span class="chip-pill-tier ${tierClass}">${r.tier}</span>` : "";

        const serial = r.serial_number ?? "";
        const max = r.max_mint_size ?? "";
        const momentUrl = r.nft_id ? `https://nflallday.com/moments/${r.nft_id}` : "";
        const marketUrl = r.edition_id ? `https://nflallday.com/listing/moment/${r.edition_id}` : "";

        const lowAsk = formatPrice(r.low_ask_usd);
        const avgSale = formatPrice(r.avg_sale_usd);
        const topSale = formatPrice(r.top_sale_usd);

        // Use abbreviations for compact display
        const teamAbbrev = getTeamAbbrev(r.team_name);
        const seriesAbbrev = abbreviateSeries(r.series_name);
        const setAbbrev = abbreviateSet(r.set_name);

        // Compact date format (just date, no time)
        const eventDate = r.last_event_ts ? formatDate(r.last_event_ts).split(',')[0] : "";

        // Get duplicate count for this edition
        const duplicateCount = r.edition_id ? editionCounts[r.edition_id] : 1;
        const ownedDisplay = duplicateCount > 1 ? `<span class="duplicate-count" title="Owned ${duplicateCount} of this moment">${duplicateCount}</span>` : "1";

        // Get badges for this moment
        const badgesHtml = getMomentBadges(r);

        tr.innerHTML = `
      <td>${playerName}${badgesHtml}</td>
      <td style="text-align: center;">${ownedDisplay}</td>
      <td title="${r.team_name || ""}">${teamAbbrev}</td>
      <td>${r.position || ""}</td>
      <td>${tierHtml}</td>
      <td>${serial}</td>
      <td>${max}</td>
      <td title="${r.series_name || ""}">${seriesAbbrev}</td>
      <td title="${r.set_name || ""}">${setAbbrev}</td>
      <td>${lowAsk}</td>
      <td>${avgSale}</td>
      <td>${topSale}</td>
      <td>
        <span class="locked-pill ${r.is_locked ? "locked" : "unlocked"}">
          ${r.is_locked ? "🔒" : "✓"}
        </span>
      </td>
      <td>${eventDate}</td>
      <td>
        <div class="link-group">
          ${momentUrl ? `<a href="${momentUrl}" target="_blank" rel="noopener" title="View Moment">M</a>` : ""}
          ${marketUrl ? `<a href="${marketUrl}" target="_blank" rel="noopener" title="View Market">$</a>` : ""}
        </div>
      </td>
    `;
        els.tbody.appendChild(tr);

        // Mobile card
        if (mobileCards) {
            const tierLower = (r.tier || "common").toLowerCase();
            const teamAbbrev = getTeamAbbrev(r.team_name);
            const cardUrl = marketUrl || momentUrl || "#";
            const lockedClass = r.is_locked ? "locked" : "unlocked";

            // Get duplicate count for mobile view
            const duplicateCount = r.edition_id ? editionCounts[r.edition_id] : 1;
            const mobileCountBadge = duplicateCount > 1 ? ` <span class="duplicate-count">x${duplicateCount}</span>` : "";

            // Get badges for mobile
            const mobileBadgesHtml = getMomentBadges(r);

            const card = document.createElement("a");
            card.href = cardUrl;
            card.target = "_blank";
            card.className = `mobile-card ${lockedClass}`;
            card.innerHTML = `
                <div class="mobile-card-icon ${tierLower}">🏈</div>
                <div class="mobile-card-main">
                    <div class="mobile-card-header">
                        <span class="mobile-card-title">${playerName}${mobileBadgesHtml}</span>
                        <span class="mobile-card-badge">${teamAbbrev}</span>
                        <span class="mobile-card-badge tier ${tierLower}">${r.tier || "?"}</span>
                        ${duplicateCount > 1 ? `<span class="duplicate-count">x${duplicateCount}</span>` : ""}
                    </div>
                    <div class="mobile-card-details">
                        <span>${r.set_name || "Unknown Set"}</span>
                        <span>#${serial || "?"}</span>
                        <span>${r.is_locked ? "🔒" : "✓"}</span>
                    </div>
                </div>
                <div class="mobile-card-right">
                    <div class="mobile-card-value">${lowAsk || "—"}</div>
                    <div class="mobile-card-sub">${r.position || ""}</div>
                </div>
            `;
            mobileCards.appendChild(card);
        }
    }

    if (els.title) {
        els.title.textContent = `Moments – ${total.toLocaleString()} total`;
    }
    if (els.pagerInfo) {
        const from = total ? start + 1 : 0;
        const to = end;
        els.pagerInfo.textContent = `Page ${currentPage} of ${totalPages} • showing ${from}-${to} of ${total}`;
    }
    if (els.pagerPrev) {
        els.pagerPrev.disabled = currentPage <= 1;
    }
    if (els.pagerNext) {
        els.pagerNext.disabled = currentPage >= totalPages;
    }

    updateSortHeaderClasses();
    updateMobileSortDropdowns();
}

function updateSortHeaderClasses() {
    const els = getEls();
    if (!els.table) return;
    const ths = els.table.querySelectorAll("th.sortable");
    ths.forEach((th) => {
        th.classList.remove("sort-asc", "sort-desc");
        const key = th.getAttribute("data-sort-key");
        if (key === currentSortKey) {
            th.classList.add(currentSortDir === "asc" ? "sort-asc" : "sort-desc");
        }
    });
}

function updateMobileSortDropdowns() {
    const mobileSortKey = document.getElementById("mobile-sort-key");
    const mobileSortDir = document.getElementById("mobile-sort-dir");

    if (mobileSortKey) {
        mobileSortKey.value = currentSortKey;
    }
    if (mobileSortDir) {
        mobileSortDir.value = currentSortDir;
    }
}

// Client-side cache for prices and filters
const priceCache = new Map();
const filterCache = new Map();

// Make fetchWalletSummary available globally for profile search
window.fetchWalletSummary = async function fetchWalletSummary(wallet) {
    const els = getEls();
    if (!els.summaryCard) return;

    // Show loading state in summary card
    els.summaryCard.style.display = "flex";
    els.summaryCard.innerHTML = `
        <div style="display: flex; align-items: center; gap: 0.75rem; padding: 1rem; width: 100%;">
            <div class="loading-spinner" style="width: 24px; height: 24px; border-width: 3px; margin: 0;"></div>
            <span style="color: var(--text-muted);">Loading wallet summary...</span>
        </div>
    `;

    try {
        // Use fast database query (data kept current by sync scripts)
        const res = await fetch(`/api/wallet-summary?wallet=${encodeURIComponent(wallet)}`);
        const data = await res.json();
        if (!data.ok) return;

        const stats = data.stats || {};
        const byTier = stats.byTier || {};

        const addressLabel = data.displayName ? `${data.displayName} · ${data.wallet}` : data.wallet;

        const holdingsLastSyncedAt = data.holdingsLastSyncedAt || data.holdingsLastEventTs || null;
        const pricesLastScrapedAt = data.pricesLastScrapedAt || null;

        // Format dollar values nicely
        const floorVal = Number(stats.floorValue || 0);
        const aspVal = Number(stats.aspValue || 0);

        const floorText = floorVal.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });

        const aspText = aspVal.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });

        const holdingsSyncText = holdingsLastSyncedAt ? formatDate(holdingsLastSyncedAt) : "Unknown";
        const pricesSyncText = pricesLastScrapedAt ? formatDate(pricesLastScrapedAt) : "Unknown";

        // Check if user is logged in and if this is their default wallet
        let isLoggedIn = false;
        let isDefault = false;
        try {
            const meRes = await fetch("/api/me", { credentials: "include" });
            const meData = await meRes.json();
            if (meData.ok && meData.user) {
                isLoggedIn = true;
                isDefault = meData.user.default_wallet_address &&
                    meData.user.default_wallet_address.toLowerCase() === wallet.toLowerCase();
            }
        } catch (err) {
            console.error("Failed to check login status:", err);
        }

        els.summaryCard.innerHTML = `
      <div class="wallet-summary-main" style="width: 100%;">
        <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; flex-wrap: wrap;">
          <div style="text-align: left;">
            <div class="wallet-summary-label mobile-hidden" style="text-align: left;">Wallet Overview</div>
            <div class="wallet-summary-address" style="text-align: left;">${addressLabel}</div>
          </div>
          <div class="mobile-hidden" style="display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center;">
            ${isLoggedIn ? `
              <button id="btn-set-default-wallet" 
                      type="button"
                      class="btn-secondary" 
                      style="font-size: 0.75rem; padding: 0.35rem 0.7rem; ${isDefault ? 'opacity: 0.6; cursor: not-allowed;' : 'cursor: pointer;'}"
                      ${isDefault ? 'disabled' : ''}
                      title="${isDefault ? 'This is already your default wallet' : 'Set this wallet as your default'}">
                ${isDefault ? '✓ Default' : '⭐ Set Default'}
              </button>
            ` : ''}
            <button id="btn-refresh-wallet" 
                    type="button"
                    class="btn-secondary" 
                    style="font-size: 0.75rem; padding: 0.35rem 0.7rem; cursor: pointer;"
                    title="Refresh wallet holdings from blockchain">
              🔄 Refresh
            </button>
          </div>
        </div>
        <div class="wallet-summary-chips mobile-hidden" style="margin-top: 0.5rem; text-align: left;">
          <span class="chip">Total: ${stats.momentsTotal ?? 0}</span>
          <span class="chip">Unlocked: ${stats.unlockedCount ?? 0}</span>
          <span class="chip">Locked: ${stats.lockedCount ?? 0}</span>
          <span class="chip" title="Sum of per-edition lowest asks">Floor: $${floorText}</span>
          <span class="chip" title="Sum of per-edition average sale prices">ASP: $${aspText}</span>
        </div>
        <!-- Mobile-only simplified stats -->
        <div class="wallet-summary-chips mobile-show" style="display: none; text-align: left;">
          <span class="chip">💰 $${floorText}</span>
          <span class="chip">📊 $${aspText}</span>
        </div>

      </div>
      <div class="wallet-summary-chips" style="flex-wrap: wrap; margin-top: 0.5rem;">
        <span class="chip-pill-tier chip-common" style="cursor: pointer;" onclick="filterByTier('Common')" title="Click to filter by Common tier">Common: ${byTier.Common ?? 0}</span>
        <span class="chip-pill-tier chip-uncommon" style="cursor: pointer;" onclick="filterByTier('Uncommon')" title="Click to filter by Uncommon tier">Uncommon: ${byTier.Uncommon ?? 0}</span>
        <span class="chip-pill-tier chip-rare" style="cursor: pointer;" onclick="filterByTier('Rare')" title="Click to filter by Rare tier">Rare: ${byTier.Rare ?? 0}</span>
        <span class="chip-pill-tier chip-legendary" style="cursor: pointer;" onclick="filterByTier('Legendary')" title="Click to filter by Legendary tier">Legendary: ${byTier.Legendary ?? 0}</span>
        <span class="chip-pill-tier chip-ultimate" style="cursor: pointer;" onclick="filterByTier('Ultimate')" title="Click to filter by Ultimate tier">Ultimate: ${byTier.Ultimate ?? 0}</span>
      </div>
    `;

        // Wire up the "Set as default wallet" button
        const setDefaultBtn = document.getElementById("btn-set-default-wallet");
        if (setDefaultBtn && !setDefaultBtn.disabled) {
            setDefaultBtn.addEventListener("click", async (e) => {
                e.preventDefault();
                e.stopPropagation();

                try {
                    setDefaultBtn.disabled = true;
                    setDefaultBtn.textContent = "Saving...";

                    const res = await fetch("/api/me/wallet", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        credentials: "include",
                        body: JSON.stringify({ wallet_address: wallet })
                    });

                    const data = await res.json();
                    if (!res.ok || !data.ok) {
                        throw new Error(data.error || "Failed to set default wallet");
                    }

                    setDefaultBtn.textContent = "✓ Default wallet";
                    setDefaultBtn.disabled = true;
                    setDefaultBtn.style.opacity = "0.6";
                    setDefaultBtn.style.cursor = "not-allowed";
                    setDefaultBtn.title = "This is now your default wallet";

                    // Update header to reflect new default
                    if (window.updateNavAccount) window.updateNavAccount();
                } catch (err) {
                    console.error("Failed to set default wallet:", err);
                    alert("Failed to set default wallet: " + (err.message || "Unknown error"));
                    setDefaultBtn.disabled = false;
                    setDefaultBtn.textContent = "Set as default wallet";
                }
            });
        }

        // Wire up the "Refresh Holdings" button
        const refreshBtn = document.getElementById("btn-refresh-wallet");
        if (refreshBtn) {
            refreshBtn.addEventListener("click", async (e) => {
                e.preventDefault();
                e.stopPropagation();

                try {
                    refreshBtn.disabled = true;
                    refreshBtn.textContent = "🔄 Refreshing...";

                    // Refresh wallet holdings
                    allMoments = await fetchWalletMoments(wallet, true); // Force refresh

                    await attachPricesToMoments(allMoments);

                    // Update summary with fresh data
                    await fetchWalletSummary(wallet);

                    // Rebuild filters and re-render
                    const editionCounts = countDuplicatesByEdition(allMoments);
                    for (const moment of allMoments) {
                        moment.owned_count = moment.edition_id ? (editionCounts[moment.edition_id] || 1) : 1;
                    }

                    updateSummaryWithStats(wallet);
                    buildFilterOptions();
                    applyFilters();
                    applySort();
                    renderPage(1);

                    refreshBtn.textContent = "✓ Refreshed";
                    setTimeout(() => {
                        refreshBtn.textContent = "🔄 Refresh Holdings";
                        refreshBtn.disabled = false;
                    }, 2000);
                } catch (err) {
                    console.error("Failed to refresh wallet:", err);
                    alert("Failed to refresh wallet: " + (err.message || "Unknown error"));
                    refreshBtn.textContent = "🔄 Refresh Holdings";
                    refreshBtn.disabled = false;
                }
            });
        }
        els.summaryCard.style.display = "flex";
    } catch (err) {
        console.error("fetchWalletSummary error", err);
    }
}

function updateSummaryWithStats(wallet) {
    const els = getEls();
    if (!els.summaryCard || !allMoments || allMoments.length === 0) return;

    const topSets = computeTopSets(allMoments);
    const topPlayers = computeTopPlayers(allMoments);

    // Find the main summary div and append stats
    const mainDiv = els.summaryCard.querySelector(".wallet-summary-main");
    if (!mainDiv) return;

    // Remove any existing stats divs
    const existingStats = mainDiv.querySelectorAll(".wallet-stats-extra");
    existingStats.forEach(el => el.remove());

    // Add new stats (hidden on mobile)
    if (topSets.length > 0 || topPlayers.length > 0) {
        const statsDiv = document.createElement("div");
        statsDiv.className = "wallet-stats-extra mobile-hidden";
        statsDiv.style.cssText = "margin-top: 0.5rem;";

        if (topSets.length > 0) {
            const setsDiv = document.createElement("div");
            setsDiv.className = "wallet-summary-chips";
            setsDiv.style.cssText = "margin-top: 0.5rem;";
            setsDiv.innerHTML = `
                <span style="font-size: 0.7rem; color: var(--text-muted); margin-right: 0.5rem;">Top sets:</span>
                ${topSets.map(s => `<span class="chip" style="font-size: 0.7rem;">${s.name} (${s.count})</span>`).join('')}
            `;
            statsDiv.appendChild(setsDiv);
        }

        if (topPlayers.length > 0) {
            const playersDiv = document.createElement("div");
            playersDiv.className = "wallet-summary-chips";
            playersDiv.style.cssText = "margin-top: 0.5rem;";
            playersDiv.innerHTML = `
                <span style="font-size: 0.7rem; color: var(--text-muted); margin-right: 0.5rem;">Top players:</span>
                ${topPlayers.map(p => `<span class="chip" style="font-size: 0.7rem;">${p.name} (${p.count})</span>`).join('')}
            `;
            statsDiv.appendChild(playersDiv);
        }

        mainDiv.appendChild(statsDiv);
    }
}

async function fetchWalletMoments(wallet, forceRefresh = false) {
    // Use database endpoint - it has all holdings including locked moments
    // The sync script keeps it up-to-date with blockchain + locked status from Snowflake
    let url = `/api/query?wallet=${encodeURIComponent(wallet)}${forceRefresh ? '&refresh=1' : ''}`;

    try {
        const res = await fetch(url);
        const data = await res.json();
        if (data.ok) {
            return data.rows || [];
        }
        // If blockchain query fails, fallback to database
        console.warn('Blockchain query failed, falling back to database:', data.error);
    } catch (err) {
        console.warn('Blockchain query error, falling back to database:', err);
    }

    // Fallback to database query
    url = `/api/query?wallet=${encodeURIComponent(wallet)}${forceRefresh ? '&refresh=1' : ''}`;
    const res = await fetch(url);
    const data = await res.json();
    if (!data.ok) {
        throw new Error(data.error || "Failed to load wallet");
    }
    return data.rows || [];
}

async function attachPricesToMoments(moments) {
    const editionIds = [...new Set(moments.map((r) => r.edition_id).filter(Boolean))];
    if (!editionIds.length) return;

    // Check cache first
    const uncachedIds = [];
    const lowAskMap = {};
    const aspMap = {};
    const topSaleMap = {};

    for (const id of editionIds) {
        const cached = priceCache.get(id);
        if (cached) {
            if (cached.lowAsk != null) lowAskMap[id] = cached.lowAsk;
            if (cached.asp != null) aspMap[id] = cached.asp;
            if (cached.topSale != null) topSaleMap[id] = cached.topSale;
        } else {
            uncachedIds.push(id);
        }
    }

    // For very large wallets, a single /api/prices?editions=... URL can
    // exceed browser/server limits. Chunk the requests and merge results.
    const chunkSize = 200;

    try {
        for (let i = 0; i < uncachedIds.length; i += chunkSize) {
            const chunk = uncachedIds.slice(i, i + chunkSize);
            const qs = encodeURIComponent(chunk.join(","));
            const res = await fetch(`/api/prices?editions=${qs}`);
            const data = await res.json();

            if (data && data.ok !== false) {
                const low = data.lowAsk || {};
                const asp = data.asp || {};
                const top = data.topSale || {};

                // Merge into maps
                Object.assign(lowAskMap, low);
                Object.assign(aspMap, asp);
                Object.assign(topSaleMap, top);

                // Cache results
                for (const id of chunk) {
                    priceCache.set(id, {
                        lowAsk: low[id] ?? null,
                        asp: asp[id] ?? null,
                        topSale: top[id] ?? null
                    });
                }
            }
        }

        for (const r of moments) {
            const id = r.edition_id;
            if (!id) continue;
            const low = lowAskMap[id];
            const avg = aspMap[id];
            const top = topSaleMap[id];

            r.low_ask_usd = low != null ? Number(low) : null;
            r.avg_sale_usd = avg != null ? Number(avg) : null;
            r.top_sale_usd = top != null ? Number(top) : null;
        }
    } catch (err) {
        console.error("attachPricesToMoments error", err);
    }
}

function wireEvents() {
    const els = getEls();
    // Wallet form removed - wallet loading is now handled via profile search

    const filterEls = [
        els.filterTeam,
        els.filterPlayer,
        els.filterSeries,
        els.filterSet,
        els.filterTier,
        els.filterPosition,
        els.filterLocked
    ].filter(Boolean);

    filterEls.forEach((sel) => {
        sel.addEventListener("change", () => {
            applyFilters();
            applySort();
            renderPage(1);
        });
    });

    // Badge filter checkboxes
    const badgeCheckboxes = document.querySelectorAll('.badge-filter-list input[type="checkbox"]');
    badgeCheckboxes.forEach((cb) => {
        cb.addEventListener("change", () => {
            applyFilters();
            applySort();
            renderPage(1);
        });
    });

    // Parallel filter checkboxes
    const parallelCheckboxes = document.querySelectorAll('.parallel-filter-list input[type="checkbox"]');
    parallelCheckboxes.forEach((cb) => {
        cb.addEventListener("change", () => {
            applyFilters();
            applySort();
            renderPage(1);
        });
    });

    if (els.resetFilters) {
        els.resetFilters.addEventListener("click", () => {
            filterEls.forEach((sel) => {
                sel.value = "";
            });
            // Also reset badge checkboxes
            badgeCheckboxes.forEach((cb) => {
                cb.checked = false;
            });
            // Also reset parallel checkboxes
            parallelCheckboxes.forEach((cb) => {
                cb.checked = false;
            });
            applyFilters();
            applySort();
            renderPage(1);
        });
    }

    if (els.table) {
        const ths = els.table.querySelectorAll("th.sortable");
        ths.forEach((th) => {
            th.addEventListener("click", () => {
                const key = th.getAttribute("data-sort-key");
                if (!key) return;
                if (currentSortKey === key) {
                    currentSortDir = currentSortDir === "asc" ? "desc" : "asc";
                } else {
                    currentSortKey = key;
                    currentSortDir = "asc";
                }
                // Update mobile sort dropdowns to match
                updateMobileSortDropdowns();
                applySort();
                renderPage(1);
            });
        });
    }

    // Mobile sort dropdowns
    const mobileSortKey = document.getElementById("mobile-sort-key");
    const mobileSortDir = document.getElementById("mobile-sort-dir");

    if (mobileSortKey) {
        mobileSortKey.addEventListener("change", () => {
            currentSortKey = mobileSortKey.value;
            applySort();
            renderPage(1);
            updateSortHeaderClasses();
        });
    }

    if (mobileSortDir) {
        mobileSortDir.addEventListener("change", () => {
            currentSortDir = mobileSortDir.value;
            applySort();
            renderPage(1);
            updateSortHeaderClasses();
        });
    }

    if (els.pagerPrev) {
        els.pagerPrev.addEventListener("click", () => {
            renderPage(currentPage - 1);
        });
    }
    if (els.pagerNext) {
        els.pagerNext.addEventListener("click", () => {
            renderPage(currentPage + 1);
        });
    }

    if (els.exportBtn) {
        els.exportBtn.addEventListener("click", () => {
            exportCsv();
        });
    }
}

function exportCsv() {
    const rows = filteredMoments;
    if (!rows.length) return;

    const headers = [
        "wallet_address",
        "nft_id",
        "edition_id",
        "play_id",
        "series_id",
        "set_id",
        "tier",
        "serial_number",
        "max_mint_size",
        "owned_count",
        "first_name",
        "last_name",
        "team_name",
        "position",
        "jersey_number",
        "series_name",
        "set_name",
        "is_locked",
        "last_event_ts",
        "low_ask_usd",
        "avg_sale_usd",
        "top_sale_usd"
    ];

    const lines = [];
    lines.push(headers.join(","));

    for (const r of rows) {
        const line = headers
            .map((h) => {
                let v = r[h];
                if (v == null) v = "";
                const s = String(v).replace(/"/g, '""');
                if (s.search(/[",\n]/) >= 0) {
                    return `"${s}"`;
                }
                return s;
            })
            .join(",");
        lines.push(line);
    }

    const csv = lines.join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = "wallet-moments.csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Track which wallets have already been background-refreshed this session
const backgroundRefreshedWallets = new Set();
let backgroundRefreshInProgress = false;

// Background blockchain refresh - runs silently after initial database load
// Fetches real-time data from blockchain and updates UI if different
// Only runs ONCE per wallet per page session to avoid spamming the blockchain
async function backgroundRefreshFromBlockchain(wallet) {
    // Prevent duplicate/concurrent background refreshes
    if (backgroundRefreshInProgress) {
        console.log('[Background Refresh] Already in progress, skipping');
        return;
    }
    if (backgroundRefreshedWallets.has(wallet)) {
        console.log('[Background Refresh] Already refreshed this wallet this session, skipping');
        return;
    }

    backgroundRefreshInProgress = true;
    backgroundRefreshedWallets.add(wallet);

    try {
        console.log('[Background Refresh] Starting blockchain sync for', wallet.substring(0, 10) + '...');


        // Show subtle refresh indicator
        const refreshIndicator = document.createElement('div');
        refreshIndicator.id = 'background-refresh-indicator';
        refreshIndicator.style.cssText = `
            position: fixed;
            bottom: 1rem;
            right: 1rem;
            background: var(--surface);
            color: var(--text-muted);
            padding: 0.5rem 1rem;
            border-radius: 0.5rem;
            font-size: 0.75rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
            z-index: 1000;
            opacity: 0.9;
        `;
        refreshIndicator.innerHTML = '<span class="loading-spinner" style="width: 12px; height: 12px; border-width: 2px;"></span> Syncing with blockchain...';
        document.body.appendChild(refreshIndicator);

        // Fetch from blockchain (source=blockchain forces real-time data)
        const res = await fetch(`/api/query?wallet=${encodeURIComponent(wallet)}&source=blockchain`);
        const data = await res.json();

        // Remove indicator
        refreshIndicator.remove();

        if (!data.ok || !data.rows) {
            console.log('[Background Refresh] Blockchain query failed, keeping cached data');
            return;
        }

        const blockchainMoments = data.rows;
        const cachedCount = allMoments.length;
        const blockchainCount = blockchainMoments.length;

        // Check if data is different
        if (cachedCount === blockchainCount) {
            console.log(`[Background Refresh] ✅ Data is current (${cachedCount} moments)`);
            return;
        }

        // Data is different! Update silently
        console.log(`[Background Refresh] 🔄 Data changed: ${cachedCount} → ${blockchainCount} moments`);

        // Update global state
        allMoments = blockchainMoments;
        await attachPricesToMoments(allMoments);

        // Add owned_count to all moments
        const editionCounts = countDuplicatesByEdition(allMoments);
        for (const moment of allMoments) {
            moment.owned_count = moment.edition_id ? (editionCounts[moment.edition_id] || 1) : 1;
        }

        // Update summary with new data
        updateSummaryWithStats(wallet);

        // Also update the Total count in wallet summary to match actual moments count
        const totalChip = document.querySelector('.wallet-summary-chips .chip');
        if (totalChip && totalChip.textContent.startsWith('Total:')) {
            const lockedCount = allMoments.filter(m => m.is_locked).length;
            const unlockedCount = allMoments.length - lockedCount;

            // Update all the count chips
            const chips = document.querySelectorAll('.wallet-summary-chips .chip');
            chips.forEach(chip => {
                if (chip.textContent.startsWith('Total:')) {
                    chip.textContent = `Total: ${allMoments.length}`;
                } else if (chip.textContent.startsWith('Unlocked:')) {
                    chip.textContent = `Unlocked: ${unlockedCount}`;
                } else if (chip.textContent.startsWith('Locked:')) {
                    chip.textContent = `Locked: ${lockedCount}`;
                }
            });
        }

        // Re-apply filters and sort, then re-render
        buildFilterOptions();
        applyFilters();
        applySort();
        renderPage(currentPage);

        // Show subtle notification that data was updated
        const updateNotice = document.createElement('div');
        updateNotice.style.cssText = `
            position: fixed;
            bottom: 1rem;
            right: 1rem;
            background: var(--accent1);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 0.5rem;
            font-size: 0.75rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
            z-index: 1000;
            animation: fadeIn 0.3s ease;
        `;
        const diff = blockchainCount - cachedCount;
        const diffText = diff > 0 ? `+${diff}` : `${diff}`;
        updateNotice.textContent = `✓ Synced: ${diffText} moment${Math.abs(diff) !== 1 ? 's' : ''}`;
        document.body.appendChild(updateNotice);

        // Auto-remove after 3 seconds
        setTimeout(() => updateNotice.remove(), 3000);

    } catch (err) {
        console.warn('[Background Refresh] Error:', err.message);
        // Silently fail - cached data is still valid
        const indicator = document.getElementById('background-refresh-indicator');
        if (indicator) indicator.remove();
    } finally {
        backgroundRefreshInProgress = false;
    }
}

// This is what the HTML calls
window.runQuery = async function runQuery(walletRaw) {
    const els = getEls();
    const wallet = (walletRaw || "").trim().toLowerCase();
    if (!wallet) return;

    // Show loading animation
    showLoadingAnimation();

    if (els.pagerInfo) {
        els.pagerInfo.textContent = "";
    }

    // Clear any previous error state
    const errorDiv = document.getElementById("wallet-error");
    if (errorDiv) {
        errorDiv.remove();
    }

    try {
        await fetchWalletSummary(wallet);
        // Use cached database data for FAST loading
        // Data is kept up-to-date by sync scripts (real-time WebSocket events + periodic Snowflake sync)
        allMoments = await fetchWalletMoments(wallet, false);

        await attachPricesToMoments(allMoments);

        // Add owned_count to all moments for sorting
        const editionCounts = countDuplicatesByEdition(allMoments);
        for (const moment of allMoments) {
            moment.owned_count = moment.edition_id ? (editionCounts[moment.edition_id] || 1) : 1;
        }

        // Update summary with mini stats now that we have moments
        updateSummaryWithStats(wallet);

        buildFilterOptions();
        applyFilters();
        applySort();

        // Hide loading animation before rendering
        hideLoadingAnimation();
        renderPage(1);

        // STALE-WHILE-REVALIDATE: Fire off background blockchain refresh
        // This runs silently and updates the UI only if data has changed
        backgroundRefreshFromBlockchain(wallet);
    } catch (err) {
        console.error("runQuery error", err);
        const errorMsg = err.message || String(err);

        // Hide loading animation on error
        hideLoadingAnimation();

        if (els.title) {
            els.title.textContent = "Failed to load wallet";
        }

        // Show user-friendly error with retry button
        if (els.tbody) {
            els.tbody.innerHTML = "";
            const errorDiv = document.createElement("div");
            errorDiv.id = "wallet-error";
            errorDiv.style.cssText = "padding: 2rem; text-align: center; color: var(--text-muted);";
            errorDiv.innerHTML = `
                <div style="margin-bottom: 1rem; color: var(--danger);">❌ ${errorMsg}</div>
                <button class="btn-primary" onclick="window.runQuery('${wallet}')" style="margin-top: 0.5rem;">Retry</button>
            `;
            els.tbody.appendChild(errorDiv);
        }

        // Also show error in mobile cards
        if (els.mobileCards) {
            els.mobileCards.innerHTML = `
                <div style="padding: 2rem; text-align: center; color: var(--text-muted);">
                    <div style="margin-bottom: 1rem; color: var(--danger);">❌ ${errorMsg}</div>
                    <button class="btn-primary" onclick="window.runQuery('${wallet}')" style="margin-top: 0.5rem;">Retry</button>
                </div>
            `;
        }
    }
};

document.addEventListener("DOMContentLoaded", () => {
    wireEvents();
    updateSortHeaderClasses();
    updateMobileSortDropdowns();
});
