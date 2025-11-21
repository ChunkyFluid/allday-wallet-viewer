document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("profile-search-form");
  const input = document.getElementById("profile-search-input");
  const table = document.getElementById("profiles-table");
  const tbody = document.getElementById("profiles-tbody");
  const empty = document.getElementById("profiles-empty");

  const summaryEmpty = document.getElementById("profile-summary-empty");
  const summaryBox = document.getElementById("profile-summary");
  const psTotal = document.getElementById("ps-total");
  const psLocked = document.getElementById("ps-locked");
  const psUnlocked = document.getElementById("ps-unlocked");
  const psCommon = document.getElementById("ps-common");
  const psUncommon = document.getElementById("ps-uncommon");
  const psRare = document.getElementById("ps-rare");
  const psLegendary = document.getElementById("ps-legendary");
  const psUltimate = document.getElementById("ps-ultimate");
  const psDisplayName = document.getElementById("ps-display-name");
  const psWallet = document.getElementById("ps-wallet");
  const psOpenMain = document.getElementById("ps-open-main");

  async function fetchSummary(wallet) {
    try {
      const url = `/api/wallet-summary?wallet=${encodeURIComponent(wallet)}`;
      const res = await fetch(url);
      const data = await res.json();
      if (!data.ok) {
        throw new Error(data.error || "Failed to load summary");
      }

      const stats = data.stats || {};
      const byTier = stats.byTier || {};

      psTotal.textContent = stats.momentsTotal ?? 0;
      psLocked.textContent = stats.lockedCount ?? 0;
      psUnlocked.textContent = stats.unlockedCount ?? 0;
      psCommon.textContent = byTier.Common ?? 0;
      psUncommon.textContent = byTier.Uncommon ?? 0;
      psRare.textContent = byTier.Rare ?? 0;
      psLegendary.textContent = byTier.Legendary ?? 0;
      psUltimate.textContent = byTier.Ultimate ?? 0;

      psDisplayName.textContent = data.displayName ? data.displayName : "";
      psWallet.textContent = data.wallet || wallet;
      psOpenMain.href = `/?wallet=${encodeURIComponent(wallet)}`;

      summaryEmpty.style.display = "none";
      summaryBox.style.display = "block";
    } catch (err) {
      console.error(err);
      summaryEmpty.textContent = "Failed to load wallet summary.";
      summaryEmpty.style.display = "block";
      summaryBox.style.display = "none";
    }
  }

  function renderProfiles(rows) {
    tbody.innerHTML = "";
    if (!rows || !rows.length) {
      table.style.display = "none";
      empty.style.display = "block";
      return;
    }
    empty.style.display = "none";
    table.style.display = "table";

    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const dn = row.display_name || "";
      const wallet = row.wallet_address || "";

      tr.innerHTML = `
        <td>${dn || "<span style='color:var(--text-muted);font-style:italic;'>Unnamed</span>"}</td>
        <td><code>${wallet}</code></td>
        <td><button class="btn-primary btn-xs" data-wallet="${wallet}">View</button></td>
      `;
      tbody.appendChild(tr);
    });

    tbody.querySelectorAll("button[data-wallet]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const wallet = btn.getAttribute("data-wallet");
        fetchSummary(wallet);
      });
    });
  }

  if (form) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const q = input.value.trim();
      if (!q) return;

      summaryEmpty.style.display = "block";
      summaryBox.style.display = "none";
      summaryEmpty.textContent = "Loading wallet summary…";

      try {
        const res = await fetch(`/api/search-profiles?q=${encodeURIComponent(q)}`);
        const data = await res.json();
        if (!data.ok) {
          throw new Error(data.error || "Search failed");
        }
        renderProfiles(data.rows || []);
        if (data.rows && data.rows[0]) {
          const firstWallet = data.rows[0].wallet_address;
          fetchSummary(firstWallet);
        }
      } catch (err) {
        console.error(err);
        empty.textContent = "Search failed. Please try again.";
        empty.style.display = "block";
        table.style.display = "none";
      }
    });
  }
});
