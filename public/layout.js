// public/layout.js
(function () {
    function buildHeaderHTML() {
        const path = normalizePath(window.location.pathname);

        return `
      <header class="app-header">
        <a href="/" class="brand">
          <span style="font-size: 1.8rem;">🐱</span>
          <div class="brand-title desktop-only">CHUNKY VIEWER</div>
          <div class="brand-title mobile-only">Chunky</div>
        </a>

        <button class="menu-toggle" id="menu-toggle" aria-label="Toggle menu">☰</button>

        <nav class="main-nav" id="main-nav">
          <a href="/" class="${path === '/' || path === '/index.html' ? 'active' : ''}">👛 Wallet</a>
          <a href="/top-holders.html" class="${path === '/top-holders.html' ? 'active' : ''}">🏅 Leaderboard</a>
          <a href="/explorer.html" class="${path === '/explorer.html' ? 'active' : ''}">🔎 Browse</a>
          <a href="/sniper.html" class="${path === '/sniper.html' ? 'active nav-hot' : 'nav-hot'}">🎯 Sniper</a>
          <a href="/set-completion.html" class="${path === '/set-completion.html' ? 'active' : ''}">📊 Sets</a>
          <a href="/serial-finder.html" class="${path === '/serial-finder.html' ? 'active' : ''}">🏆 Serials</a>
          <a href="/wallet-compare.html" class="${path === '/wallet-compare.html' ? 'active' : ''}">⚖️ Compare</a>
          <a href="/rarity-score.html" class="${path === '/rarity-score.html' ? 'active' : ''}">📈 Rarity</a>
          <a href="/insights.html" class="${path === '/insights.html' ? 'active' : ''}">💡 Insights</a>
          <a href="/faq.html" class="${path === '/faq.html' ? 'active' : ''}">❓ FAQ</a>
          <a href="/login.html" id="nav-account-link" class="${path === '/login.html' ? 'active' : ''}">🔑 Login</a>
        </nav>
      </header>
      <div class="site-banner">
        Website made with ❤️ by Chunky. Its an ongoing Project. Not my job/income. May be paid to use in future. Donations Welcome.</span>
        <a href="https://www.paypal.com/ncp/payment/7L6XYSZY9TQBJ" target="_blank" class="donate-link">💖 Donate</a>
      </div>
    `;
    }

    function buildFooterHTML() {
        return `
      <footer style="margin-top: 3rem; padding: 2rem 1rem; text-align: center; border-top: 1px solid rgba(255,255,255,0.08); color: rgba(255,255,255,0.5); font-size: 0.8rem; background: rgba(0,0,0,0.2);">
        <div style="display: flex; justify-content: center; align-items: center; gap: 1.5rem; flex-wrap: wrap; max-width: 1200px; margin: 0 auto;">
          <div style="display: flex; align-items: center; gap: 0.5rem;">
            <span style="font-size: 1.1rem;">👁️</span>
            <span id="visit-count" style="font-weight: 600; color: #fff; font-family: 'JetBrains Mono', monospace;">-</span> 
            <span>unique visits</span>
          </div>
          <span style="opacity: 0.3;">•</span>
          <div style="display: flex; align-items: center; gap: 0.5rem;">
            <span style="font-size: 1.1rem;">📊</span>
            <span id="total-hits" style="font-weight: 600; color: #fff; font-family: 'JetBrains Mono', monospace;">-</span> 
            <span>total visits</span>
          </div>
          <span style="opacity: 0.3;">•</span>
          <span>Built with ❤️ by Chunky</span>
          <span style="opacity: 0.3;">•</span>
          <span>© 2025 Chunky Viewer</span>
        </div>
      </footer>
    `;
    }

    function normalizePath(path) {
        if (!path) return "/";
        if (path === "/index.html") return "/";
        return path;
    }

    async function updateNavAccount() {
        const link = document.getElementById("nav-account-link");
        if (!link) return;

        try {
            const res = await fetch("/api/me");
            const data = await res.json();

            if (data && data.ok && data.user && data.user.default_wallet_address) {
                link.href = "/login.html";

                // Fetch display name from wallet_profiles (same way wallet page does it)
                const wallet = data.user.default_wallet_address.toLowerCase();
                try {
                    const profileRes = await fetch(`/api/wallet-profile?wallet=${encodeURIComponent(wallet)}`);
                    const profileData = await profileRes.json();

                    if (profileData && profileData.ok && profileData.profile && profileData.profile.display_name) {
                        link.textContent = profileData.profile.display_name;
                    } else {
                        // Fallback to shortened wallet address
                        const walletAddr = wallet.replace(/^(flow|dapper):/, "");
                        link.textContent = walletAddr.length > 10
                            ? walletAddr.slice(0, 6) + "..." + walletAddr.slice(-4)
                            : walletAddr;
                    }
                } catch (profileErr) {
                    // Fallback to shortened wallet address if profile fetch fails
                    const walletAddr = wallet.replace(/^(flow|dapper):/, "");
                    link.textContent = walletAddr.length > 10
                        ? walletAddr.slice(0, 6) + "..." + walletAddr.slice(-4)
                        : walletAddr;
                }

                link.title = `Logged in · Wallet: ${wallet}`;
            } else {
                link.href = "/login.html";
                link.textContent = "Login";
            }
        } catch (err) {
            console.error("updateNavAccount error:", err);
            link.href = "/login.html";
            link.textContent = "Login";
        }
    }

    function initMobileMenu() {
        const toggle = document.getElementById("menu-toggle");
        const nav = document.getElementById("main-nav");

        if (!toggle || !nav) return;

        toggle.addEventListener("click", () => {
            const isOpen = nav.classList.toggle("open");
            toggle.textContent = isOpen ? "✕" : "☰";
            toggle.setAttribute("aria-expanded", isOpen);

            // Prevent body scroll when menu is open
            document.body.style.overflow = isOpen ? "hidden" : "";
        });

        // Close menu when clicking a link
        nav.querySelectorAll("a").forEach(link => {
            link.addEventListener("click", () => {
                nav.classList.remove("open");
                toggle.textContent = "☰";
                document.body.style.overflow = "";
            });
        });

        // Close menu on escape key
        document.addEventListener("keydown", (e) => {
            if (e.key === "Escape" && nav.classList.contains("open")) {
                nav.classList.remove("open");
                toggle.textContent = "☰";
                document.body.style.overflow = "";
            }
        });
    }

    function initVisitCounter() {
        const counterEl = document.getElementById('visit-count');
        const hitsEl = document.getElementById('total-hits');
        if (!counterEl && !hitsEl) return;

        // Record visit and get count
        fetch('/api/visit', { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.count !== undefined && counterEl) {
                    counterEl.textContent = data.count.toLocaleString();
                }
                if (data.totalHits !== undefined && hitsEl) {
                    hitsEl.textContent = data.totalHits.toLocaleString();
                }
            })
            .catch(() => {
                // Fallback: try to get count without recording
                fetch('/api/visit-count')
                    .then(r => r.json())
                    .then(data => {
                        if (data.count !== undefined && counterEl) {
                            counterEl.textContent = data.count.toLocaleString();
                        }
                        if (data.totalHits !== undefined && hitsEl) {
                            hitsEl.textContent = data.totalHits.toLocaleString();
                        }
                    })
                    .catch(() => {
                        if (counterEl) counterEl.textContent = '?';
                        if (hitsEl) hitsEl.textContent = '?';
                    });
            });
    }

    function initLayout() {
        // Init Header
        const headerContainer = document.getElementById("app-header");
        if (headerContainer) {
            headerContainer.innerHTML = buildHeaderHTML();
            updateNavAccount();
            initMobileMenu();
        }

        // Init Footer
        let footerContainer = document.getElementById("app-footer");
        if (!footerContainer) {
            // If no explicit footer container, append to body
            footerContainer = document.createElement('div');
            footerContainer.id = "app-footer";
            document.body.appendChild(footerContainer);
        }
        footerContainer.innerHTML = buildFooterHTML();
        initVisitCounter();
    }

    // Run immediately
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initLayout);
    } else {
        initLayout();
    }
})();
