// public/layout.js
(function () {
    function buildHeaderHTML() {
        const path = normalizePath(window.location.pathname);

        function navLink(href, label, opts = {}) {
            const isActive = href === "/" ? path === "/" || path === "/index.html" : path === href;
            const activeClass = isActive ? ' class="active"' : "";
            const idAttr = opts.id ? ` id="${opts.id}"` : "";
            return `<a href="${href}"${idAttr}${activeClass}>${label}</a>`;
        }

        return `
      <header class="app-header">
        <a href="/" class="brand">
          <div class="brand-logo">🐱</div>
          <div class="brand-title desktop-only">CHUNKY'S NFLAD VIEWER</div>
          <div class="brand-title mobile-only">Chunky Viewer</div>
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
          <a href="/trade-analyzer.html" class="${path === '/trade-analyzer.html' ? 'active' : ''}">🔄 Trades</a>
          <a href="/playbook.html" class="${path === '/playbook.html' ? 'active' : ''}">📖 Playbook</a>
          <a href="/challenges.html" class="${path === '/challenges.html' ? 'active' : ''}">🎮 Challenges</a>
          <a href="/insights.html" class="${path === '/insights.html' ? 'active' : ''}">💡 Insights</a>
          <a href="/faq.html" class="${path === '/faq.html' ? 'active' : ''}">❓ FAQ</a>
          <a href="/login.html" id="nav-account-link" class="${path === '/login.html' ? 'active' : ''}">🔑 Login</a>
        </nav>
      </header>
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

    function initHeader() {
        const container = document.getElementById("app-header");
        if (!container) return;

        container.innerHTML = buildHeaderHTML();
        updateNavAccount();
        initMobileMenu();
    }

    // Run immediately
    initHeader();
})();
