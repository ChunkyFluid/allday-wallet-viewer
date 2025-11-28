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
        <div class="brand">
          <div class="brand-logo">🐱</div>
          <div class="brand-title">CHUNKY'S NFLAD VIEWER</div>
        </div>

        <nav class="main-nav">
          ${navLink("/", "Wallet")}
          ${navLink("/profiles.html", "Profiles")}
          ${navLink("/top-holders.html", "Top holders")}
          ${navLink("/explorer.html", "Browse")}
          <a href="/sniper.html" class="${path === '/sniper.html' ? 'active nav-hot' : 'nav-hot'}">🎯 Sniper</a>
          ${navLink("/live-transactions.html", "Live")}
          ${navLink("/insights.html", "Insights")}
          ${navLink("/faq.html", "FAQ")}
          ${navLink("/contact.html", "Contact")}
          ${navLink("/login.html", "Login", { id: "nav-account-link" })}
        </nav>

      </header>
    `;
    }

    function normalizePath(path) {
        if (!path) return "/";
        // Treat /index.html as /
        if (path === "/index.html") return "/";
        return path;
    }

    async function updateNavAccount() {
        const link = document.getElementById("nav-account-link");
        if (!link) return;

        try {
            const res = await fetch("/api/me");
            const data = await res.json();

            if (data && data.user) {
                link.href = "/login.html";
                link.textContent = data.user.email;
            } else {
                link.href = "/login.html";
                link.textContent = "Login";
            }
        } catch (err) {
            console.error("updateNavAccount error:", err);
        }
    }

    function initHeader() {
        const container = document.getElementById("app-header");
        if (!container) return;

        container.innerHTML = buildHeaderHTML();
        updateNavAccount();
    }

    // Run immediately (scripts are loaded at end of body)
    initHeader();
})();
