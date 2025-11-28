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
          ${navLink("/", "Wallet")}
          ${navLink("/top-holders.html", "Leaderboard")}
          ${navLink("/explorer.html", "Browse")}
          <a href="/sniper.html" class="${path === '/sniper.html' ? 'active nav-hot' : 'nav-hot'}">🎯 Sniper</a>
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
        if (path === "/index.html") return "/";
        return path;
    }

    async function updateNavAccount() {
        const link = document.getElementById("nav-account-link");
        if (!link) return;

        try {
            const res = await fetch("/api/me");
            const data = await res.json();

            if (data && data.ok && data.user) {
                link.href = "/login.html";
                link.textContent = "⚙️ Account";
                link.title = data.user.email;
            } else {
                link.href = "/login.html";
                link.textContent = "Login";
            }
        } catch (err) {
            console.error("updateNavAccount error:", err);
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
