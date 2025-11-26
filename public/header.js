// Shared header functionality
(function() {
  // Set active nav link based on current page
  function setActiveNavLink() {
    const path = window.location.pathname;
    const navLinks = {
      '/': 'nav-wallet',
      '/index.html': 'nav-wallet',
      '/top-holders.html': 'nav-top-holders',
      '/explorer.html': 'nav-explorer',
      '/insights.html': 'nav-insights',
      '/faq.html': 'nav-faq',
      '/contact.html': 'nav-contact',
      '/login.html': 'nav-account-link'
    };

    // Remove active class from all nav links
    document.querySelectorAll('.main-nav a').forEach(link => {
      link.classList.remove('active');
    });

    // Add active class to current page
    const activeId = navLinks[path];
    if (activeId) {
      const activeLink = document.getElementById(activeId);
      if (activeLink) {
        activeLink.classList.add('active');
      }
    }
  }

  // Update nav account link
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

  // Wire up wallet form
  function wireWalletForm() {
    const form = document.getElementById("wallet-form");
    const input = document.getElementById("wallet-input");
    
    if (form && input) {
      form.addEventListener("submit", (e) => {
        e.preventDefault();
        const wallet = (input.value || "").trim();
        if (wallet) {
          window.location.href = `/?wallet=${encodeURIComponent(wallet)}`;
        }
      });
    }
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      setActiveNavLink();
      updateNavAccount();
      wireWalletForm();
    });
  } else {
    setActiveNavLink();
    updateNavAccount();
    wireWalletForm();
  }
})();

