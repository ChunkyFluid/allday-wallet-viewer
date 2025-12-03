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
      '/sniper.html': 'nav-sniper',
      '/insights.html': 'nav-insights',
      '/live-transactions.html': 'nav-live',
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
      const res = await fetch("/api/me", { credentials: "include" });
      const data = await res.json();

      if (data && data.ok && data.user) {
        // Show display name if available, otherwise email or wallet address
        let displayText;
        if (data.user.display_name && data.user.display_name.trim()) {
          // Use display_name if available (this is what we want to show)
          displayText = data.user.display_name;
        } else if (data.user.default_wallet_address) {
          // Fallback: show shortened wallet address without "flow:" prefix
          const walletAddr = data.user.default_wallet_address;
          if (walletAddr.length > 10) {
            displayText = walletAddr.slice(0, 6) + "..." + walletAddr.slice(-4);
          } else {
            displayText = walletAddr;
          }
        } else if (data.user.email) {
          // Handle Flow wallet addresses (flow:0x... or dapper:0x...)
          if (data.user.email.startsWith("flow:") || data.user.email.startsWith("dapper:")) {
            const walletAddr = data.user.email.replace(/^(flow|dapper):/, "");
            // Show shortened version: 0x1234...5678
            if (walletAddr.length > 10) {
              displayText = walletAddr.slice(0, 6) + "..." + walletAddr.slice(-4);
            } else {
              displayText = walletAddr;
            }
          } else {
            displayText = data.user.email;
          }
        } else {
          displayText = "Account";
        }
        
        link.href = "/login.html";
        link.textContent = displayText;
        link.title = data.user.default_wallet_address 
          ? `Logged in · Default wallet: ${data.user.default_wallet_address}`
          : "Logged in";
      } else {
        link.href = "/login.html";
        link.textContent = "Login";
        link.title = "";
      }
    } catch (err) {
      console.error("updateNavAccount error:", err);
      link.href = "/login.html";
      link.textContent = "Login";
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

