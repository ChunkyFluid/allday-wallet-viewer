// Reusable wallet search component with autocomplete
// Usage: initWalletSearch(inputId, onSelect) 
//   - inputId: ID of the input element
//   - onSelect: callback function(walletAddress, displayName) when a wallet is selected

function initWalletSearch(inputId, onSelect) {
  const input = document.getElementById(inputId);
  if (!input) return;
  
  // Create dropdown container
  const wrapper = document.createElement('div');
  wrapper.className = 'wallet-search-wrapper';
  input.parentNode.insertBefore(wrapper, input);
  wrapper.appendChild(input);
  
  const dropdown = document.createElement('div');
  dropdown.className = 'wallet-search-dropdown';
  wrapper.appendChild(dropdown);
  
  let debounceTimer = null;
  let selectedIndex = -1;
  let results = [];
  
  // Search as user types
  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    const q = input.value.trim();
    
    if (q.length < 2) {
      dropdown.innerHTML = '';
      dropdown.style.display = 'none';
      return;
    }
    
    // If it looks like a wallet address, don't search
    if (q.startsWith('0x') && q.length > 10) {
      dropdown.innerHTML = '';
      dropdown.style.display = 'none';
      return;
    }
    
    debounceTimer = setTimeout(() => searchProfiles(q), 200);
  });
  
  async function searchProfiles(q) {
    try {
      const res = await fetch(`/api/search-profiles?q=${encodeURIComponent(q)}&limit=8`);
      const data = await res.json();
      
      if (!data.ok || !data.rows || data.rows.length === 0) {
        dropdown.innerHTML = '<div class="wallet-search-empty">No results found</div>';
        dropdown.style.display = 'block';
        results = [];
        return;
      }
      
      results = data.rows;
      selectedIndex = -1;
      renderDropdown();
    } catch (err) {
      console.error('Search error:', err);
      dropdown.innerHTML = '<div class="wallet-search-empty">Search failed</div>';
      dropdown.style.display = 'block';
    }
  }
  
  function renderDropdown() {
    dropdown.innerHTML = results.map((r, i) => `
      <div class="wallet-search-item ${i === selectedIndex ? 'selected' : ''}" data-index="${i}">
        <div class="wallet-search-name">${escapeHtml(r.display_name || 'Unknown')}</div>
        <div class="wallet-search-addr">${r.wallet_address}</div>
      </div>
    `).join('');
    dropdown.style.display = 'block';
    
    // Add click handlers
    dropdown.querySelectorAll('.wallet-search-item').forEach(item => {
      item.addEventListener('click', () => {
        const idx = parseInt(item.dataset.index);
        selectResult(idx);
      });
    });
  }
  
  function selectResult(index) {
    if (index >= 0 && index < results.length) {
      const r = results[index];
      // Show name and shortened address in input
      const shortAddr = r.wallet_address.slice(0, 6) + '...' + r.wallet_address.slice(-4);
      input.value = r.display_name ? `${r.display_name} (${shortAddr})` : r.wallet_address;
      // Store the full wallet address as a data attribute for retrieval
      input.dataset.walletAddress = r.wallet_address;
      input.dataset.displayName = r.display_name || '';
      dropdown.style.display = 'none';
      if (onSelect) onSelect(r.wallet_address, r.display_name);
    }
  }
  
  // Keyboard navigation
  input.addEventListener('keydown', (e) => {
    if (dropdown.style.display !== 'block' || results.length === 0) return;
    
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      selectedIndex = Math.min(selectedIndex + 1, results.length - 1);
      renderDropdown();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      selectedIndex = Math.max(selectedIndex - 1, 0);
      renderDropdown();
    } else if (e.key === 'Enter' && selectedIndex >= 0) {
      e.preventDefault();
      selectResult(selectedIndex);
    } else if (e.key === 'Escape') {
      dropdown.style.display = 'none';
    }
  });
  
  // Close dropdown when clicking outside
  document.addEventListener('click', (e) => {
    if (!wrapper.contains(e.target)) {
      dropdown.style.display = 'none';
    }
  });
  
  // Allow pressing Enter on wallet address to submit
  input.addEventListener('keypress', (e) => {
    if (e.key === 'Enter' && dropdown.style.display !== 'block') {
      const val = input.value.trim();
      if (val && onSelect) {
        onSelect(val, null);
      }
    }
  });
  
  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
}

// Add styles
const walletSearchStyles = document.createElement('style');
walletSearchStyles.textContent = `
  .wallet-search-wrapper {
    position: relative;
    flex: 1;
    min-width: 250px;
  }
  
  .wallet-search-wrapper input {
    width: 100%;
    box-sizing: border-box;
  }
  
  .wallet-search-dropdown {
    display: none;
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    background: rgba(11, 16, 32, 0.98);
    border: 1px solid rgba(255, 255, 255, 0.15);
    border-radius: 8px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
    z-index: 1000;
    max-height: 300px;
    overflow-y: auto;
    margin-top: 4px;
  }
  
  .wallet-search-item {
    padding: 10px 12px;
    cursor: pointer;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    transition: background 0.15s;
  }
  
  .wallet-search-item:last-child {
    border-bottom: none;
  }
  
  .wallet-search-item:hover,
  .wallet-search-item.selected {
    background: rgba(37, 99, 235, 0.2);
  }
  
  .wallet-search-name {
    font-weight: 600;
    color: #f8f9ff;
    margin-bottom: 2px;
  }
  
  .wallet-search-addr {
    font-size: 0.75rem;
    color: #8c95b8;
    font-family: monospace;
  }
  
  .wallet-search-empty {
    padding: 12px;
    color: #8c95b8;
    text-align: center;
    font-size: 0.9rem;
  }
`;
document.head.appendChild(walletSearchStyles);

