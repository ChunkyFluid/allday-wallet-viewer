/**
 * Multi-Select Dropdown Component
 * Creates checkbox-based multi-select dropdowns that match the dark theme
 */

class MultiSelect {
  constructor(container, options = {}) {
    this.container = typeof container === 'string' ? document.getElementById(container) : container;
    this.options = {
      placeholder: options.placeholder || 'All',
      allLabel: options.allLabel || 'All',
      onChange: options.onChange || (() => {}),
      maxDisplay: options.maxDisplay || 2, // Max items to show before "X selected"
      ...options
    };
    this.items = [];
    this.selectedValues = new Set();
    this.isOpen = false;
    this.init();
  }

  init() {
    this.container.classList.add('multi-select');
    this.container.innerHTML = `
      <button type="button" class="multi-select-btn">
        <span class="multi-select-text">${this.options.placeholder}</span>
        <span class="multi-select-arrow">▼</span>
      </button>
      <div class="multi-select-dropdown">
        <div class="multi-select-options"></div>
      </div>
    `;

    this.btn = this.container.querySelector('.multi-select-btn');
    this.textEl = this.container.querySelector('.multi-select-text');
    this.dropdown = this.container.querySelector('.multi-select-dropdown');
    this.optionsContainer = this.container.querySelector('.multi-select-options');

    this.btn.addEventListener('click', (e) => {
      e.stopPropagation();
      this.toggle();
    });

    // Close when clicking outside
    document.addEventListener('click', (e) => {
      if (!this.container.contains(e.target)) {
        this.close();
      }
    });
  }

  setItems(items) {
    this.items = items;
    this.render();
  }

  render() {
    this.optionsContainer.innerHTML = this.items.map(item => {
      const value = typeof item === 'object' ? item.value : item;
      const label = typeof item === 'object' ? item.label : item;
      const checked = this.selectedValues.has(value) ? 'checked' : '';
      return `
        <label class="multi-select-option">
          <input type="checkbox" value="${value}" ${checked}>
          <span class="multi-select-checkbox"></span>
          <span class="multi-select-label">${label}</span>
        </label>
      `;
    }).join('');

    // Add event listeners
    this.optionsContainer.querySelectorAll('input').forEach(input => {
      input.addEventListener('change', () => {
        if (input.checked) {
          this.selectedValues.add(input.value);
        } else {
          this.selectedValues.delete(input.value);
        }
        this.updateText();
        this.options.onChange(this.getValues());
      });
    });

    this.updateText();
  }

  updateText() {
    const count = this.selectedValues.size;
    if (count === 0) {
      this.textEl.textContent = this.options.placeholder;
      this.textEl.classList.remove('has-selection');
    } else if (count <= this.options.maxDisplay) {
      const labels = Array.from(this.selectedValues).map(v => {
        const item = this.items.find(i => (typeof i === 'object' ? i.value : i) === v);
        return typeof item === 'object' ? item.label : item;
      });
      this.textEl.textContent = labels.join(', ');
      this.textEl.classList.add('has-selection');
    } else {
      this.textEl.textContent = `${count} selected`;
      this.textEl.classList.add('has-selection');
    }
  }

  toggle() {
    this.isOpen ? this.close() : this.open();
  }

  open() {
    this.isOpen = true;
    this.dropdown.classList.add('open');
    this.btn.classList.add('open');
  }

  close() {
    this.isOpen = false;
    this.dropdown.classList.remove('open');
    this.btn.classList.remove('open');
  }

  getValues() {
    return Array.from(this.selectedValues);
  }

  setValues(values) {
    this.selectedValues = new Set(values);
    this.render();
  }

  clear() {
    this.selectedValues.clear();
    this.render();
    this.options.onChange([]);
  }
}

// CSS styles for multi-select (injected once)
if (!document.getElementById('multi-select-styles')) {
  const style = document.createElement('style');
  style.id = 'multi-select-styles';
  style.textContent = `
    .multi-select {
      position: relative;
      width: 100%;
    }

    .multi-select-btn {
      width: 100%;
      background: #050a18;
      border-radius: 10px;
      border: 1px solid var(--border, #1b2236);
      padding: 0.5rem 0.65rem;
      color: var(--text, #f8f9ff);
      font-size: 0.85rem;
      outline: none;
      font-family: inherit;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      text-align: left;
      transition: border-color 0.15s;
    }

    .multi-select-btn:hover,
    .multi-select-btn:focus,
    .multi-select-btn.open {
      border-color: var(--accent, #6f42c1);
    }

    .multi-select-text {
      flex: 1;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: var(--text-muted, #8c95b8);
    }

    .multi-select-text.has-selection {
      color: var(--text, #f8f9ff);
    }

    .multi-select-arrow {
      font-size: 0.65rem;
      color: var(--text-muted, #8c95b8);
      transition: transform 0.15s;
    }

    .multi-select-btn.open .multi-select-arrow {
      transform: rotate(180deg);
    }

    .multi-select-dropdown {
      position: absolute;
      top: 100%;
      left: 0;
      right: 0;
      background: #0b1020;
      border: 1px solid var(--border, #1b2236);
      border-radius: 10px;
      margin-top: 4px;
      max-height: 250px;
      overflow-y: auto;
      z-index: 1000;
      display: none;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
    }

    .multi-select-dropdown.open {
      display: block;
    }

    .multi-select-options {
      padding: 6px;
    }

    .multi-select-option {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      border-radius: 6px;
      cursor: pointer;
      transition: background 0.15s;
      user-select: none;
    }

    .multi-select-option:hover {
      background: rgba(111, 66, 193, 0.15);
    }

    .multi-select-option input {
      display: none;
    }

    .multi-select-checkbox {
      width: 18px;
      height: 18px;
      border: 2px solid var(--text-muted, #8c95b8);
      border-radius: 4px;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      transition: all 0.15s;
    }

    .multi-select-option input:checked + .multi-select-checkbox {
      background: linear-gradient(135deg, #6f42c1, #007bff);
      border-color: transparent;
    }

    .multi-select-option input:checked + .multi-select-checkbox::after {
      content: "✓";
      color: white;
      font-size: 12px;
      font-weight: bold;
    }

    .multi-select-label {
      font-size: 0.85rem;
      color: var(--text, #f8f9ff);
    }

    /* Scrollbar styling */
    .multi-select-dropdown::-webkit-scrollbar {
      width: 6px;
    }

    .multi-select-dropdown::-webkit-scrollbar-track {
      background: transparent;
    }

    .multi-select-dropdown::-webkit-scrollbar-thumb {
      background: rgba(111, 66, 193, 0.4);
      border-radius: 3px;
    }

    .multi-select-dropdown::-webkit-scrollbar-thumb:hover {
      background: rgba(111, 66, 193, 0.6);
    }
  `;
  document.head.appendChild(style);
}

// Export for use
window.MultiSelect = MultiSelect;

