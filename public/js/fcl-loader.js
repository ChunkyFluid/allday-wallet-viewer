// FCL Loader - Simple wrapper that loads FCL from a working source
// This is a temporary solution until we get bundling working

(function() {
  'use strict';
  
  // Try to load FCL from multiple sources
  const sources = [
    'https://unpkg.com/@onflow/fcl@1.20.6/dist/fcl.umd.min.js',
    'https://cdn.jsdelivr.net/npm/@onflow/fcl@1.20.6/dist/fcl.umd.min.js'
  ];
  
  let loaded = false;
  let currentSource = 0;
  
  function tryLoad() {
    if (loaded || currentSource >= sources.length) return;
    
    const script = document.createElement('script');
    script.src = sources[currentSource];
    script.crossOrigin = 'anonymous';
    
    script.onload = function() {
      // Wait a bit for FCL to initialize
      setTimeout(() => {
        let fcl = window.fcl || window.FCL;
        
        if (fcl && typeof fcl.config === 'function') {
          // Configure FCL
          fcl.config()
            .put("accessNode.api", "https://rest-mainnet.onflow.org")
            .put("discovery.wallet", "https://fcl-discovery.onflow.org/authn")
            .put("app.detail.title", "Chunky's NFLAD Viewer")
            .put("app.detail.icon", window.location.origin + "/favicon.ico");
          
          window.fcl = fcl;
          console.log('✅ FCL loaded and configured from:', sources[currentSource]);
          loaded = true;
          
          // Dispatch event
          window.dispatchEvent(new Event('fcl-loaded'));
        } else {
          console.warn('FCL loaded but not accessible');
          currentSource++;
          if (currentSource < sources.length) {
            tryLoad();
          }
        }
      }, 100);
    };
    
    script.onerror = function() {
      console.warn('Failed to load FCL from:', sources[currentSource]);
      currentSource++;
      if (currentSource < sources.length) {
        tryLoad();
      } else {
        console.error('All FCL CDN sources failed');
      }
    };
    
    document.head.appendChild(script);
  }
  
  // Start loading
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', tryLoad);
  } else {
    tryLoad();
  }
})();

