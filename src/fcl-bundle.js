// FCL Bundle Entry Point
// This file imports FCL and exposes it globally for use in login.html

import * as fcl from '@onflow/fcl';

// Configure FCL for Flow Mainnet - Dapper Wallet only
fcl.config()
  .put("accessNode.api", "https://rest-mainnet.onflow.org")
  // Discovery endpoint for wallet selection UI
  .put("discovery.wallet", "https://fcl-discovery.onflow.org/authn")
  // Discovery API endpoint for programmatic access (required for snapshot())
  .put("discovery.authn.endpoint", "https://fcl-discovery.onflow.org/api/authn")
  // Include only Dapper Wallet (using wallet UID - recommended)
  .put("discovery.authn.include", ["dapper-wallet"])
  // Exclude all other default wallets
  .put("discovery.authn.exclude", ["flow-wallet", "nufi", "blocto"])
  .put("app.detail.title", "Chunky's NFLAD Viewer")
  .put("app.detail.icon", typeof window !== 'undefined' ? window.location.origin + "/favicon.ico" : "/favicon.ico")
  .put("service.OpenID.scopes", "email");

// Expose FCL globally - ensure it's the actual FCL object, not the module wrapper
if (typeof window !== 'undefined') {
  // Direct assignment - esbuild will bundle this correctly
  window.fcl = fcl;
  // Also expose as default for compatibility
  if (fcl && typeof fcl === 'object' && !fcl.config) {
    // If fcl is a module wrapper, extract the actual FCL
    window.fcl = fcl.default || fcl.fcl || fcl;
  }
  console.log('✅ FCL loaded and configured');
}

// Export for module systems
export default fcl;
export { fcl };

