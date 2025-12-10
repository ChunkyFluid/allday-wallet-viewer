// Minimal FCL bundle entry for browser builds
import * as fcl from "@onflow/fcl";

// Configure defaults for mainnet (override via .env or consuming code if needed)
fcl.config().put("app.detail.title", "NFL All Day Wallet Viewer");

// Expose to window for legacy scripts
if (typeof window !== "undefined") {
  window.fcl = fcl;
}

export default fcl;

