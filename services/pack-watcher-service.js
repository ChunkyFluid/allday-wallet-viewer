/**
 * Standalone Pack Opening Watcher
 * Run this to monitor pack openings in real-time
 * Usage: node services/pack-watcher-service.js
 */

import { watchPackOpenings, stopWatching } from './pack-opening-watcher.js';

console.log('🚀 Starting Pack Opening Watcher Service...\n');

// Start watching
watchPackOpenings().catch(err => {
    console.error('Fatal error starting pack watcher:', err);
    process.exit(1);
});

// Handle graceful shutdown
process.on('SIGTERM', () => {
    console.log('\n📦 Received SIGTERM, shutting down gracefully...');
    stopWatching();
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('\n📦 Received SIGINT, shutting down gracefully...');
    stopWatching();
    process.exit(0);
});

console.log('✅ Pack watcher service running');
console.log('Press Ctrl+C to stop\n');
