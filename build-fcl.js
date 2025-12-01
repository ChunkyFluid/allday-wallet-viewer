// Simple esbuild script to bundle FCL
import esbuild from 'esbuild';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { mkdirSync, writeFileSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Ensure output directory exists
const outDir = join(__dirname, 'public', 'js');
mkdirSync(outDir, { recursive: true });

const entryPoint = join(__dirname, 'src', 'fcl-bundle.js');
const outfile = join(outDir, 'fcl-bundle.js');

console.log('Building FCL bundle...');
console.log('Entry:', entryPoint);
console.log('Output:', outfile);

esbuild.build({
  entryPoints: [entryPoint],
  bundle: true,
  outfile: outfile,
  format: 'iife',
  globalName: 'fcl',
  platform: 'browser',
  target: 'es2020',
  minify: false,
  sourcemap: false,
  logLevel: 'info'
}).then(() => {
  console.log('✅ FCL bundle created successfully!');
  process.exit(0);
}).catch((error) => {
  console.error('❌ Build failed:');
  console.error(error);
  process.exit(1);
});

