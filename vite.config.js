import { defineConfig } from 'vite';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig({
  build: {
    lib: {
      entry: path.resolve(__dirname, 'src/fcl-bundle.js'),
      name: 'fcl',
      fileName: 'fcl-bundle',
      formats: ['iife']
    },
    rollupOptions: {
      output: {
        // Extend window object with fcl
        extend: true
      }
    },
    outDir: 'public/js',
    emptyOutDir: false
  }
});

