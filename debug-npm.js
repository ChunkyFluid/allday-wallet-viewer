import { execSync } from 'child_process';
console.log('--- Env ---');
console.log(JSON.stringify(process.env, null, 2));
console.log('--- NPM Config (try) ---');
try {
    const out = execSync('npm config list', { stdio: 'pipe' });
    console.log(out.toString());
} catch (e) {
    console.log('npm config failed:', e.message);
    if (e.stdout) console.log('stdout:', e.stdout.toString());
    if (e.stderr) console.log('stderr:', e.stderr.toString());
}
