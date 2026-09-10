/* Copies web/ into mobile/www so Capacitor has a webDir inside its own
   project. Capacitor will not follow a webDir that points outside the
   project root, and duplicating the files in git would guarantee drift, so
   the copy happens at build time and www/ is ignored. */
'use strict';

const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, '..', '..', 'web');
const DEST = path.join(__dirname, '..', 'www');

// The test harness and its dependencies have no business in an app bundle.
const SKIP = new Set(['test', 'node_modules', 'README.md']);

function copyDir(from, to) {
  fs.mkdirSync(to, { recursive: true });
  for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
    if (SKIP.has(entry.name)) continue;
    const s = path.join(from, entry.name);
    const d = path.join(to, entry.name);
    if (entry.isDirectory()) copyDir(s, d);
    else fs.copyFileSync(s, d);
  }
}

fs.rmSync(DEST, { recursive: true, force: true });
copyDir(SRC, DEST);

// Sanity: the shell must be there, or the app opens to a blank screen.
for (const required of ['index.html', 'css/app.css', 'js/app.js', 'dive_sites.json']) {
  if (!fs.existsSync(path.join(DEST, required))) {
    console.error(`copy-web: ${required} missing from the bundle`);
    process.exit(1);
  }
}
const n = fs.readdirSync(DEST).length;
console.log(`copy-web: web/ -> mobile/www (${n} top-level entries)`);
