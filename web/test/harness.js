/* Shared jsdom harness. The app is split across web/js/*.js loaded with plain
   <script src> tags; jsdom does not fetch those, so they are inlined here in
   the same order the page declares them. */
const fs = require('fs');
const path = require('path');

const WEB = path.join(__dirname, '..');

const ORDER = ['util', 'api', 'catalogue', 'charts', 'ui', 'views', 'map', 'logdive', 'app'];

/** index.html with the CDN assets removed and the local scripts inlined. */
function buildHtml() {
  let html = fs.readFileSync(path.join(WEB, 'index.html'), 'utf8')
    .replace(/<script src="https:\/\/unpkg[^<]*<\/script>/g, '')
    .replace(/<link rel="stylesheet" href="https:\/\/[^>]*>/g, '')
    .replace(/<link rel="preconnect"[^>]*>/g, '')
    .replace(/<link rel="stylesheet" href="css\/app\.css">/g, '');

  for (const name of ORDER) {
    const src = fs.readFileSync(path.join(WEB, 'js', `${name}.js`), 'utf8');
    html = html.replace(
      `<script src="js/${name}.js"></script>`,
      `<script>\n${src}\n</script>`);
  }
  return html;
}

module.exports = { buildHtml, WEB, ORDER };
