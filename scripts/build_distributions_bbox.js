// Run: node scripts/build_distributions_bbox.js
import fs from 'node:fs/promises';
import path from 'node:path';

const ROOT = process.cwd();
const DIST_DIR = path.join(ROOT, 'data', 'distributions');
const OUT = path.join(ROOT, 'web', 'data', 'distributions_bbox.json');

function sciFromFilename(fname) { return fname.replace(/\.geojson$/i, ''); }

function computeGeoJSONBbox(geo) {
  let minX =  Infinity, minY =  Infinity, maxX = -Infinity, maxY = -Infinity;
  function visitCoords(coords) {
    if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
      const x = coords[0], y = coords[1];
      if (x < minX) minX = x; if (y < minY) minY = y;
      if (x > maxX) maxX = x; if (y > maxY) maxY = y;
      return;
    }
    for (const c of coords) visitCoords(c);
  }
  function visitFeature(f) {
    const g = f && f.geometry;
    if (!g) return;
    if (g.type === 'GeometryCollection') {
      for (const gg of g.geometries) visitFeature({ geometry: gg });
    } else if (g.coordinates) {
      visitCoords(g.coordinates);
    }
  }
  if (geo.type === 'FeatureCollection') {
    for (const f of geo.features) visitFeature(f);
  } else if (geo.type === 'Feature') {
    visitFeature(geo);
  } else if (geo && geo.type) {
    visitFeature({ geometry: geo });
  }
  if (minX === Infinity) return null;
  return [minX, minY, maxX, maxY];
}

(async () => {
  const files = (await fs.readdir(DIST_DIR)).filter(f => f.toLowerCase().endsWith('.geojson'));
  const out = {};
  for (const f of files) {
    try {
      const sci = sciFromFilename(f);
      const raw = await fs.readFile(path.join(DIST_DIR, f), 'utf8');
      const bbox = computeGeoJSONBbox(JSON.parse(raw));
      if (bbox) out[sci] = bbox;
    } catch {}
  }
  await fs.mkdir(path.dirname(OUT), { recursive: true });
  await fs.writeFile(OUT, JSON.stringify(out, null, 2), 'utf8');
  console.log(`Wrote ${Object.keys(out).length} bboxes -> ${path.relative(ROOT, OUT)}`);
})();


