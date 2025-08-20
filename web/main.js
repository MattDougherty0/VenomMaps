// ---- Common names + helpers -------------------------------------------------
import Fuse from 'https://cdn.jsdelivr.net/npm/fuse.js@6.6.2/dist/fuse.esm.js';

export let COMMON = [];                 // array of { sci, common, aliases[], venom_risk? }
export const COMMON_BY_SCI = Object.create(null);
export let SIGHTINGS_INDEX = []; // optional index loaded at boot

export async function loadCommon() {
  const res = await fetch('./data/species_common.json');
  if (!res.ok) throw new Error('Failed to load species_common.json');
  COMMON = await res.json();
  for (const e of COMMON) {
    COMMON_BY_SCI[e.sci] = e;
  }
  buildFuse();
  // try to load sightings index for counts
  try {
    const idxRes = await fetch('./data/sightings_index.json');
    if (idxRes.ok) SIGHTINGS_INDEX = await idxRes.json();
  } catch {}
}

export function sciPretty(sci) {
  return (sci || '').replace(/_/g, ' ').trim();
}

function cleanCommon(raw, sci){
  let s = String(raw || '').trim();
  if (!s) return sciPretty(sci);
  // Keep only text before any semicolon or pipe (drops language lists like "German:")
  s = s.split(';')[0].split('|')[0];
  // Remove bracketed/parenthetical metadata
  s = s.replace(/\[[^\]]*\]/g, '').replace(/\([^)]*\)/g, '');
  // If there are multiple comma-separated candidates, keep the first
  s = s.split(',')[0];
  // If a prefix label exists (e.g., "viridis: Western Rattlesnake"), strip it
  s = s.replace(/^[^:]{1,40}:\s*/, '');
  // Normalize whitespace and trailing commas
  s = s.replace(/\s{2,}/g,' ').replace(/,+$/,'').trim();
  // Title-case simple all-uppercase leftovers
  if (/^[A-Z\s-]+$/.test(s)) s = s.toLowerCase().replace(/(^|\s)\S/g, t => t.toUpperCase());
  return s || sciPretty(sci);
}

export function labelOf(sci) {
  const e = COMMON_BY_SCI[sci];
  if (!e) return sciPretty(sci); // fallback to sci only
  return `${cleanCommon(e.common, sci)} (${sciPretty(sci)})`;
}

// Deterministic color per species (HSL string)
export function colorFor(sci) {
  let hash = 0;
  for (let i = 0; i < sci.length; i++) hash = (hash * 31 + sci.charCodeAt(i)) >>> 0;
  const hue = hash % 360;
  return `hsl(${hue} 70% 50%)`;
}

// ---- Selection state --------------------------------------------------------
const selected = new Set();
const listeners = new Set();

export function getSelection() { return Array.from(selected); }
export function onSelectionChange(cb) { listeners.add(cb); cb(getSelection()); }
function emit() { const arr = getSelection(); listeners.forEach(fn => fn(arr)); }

export function toggleSpecies(sci) {
  if (selected.has(sci)) selected.delete(sci); else selected.add(sci);
  renderSelectedPanel();
  highlightSelectedInLists();
  emit();
}
export function clearSelection() {
  selected.clear();
  renderSelectedPanel();
  highlightSelectedInLists();
  emit();
}

export function selectAllSpecies() {
  // Select all species currently loaded in COMMON
  for (const e of COMMON) selected.add(e.sci);
  renderSelectedPanel();
  highlightSelectedInLists();
  emit();
}

// ---- Fuzzy search (#2) ------------------------------------------------------
let fuse;

function buildFuse() {
  fuse = new Fuse(COMMON, {
    keys: [
      { name: 'common', weight: 0.6 },
      { name: 'sci', weight: 0.3 },
      { name: 'aliases', weight: 0.1 }
    ],
    threshold: 0.35,            // forgiving for typos
    ignoreLocation: true,
    minMatchCharLength: 2,
    useExtendedSearch: false
  });
}

export function querySpecies(q) {
  q = (q || '').trim();
  if (!q) return [];
  // Fallback to simple substring if Fuse somehow isn't ready
  if (!fuse) {
    const lq = q.toLowerCase();
    return COMMON.filter(e =>
      (e.common || '').toLowerCase().includes(lq) ||
      (e.sci || '').toLowerCase().includes(lq) ||
      (e.aliases || []).some(a => String(a).toLowerCase().includes(lq))
    );
  }
  return fuse.search(q).map(r => r.item);
}

export function attachSearch() {
  const input = document.getElementById('search');
  const ul = document.getElementById('results');
  const hint = document.getElementById('hint');

  function render(items) {
    ul.innerHTML = '';
    if (!items.length) {
      hint.textContent = 'No matches. Try “rattlesnake” or “copperhead”.';
      return;
    }
    hint.textContent = '';
    for (const e of items.slice(0, 30)) {
      const li = document.createElement('li');
      li.dataset.sci = e.sci;
      const idx = SIGHTINGS_INDEX.find(x => x.sci === e.sci);
      const countBadge = idx ? `<span class=\"badge\">${idx.count.toLocaleString()} sightings</span>` : '';
      const label = labelOf(e.sci);
      li.innerHTML = `
        <span class=\"swatch\" style=\"background:${colorFor(e.sci)};\"></span>
        <span class=\"common\">${label.split(' (')[0]}</span>
        <span class=\"sci\">(${sciPretty(e.sci)})</span>
        ${countBadge}
      `;
      if (selected.has(e.sci)) li.classList.add('selected');
      li.onclick = () => toggleSpecies(e.sci);
      ul.appendChild(li);
    }
  }

  input.addEventListener('input', () => {
    const q = input.value;
    if (!q.trim()) { ul.innerHTML = ''; hint.textContent = 'Type to search e.g. “cottonmouth”, “timber rattlesnake”…'; return; }
    render(querySpecies(q));
  });
}

// ---- Nearby (in view) (#3) --------------------------------------------------
// We compute/remember a per-species bbox {sci -> [minX,minY,maxX,maxY]}
// If missing, we lazily fetch that species’ GeoJSON once and compute it.
// Results are cached in-memory and (lightly) in localStorage.

const BBOX_CACHE = Object.create(null);
const LS_KEY = 'vm_bboxes_v1';

(function loadBboxesFromStorage() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        for (const k of Object.keys(parsed)) BBOX_CACHE[k] = parsed[k];
      }
    }
  } catch {}
})();

function saveBboxesToStorage() {
  try { localStorage.setItem(LS_KEY, JSON.stringify(BBOX_CACHE)); } catch {}
}

// Compute bbox for a GeoJSON FeatureCollection or Feature
function computeGeoJSONBbox(geo) {
  let minX =  Infinity, minY =  Infinity, maxX = -Infinity, maxY = -Infinity;
  function visitCoords(coords) {
    // coords can be [x,y] or nested arrays
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
    } else {
      visitCoords(g.coordinates);
    }
  }
  if (geo.type === 'FeatureCollection') {
    for (const f of geo.features) visitFeature(f);
  } else if (geo.type === 'Feature') {
    visitFeature(geo);
  } else if (geo.type) {
    visitFeature({ geometry: geo });
  }
  if (minX === Infinity) return null;
  return [minX, minY, maxX, maxY];
}

async function getSpeciesBBox(sci) {
  if (BBOX_CACHE[sci]) return BBOX_CACHE[sci];
  // Prefer precomputed bboxes file if present
  try {
    if (!BBOX_CACHE.__loaded_precomputed) {
      const pre = await fetch('./data/distributions_bbox.json');
      if (pre.ok) {
        const m = await pre.json();
        for (const k in m) BBOX_CACHE[k] = m[k];
      }
      BBOX_CACHE.__loaded_precomputed = true;
    }
    if (BBOX_CACHE[sci]) return BBOX_CACHE[sci];
  } catch {}

  // Fallback to fetching GeoJSON
  try {
    const res = await fetch(`../data/distributions/${sci}.geojson`);
    if (!res.ok) return null;
    const geo = await res.json();
    const bbox = computeGeoJSONBbox(geo);
    if (bbox) {
      BBOX_CACHE[sci] = bbox;
      saveBboxesToStorage();
    }
    return bbox;
  } catch {
    return null;
  }
}

function intersects(b1, b2) {
  // b = [minX,minY,maxX,maxY]
  return !(b2[0] > b1[2] || b2[2] < b1[0] || b2[1] > b1[3] || b2[3] < b1[1]);
}

function overlapArea(b1, b2) {
  const minX = Math.max(b1[0], b2[0]);
  const minY = Math.max(b1[1], b2[1]);
  const maxX = Math.min(b1[2], b2[2]);
  const maxY = Math.min(b1[3], b2[3]);
  if (minX >= maxX || minY >= maxY) return 0;
  return (maxX - minX) * (maxY - minY);
}

function debounce(fn, ms = 250) {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

export function attachNearby({ onMove, getBounds }) {
  const ul = document.getElementById('nearby');
  const hint = document.getElementById('nearby-hint');
  const toggle = document.getElementById('nearby-toggle');

  function setVisible(v){
    ul.style.display = v ? '' : 'none';
    hint.style.display = v ? '' : 'none';
  }
  setVisible(false);
  toggle?.addEventListener('change', () => {
    const v = toggle.checked;
    setVisible(v);
    if (v) update();
  });

  async function update() {
    const view = getBounds(); // [minX,minY,maxX,maxY]
    // First pass: check whatever we already have cached
    const candidates = [];
    for (const e of COMMON) {
      const bb = BBOX_CACHE[e.sci];
      if (bb && intersects(bb, view)) {
        candidates.push({ e, score: overlapArea(bb, view) });
      }
    }
    // Second pass: lazily fetch bbox for a handful of not-yet-cached species
    // to enrich the list (limit to avoid hammering the browser)
    const MISSING_LIMIT = 6;
    let fetched = 0;
    for (const e of COMMON) {
      if (fetched >= MISSING_LIMIT) break;
      if (!BBOX_CACHE[e.sci]) {
        const bb = await getSpeciesBBox(e.sci);
        fetched++;
        if (bb && intersects(bb, view)) {
          candidates.push({ e, score: overlapArea(bb, view) });
        }
      }
    }
    // Sort by overlap (desc) → shows the most relevant first
    candidates.sort((a, b) => b.score - a.score);

    // Render
    ul.innerHTML = '';
    if (!candidates.length) {
      hint.textContent = 'No species in view yet. Pan/zoom the map.';
      return;
    }
    hint.textContent = '';
    for (const { e } of candidates.slice(0, 30)) {
      const li = document.createElement('li');
      const label = labelOf(e.sci);
      li.innerHTML = `
        <span class=\"common\">${label.split(' (')[0]}</span>
        <span class=\"sci\">(${sciPretty(e.sci)})</span>
        <span class=\"badge\">range overlap</span>
      `;
      if (selected.has(e.sci)) li.classList.add('selected');
      li.onclick = () => toggleSpecies(e.sci);
      ul.appendChild(li);
    }
  }

  const debounced = debounce(update, 250);
  // Update on map movements when visible
  onMove(debounced);
}


// ---- Selected panel ---------------------------------------------------------
function highlightSelectedInLists() {
  for (const ulId of ['results','nearby']) {
    const ul = document.getElementById(ulId);
    if (!ul) continue;
    for (const li of ul.querySelectorAll('li')) {
      const sci = li.dataset.sci;
      if (!sci) continue;
      li.classList.toggle('selected', selected.has(sci));
    }
  }
}

export function attachSelectedPanel() {
  const ul = document.getElementById('legend');
  const btn = document.getElementById('clear-selected');
  if (btn) btn.onclick = clearSelection;
  renderSelectedPanel();
}

function renderSelectedPanel() {
  const ul = document.getElementById('legend');
  if (!ul) return;
  ul.innerHTML = '';
  for (const sci of selected) {
    const e = COMMON_BY_SCI[sci] || { sci, common: sciPretty(sci) };
    const li = document.createElement('li');
    li.innerHTML = `
      <div>
        <span class=\"swatch\" style=\"background:${colorFor(sci)};\"></span>
        <span class=\"common\">${e.common || sciPretty(sci)}</span>
        <span class=\"sci\">(${sciPretty(sci)})</span>
      </div>
      <span class=\"rm\" title=\"Remove\">✕</span>
    `;
    li.querySelector('.rm').onclick = () => toggleSpecies(sci);
    ul.appendChild(li);
  }
}

