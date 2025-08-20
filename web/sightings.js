import { sciPretty, getSelection, onSelectionChange, labelOf } from './main.js';

let mapRef;
let layerGroup = null;   // Leaflet LayerGroup for all sightings
const speciesData = new Map(); // sci -> array of sightings
let visible = false;     // toggle (default off)
let recencyDays = null;  // null => All by default

export function initSightings(map) {
  mapRef = map;
  layerGroup = L.layerGroup(); // attach only when visible
  // Refresh on selection and map moves
  onSelectionChange(handleSelectionChange);
  mapRef.on('moveend zoomend', () => render());
}

export function setSightingsVisible(v) {
  visible = v;
  if (!visible) {
    layerGroup.clearLayers();
    if (mapRef && mapRef.hasLayer(layerGroup)) mapRef.removeLayer(layerGroup);
    return;
  }
  if (mapRef && !mapRef.hasLayer(layerGroup)) layerGroup.addTo(mapRef);
  render();
}

export function setRecencyDays(days) {
  recencyDays = days; // number or null for All
  render();
}

async function ensureSpeciesLoaded(sci) {
  if (speciesData.has(sci)) return;
  try {
    const url = `./data/sightings/${sci}.json`;
    const res = await fetch(url);
    if (!res.ok) { speciesData.set(sci, []); return; }
    const arr = await res.json();
    speciesData.set(sci, Array.isArray(arr) ? arr : []);
  } catch { speciesData.set(sci, []); }
}

async function handleSelectionChange(selection) {
  // Load newly selected species lazily, then render
  await Promise.all(selection.map(ensureSpeciesLoaded));
  await render();
}

function inBounds(p, b) {
  return p.lon >= b.getWest() && p.lon <= b.getEast() && p.lat >= b.getSouth() && p.lat <= b.getNorth();
}

function ageDays(iso) {
  if (!iso) return null; // unknown recency
  const ms = Date.now() - new Date(iso).getTime();
  return ms / (1000 * 60 * 60 * 24);
}

function recencyColor(days) {
  if (days == null) return '#9ca3af';
  if (days <= 1)  return '#ef4444'; // 0-24h
  if (days <= 7)  return '#f97316'; // 2-7d
  if (days <= 30) return '#f59e0b'; // 8-30d
  if (days <= 90) return '#eab308'; // 31-90d
  return '#9ca3af';                 // older
}

function toTitleCase(str){
  return String(str || '').toLowerCase().replace(/(^|\s)\S/g, t => t.toUpperCase());
}

function popupHtml(s) {
  const when = s.ts ? new Date(s.ts).toLocaleString() : 'Unknown';
  const ver = s.verified ? 'Verified' : 'Unverified';
  const src = toTitleCase(String(s.source || 'unknown').replace(/_/g,' '));
  return `
    <div style="min-width:220px">
      <div style="font-weight:700">${labelOf(s.sci).split(' (')[0]}</div>
      <div style="font-style:italic">${sciPretty(s.sci)}</div>
      <div style="margin:6px 0">Seen <b>${when}</b></div>
      <div>Source: ${src} · ${ver}</div>
      ${s.photo ? '<div style="margin-top:6px;">📷 Photo provided</div>' : ''}
      <div style="margin-top:8px;"><a href="#" onclick="return false;">What to do if you see one</a></div>
    </div>
  `;
}

async function render() {
  if (!visible || !layerGroup || !mapRef) return;
  layerGroup.clearLayers();

  const sel = new Set(getSelection());     // selected species
  const hasFilter = sel.size > 0;
  const b = mapRef.getBounds();

  // Pull sightings
  const pool = [];
  if (hasFilter) {
    for (const sci of sel) {
      const arr = speciesData.get(sci) || [];
      for (const s of arr) pool.push(s);
    }
  } else {
    // No selection: show all sightings across species. Ensure core species are loaded lazily.
    // Load at least top N species from index if nothing loaded yet.
    if (speciesData.size === 0) {
      try {
        const idxRes = await fetch('./data/sightings_index.json');
        if (idxRes.ok) {
          const idx = await idxRes.json();
          const toLoad = idx.slice(0, 10).map(x => x.sci);
          await Promise.all(toLoad.map(ensureSpeciesLoaded));
        }
      } catch {}
    }
    for (const arr of speciesData.values()) for (const s of arr) pool.push(s);
  }
  // filter by bounds and recency; throttle very low zoom (global view) to reduce marker count
  const zoom = mapRef.getZoom();
  const sampleEvery = zoom <= 3 ? 10 : zoom === 4 ? 5 : 1; // show fewer at low zooms
  let idx = 0;
  const filtered = pool.filter(s => {
    const days = ageDays(s.ts);
    if (recencyDays != null && days != null && days > recencyDays) return false;
    if (!inBounds(s, b)) return false;
    idx++;
    return (idx % sampleEvery) === 0;
  });

  for (const s of filtered) {
    const days = ageDays(s.ts);
    const color = s.highlight ? '#b91c1c' : recencyColor(days); // darker if highlighted
    const marker = L.circleMarker([s.lat, s.lon], {
      radius: s.highlight ? 7 : 6,
      color,
      weight: s.highlight ? 3 : 2,
      fillColor: color,
      fillOpacity: s.highlight ? 0.6 : 0.35
    }).bindPopup(popupHtml(s), { maxWidth: 280 });

    marker.addTo(layerGroup);
  }

  // Optional: show a small header somewhere with counts
  const el = document.getElementById('sightings-count');
  if (el) el.textContent = `${filtered.length} sighting${filtered.length === 1 ? '' : 's'} in view`;
}


