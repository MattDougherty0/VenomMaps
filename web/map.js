import { labelOf, colorFor } from './main.js';

let map;
// per-species range layers & cached GeoJSON
const layers = new Map();      // sci -> Leaflet layer
const geoCache = new Map();    // sci -> GeoJSON
let overlapLayer = null;       // Leaflet layer for intersection
let allRangesGroup = null;     // Group for all ranges when nothing selected
let deferRanges = true;        // Defer drawing ranges while panning

export async function initMap() {
  // Prefer canvas for better performance with many vectors
  map = L.map('map', { zoomControl: true, minZoom: 3, maxZoom: 18, preferCanvas: true })
          .setView([39.5, -98.35], 4); // USA
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
              { maxZoom: 19, attribution: '&copy; OpenStreetMap' }).addTo(map);
  allRangesGroup = L.layerGroup().addTo(map);
  // Hide ranges during move for responsiveness when deferring is enabled
  map.on('movestart', () => { if (deferRanges) clearAllRanges(); });
  map.on('moveend', () => { if (deferRanges) renderAllRanges(); });
}

export function getViewBounds() {
  const b = map.getBounds();
  return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
}
export function onMapMove(handler) { map.on('moveend', handler); }
export function getMap() { return map; }

// Public: update rendered map to match selection array
export async function setSelection(selection) {
  // If nothing is selected, show faint ranges for all species in species list
  if (!selection || selection.length === 0) {
    await renderAllRanges();
  } else {
    clearAllRanges();
  }
  // Remove layers no longer selected
  for (const [sci, layer] of layers.entries()) {
    if (!selection.includes(sci)) {
      map.removeLayer(layer);
      layers.delete(sci);
    }
  }

  // Add/update layers for new selections
  for (const sci of selection) {
    if (!layers.has(sci)) {
      const geo = await loadGeoJSON(sci);
      if (!geo) continue;
      const layer = L.geoJSON(geo, {
        style: {
          color: colorFor(sci),
          weight: 2,
          fillColor: colorFor(sci),
          fillOpacity: 0.10
        },
        onEachFeature: (_, layer) => layer.bindTooltip(labelOf(sci), { sticky: true })
      }).addTo(map);
      layers.set(sci, layer);
    }
  }

  // Fit to all selected layers on first add
  if (selection.length && layers.size === selection.length) {
    const group = L.featureGroup(Array.from(layers.values()));
    const b = group.getBounds();
    if (b.isValid()) map.fitBounds(b, { padding: [20,20] });
  }

  // Rebuild overlap highlight
  await renderOverlap(selection);
}

function clearAllRanges(){ if (allRangesGroup) allRangesGroup.clearLayers(); }

async function renderAllRanges(){
  clearAllRanges();
  try {
    const res = await fetch('./data/species_common.json');
    if (!res.ok) return;
    const list = await res.json();
    // Limit initial render to avoid too-heavy layer count
    const maxToRender = 30;
    for (const e of list.slice(0, maxToRender)) {
      const geo = await loadGeoJSON(e.sci);
      if (!geo) continue;
      L.geoJSON(geo, {
        style: {
          color: '#64748b',
          weight: 0.8,
          fillColor: '#94a3b8',
          fillOpacity: 0.08
        }
      }).addTo(allRangesGroup);
    }
  } catch {}
}

export function setDeferRanges(v) {
  deferRanges = !!v;
  if (!deferRanges) {
    // Immediately redraw if we stop deferring
    renderAllRanges();
  }
}

async function loadGeoJSON(sci) {
  if (geoCache.has(sci)) return geoCache.get(sci);
  try {
    const res = await fetch(`../data/distributions/${sci}.geojson`);
    if (!res.ok) { console.warn('No distribution for', sci); return null; }
    const geo = await res.json();
    geoCache.set(sci, geo);
    return geo;
  } catch (e) {
    console.warn('Failed to load geojson for', sci, e);
    return null;
  }
}

function combineToMulti(geo) {
  // returns a single (Multi)Polygon feature combining all parts (no dissolve)
  try {
    if (geo.type === 'Feature') return geo;
    if (geo.type === 'FeatureCollection') {
      const combined = turf.combine(geo);
      return combined.features[0]; // MultiPolygon or MultiLineString
    }
    // Geometry
    return { type: 'Feature', properties: {}, geometry: geo };
  } catch {
    // fallback: just return the first feature
    if (geo.features && geo.features.length) return geo.features[0];
    return null;
  }
}

async function renderOverlap(selection) {
  if (overlapLayer) { map.removeLayer(overlapLayer); overlapLayer = null; }
  if (selection.length < 2) return; // need at least two to intersect

  // Prepare MultiPolygon for each selected species
  const multis = [];
  for (const sci of selection) {
    const geo = await loadGeoJSON(sci);
    if (!geo) continue;
    const multi = combineToMulti(geo);
    if (multi) multis.push(multi);
  }
  if (multis.length < 2) return;

  // Intersect all selected geometries (shared area among all)
  let inter = multis[0];
  for (let i = 1; i < multis.length; i++) {
    try {
      inter = turf.intersect(inter, multis[i]);
      if (!inter) break; // no shared overlap
    } catch (e) {
      console.warn('Intersection failed at step', i, e);
      inter = null; break;
    }
  }
  if (!inter) return;

  overlapLayer = L.geoJSON(inter, {
    style: {
      color: '#7c3aed',         // purple outline
      weight: 2,
      fillColor: '#a78bfa',     // purple fill
      fillOpacity: 0.45
    }
  }).addTo(map);
}


