// Run: node scripts/build_species_common.js
import fs from 'node:fs/promises';
import path from 'node:path';
import xlsx from 'xlsx';

const ROOT = process.cwd();
const DIST_DIR = path.join(ROOT, 'data', 'distributions');
const SUPP_XLSX = path.join(ROOT, 'supplemental_material', 'SupplementalTable1.xlsx');
const OUT = path.join(ROOT, 'web', 'data', 'species_common.json');

const VENOMOUS_GENERA = new Set(['Agkistrodon','Crotalus','Sistrurus','Micrurus','Micruroides']);

// US coarse boxes (lon/lat)
const USA_BOXES = [
	[-125, 24.5, -66.9, 49.5],  // Contiguous US
	[-170, 52, -129, 71.5],     // Alaska
	[-161, 18.5, -154.5, 22.75] // Hawaii
];

function sciFromFilename(fname) {
	return fname.replace(/\.geojson$/i, '');
}
function prettySci(sci){ return sci.replace(/_/g,' '); }

async function listVenomousSpeciesFromDistributions() {
	const files = await fs.readdir(DIST_DIR);
	return files
		.filter(f => f.toLowerCase().endsWith('.geojson'))
		.map(sciFromFilename)
		.filter(sci => VENOMOUS_GENERA.has(sci.split('_')[0]));
}

function loadCommonNameMap() {
	try {
		const wb = xlsx.readFile(SUPP_XLSX);
		const sheet = wb.Sheets[wb.SheetNames[0]];
		const rows = xlsx.utils.sheet_to_json(sheet, { defval: '' });
		if (!rows.length) return new Map();
		const first = rows[0];
		const sciKey = ['scientific_name','ScientificName','Species','species','Scientific name'].find(k => k in first) || null;
		const comKey = ['common_name','CommonName','Common name','EnglishName','english_name','common'].find(k => k in first) || null;
		if (!sciKey) return new Map();
		const map = new Map();
		for (const r of rows) {
			const sciRaw = String(r[sciKey] || '').trim().replace(/\s+/g,'_');
			if (!sciRaw) continue;
			const com = comKey ? String(r[comKey] || '').trim() : '';
			if (!map.has(sciRaw)) map.set(sciRaw, com);
		}
		return map;
	} catch (e) {
		return new Map();
	}
}

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

function bboxIntersectsUS(bbox){
	if (!bbox) return false;
	const [minX,minY,maxX,maxY] = bbox;
	for (const [uMinX,uMinY,uMaxX,uMaxY] of USA_BOXES) {
		const sepX = uMinX > maxX || uMaxX < minX;
		const sepY = uMinY > maxY || uMaxY < minY;
		if (!(sepX || sepY)) return true;
	}
	return false;
}

(async () => {
	const species = await listVenomousSpeciesFromDistributions();
	const nameMap = loadCommonNameMap();

	// Filter species to those with US intersection
	const kept = [];
	for (const sci of species) {
		try {
			const geoPath = path.join(DIST_DIR, `${sci}.geojson`);
			const raw = await fs.readFile(geoPath, 'utf8');
			const geo = JSON.parse(raw);
			const bbox = computeGeoJSONBbox(geo);
			if (!bboxIntersectsUS(bbox)) continue;
			kept.push(sci);
		} catch (e) {
			// skip unreadable
		}
	}

	const out = kept.map(sci => {
		const common = nameMap.get(sci) || '';
		return {
			sci,
			common: common || prettySci(sci),
			aliases: [],
			venom_risk: "high"
		};
	}).sort((a,b) => (a.common || a.sci).localeCompare(b.common || b.sci));

	await fs.mkdir(path.dirname(OUT), { recursive: true });
	await fs.writeFile(OUT, JSON.stringify(out, null, 2), 'utf8');
	console.log(`Wrote ${out.length} USA-venomous species (with US intersection) to ${path.relative(ROOT, OUT)}`);
})();
