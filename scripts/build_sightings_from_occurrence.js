// Run: node scripts/build_sightings_from_occurrence.js
import fs from 'node:fs/promises';
import path from 'node:path';
import xlsx from 'xlsx';

const ROOT = process.cwd();
const OCC_XLSX = path.join(ROOT, 'data', 'occurrence', 'combined_records_v4_clean.xlsx');
const SPECIES_JSON = path.join(ROOT, 'web', 'data', 'species_common.json');
const OUT_DIR = path.join(ROOT, 'web', 'data', 'sightings');
const OUT_INDEX = path.join(ROOT, 'web', 'data', 'sightings_index.json');

// US coarse boxes (lon/lat)
const BOXES = [
	[-125, 24.5, -66.9, 49.5],  // Contiguous US
	[-170, 52, -129, 71.5],     // Alaska
	[-161, 18.5, -154.5, 22.75] // Hawaii
];

const DAYS_WINDOW = 365; // recent sightings only
const MAX_UNCERTAINTY_M = 50000;

function inUS(lon, lat) {
	return BOXES.some(([minX,minY,maxX,maxY]) => lon>=minX && lon<=maxX && lat>=minY && lat<=maxY);
}

function firstKey(o, names) {
	for (const k of names) if (k in o) return k;
	return null;
}

function toISODate(v){
	if (!v) return null;
	const d = new Date(v);
	return isNaN(d) ? null : d.toISOString();
}

function ageDays(iso){
	if (!iso) return Infinity;
	const ms = Date.now() - new Date(iso).getTime();
	return ms / (1000*60*60*24);
}

(async () => {
	// Load venomous species set
	const speciesArr = JSON.parse(await fs.readFile(SPECIES_JSON, 'utf8'));
	const venomSet = new Set(speciesArr.map(s => s.sci));

	const wb = xlsx.readFile(OCC_XLSX, { cellDates: true });
	const sheet = wb.Sheets[wb.SheetNames[0]];
	const rows = xlsx.utils.sheet_to_json(sheet, { defval: '' });

	if (!rows.length) {
		console.error('No rows in occurrence sheet'); process.exit(1);
	}

	// Detect columns based on actual headers
	const sample = rows[0];
	const kSci = firstKey(sample, ['final_species','taxonomy_updated_species','scientific_name','scientificName','species','Species']);
	const kLat = firstKey(sample, ['decimalLatitude','latitude','lat']);
	const kLon = firstKey(sample, ['decimalLongitude','longitude','lon']);
	// Try multiple date fields
	const kDate = firstKey(sample, ['eventDate','EventDate','date','observed_on','ObservationDate','verbatimEventDate']);
	const kYear = firstKey(sample, ['year','Year']);
	const kMonth = firstKey(sample, ['month','Month']);
	const kDay = firstKey(sample, ['day','Day']);
	const kCtry = firstKey(sample, ['country','countryCode','Country']);
	const kSrc = firstKey(sample, ['source','datasetName','Dataset','provider']);
	const kMedia = firstKey(sample, ['hasMedia','mediaType','voucher','photo']);

	let kept = 0, dropped_uncert=0, dropped_coords=0, dropped_country=0, dropped_old=0, deduped=0;
	const perSpecies = new Map(); // sci -> array
	const dedupSet = new Set();   // keys for dedup
	for (let i=0;i<rows.length;i++) {
		const r = rows[i];
		let sci = String(r[kSci] || '').trim();
		if (!sci) continue;
		sci = sci.replace(/\s+/g,'_');
		if (!venomSet.has(sci)) continue;

		const lat = Number(r[kLat]); const lon = Number(r[kLon]);
		if (!isFinite(lat) || !isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) { dropped_coords++; continue; }

		const ccRaw = String(r[kCtry] || '').trim().toUpperCase();
		const isUSCountry = ccRaw === 'US' || ccRaw === 'USA' || ccRaw === 'UNITED STATES' || ccRaw === 'UNITED STATES OF AMERICA';
		if (!isUSCountry && !inUS(lon, lat)) { dropped_country++; continue; }

		let iso = kDate ? toISODate(r[kDate]) : null;
		if (!iso && (kYear || kMonth || kDay)) {
			const y = kYear ? String(r[kYear]||'').trim() : '';
			const m = kMonth ? String(r[kMonth]||'').trim() : '';
			const d = kDay ? String(r[kDay]||'').trim() : '';
			const candidate = [y, m || '1', d || '1'].join('-');
			const candISO = toISODate(candidate);
			if (candISO) iso = candISO;
		}
		if (iso && ageDays(iso) > DAYS_WINDOW) { dropped_old++; continue; }

		// QC: coordinate uncertainty, establishment/basis if present
		const uncert = Number(r['coordinateUncertaintyInMeters'] || r['coordUncertaintyM'] || r['uncertainty'] || '');
		if (isFinite(uncert) && uncert > MAX_UNCERTAINTY_M) { dropped_uncert++; continue; }
		const est = String(r['establishmentMeans'] || '').toLowerCase();
		if (est.includes('captive') || est.includes('cultivated')) { continue; }
		const basis = String(r['basisOfRecord'] || '').toLowerCase();
		if (basis.includes('fossil') || basis.includes('preserved')) { continue; }

		const source = String(r[kSrc] || '').slice(0,120);
		const hasPhoto = (() => {
			const v = String(r[kMedia] || '').toLowerCase();
			return ['true','photo','image','photograph','voucher'].some(x => v.includes(x));
		})();

		const sLower = source.toLowerCase();
		const isPreserved = /preserved|specimen|museum/.test(sLower);
		if (isPreserved) { continue; }
		const isHumanObs = /human|observation|inat|gbif|herp/.test(sLower);
		const verified = /inat|gbif|herp|human/i.test(source);

		const rec = {
			id: `occ_${i}`,
			sci,
			ts: iso,
			lat: Number(lat.toFixed(5)),
			lon: Number(lon.toFixed(5)),
			source: source || 'unknown',
			photo: hasPhoto,
			verified,
			source_kind: isPreserved ? 'preserved_specimen' : (isHumanObs ? 'human_observation' : 'other'),
			highlight: !!(hasPhoto || (iso && !isPreserved))
		};

		// Dedup key: sci + rounded coords + day bucket (if date present)
		const dayBucket = iso ? Math.floor(new Date(iso).getTime() / 86400000) : 'nodate';
		const key = `${rec.sci}|${rec.lat}|${rec.lon}|${dayBucket}`;
		if (dedupSet.has(key)) { deduped++; continue; }
		dedupSet.add(key);

		if (!perSpecies.has(sci)) perSpecies.set(sci, []);
		perSpecies.get(sci).push(rec);

		kept++;
	}

	await fs.mkdir(OUT_DIR, { recursive: true });
	const index = [];
	for (const [sci, arr] of perSpecies.entries()) {
		const p = path.join(OUT_DIR, `${sci}.json`);
		await fs.writeFile(p, JSON.stringify(arr, null, 2), 'utf8');
		index.push({ sci, count: arr.length, bytes: (await fs.stat(p)).size, latest_ts: arr.reduce((m, s) => !m || (s.ts && s.ts > m) ? s.ts : m, null) });
	}
	await fs.writeFile(OUT_INDEX, JSON.stringify(index, null, 2), 'utf8');
	console.log(`Kept ${kept} USA venomous sightings across ${perSpecies.size} species -> ${path.relative(ROOT, OUT_DIR)}`);
	console.log(`Dropped: coords=${dropped_coords}, country=${dropped_country}, old=${dropped_old}, uncert>${MAX_UNCERTAINTY_M}m=${dropped_uncert}, dedup=${deduped}`);
})();
