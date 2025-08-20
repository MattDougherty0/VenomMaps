// Run: node scripts/prune_species_us.js
// Filters web/data/species_common.json to US-native venomous species
import fs from 'node:fs/promises';
import path from 'node:path';

const ROOT = process.cwd();
const IN = path.join(ROOT, 'web', 'data', 'species_common.json');
const OUT = IN;

// Curated allowlist of US-native species (scientific underscore names)
const ALLOWED = new Set([
  // Agkistrodon
  'Agkistrodon_contortrix',
  'Agkistrodon_piscivorus',
  // Sistrurus
  'Sistrurus_catenatus',
  'Sistrurus_tergeminus',
  'Sistrurus_miliarius',
  // Crotalus (US species)
  'Crotalus_adamanteus',
  'Crotalus_atrox',
  'Crotalus_horridus',
  'Crotalus_viridis',
  'Crotalus_oreganus',
  'Crotalus_scutulatus',
  'Crotalus_cerastes',
  'Crotalus_ruber',
  'Crotalus_molossus',
  'Crotalus_lepidus',
  'Crotalus_pricei',
  'Crotalus_willardi',
  'Crotalus_cerberus',
  'Crotalus_concolor',
  'Crotalus_mitchellii',
  'Crotalus_tigris',
  'Crotalus_stephensi',
  'Crotalus_lutosus',
  'Crotalus_helleri',
  'Crotalus_pyrrhus'
]);

(async () => {
  const raw = await fs.readFile(IN, 'utf8');
  const arr = JSON.parse(raw);
  const filtered = arr.filter(e => ALLOWED.has(e.sci));
  await fs.writeFile(OUT, JSON.stringify(filtered, null, 2), 'utf8');
  console.log(`Kept ${filtered.length} US species -> ${path.relative(ROOT, OUT)}`);
})();


