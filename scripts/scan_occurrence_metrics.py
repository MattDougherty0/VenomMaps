import os, re, json, math, gzip
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OCC_DIR = ROOT / "data" / "occurrence"
OUT_DIR = ROOT / "web" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

US_COUNTRY_TOKENS = {"US","USA","United States","United States of America"}
GBIF_BAD_ISSUES = {
    "ZERO_COORDINATE","COORDINATE_OUT_OF_RANGE","COUNTRY_COORDINATE_MISMATCH",
    "COUNTRY_MISMATCH","RECORDED_DATE_INVALID","RECORDED_DATE_MISMATCH"
}
CAPTIVE_HINTS = re.compile(r"\b(zoo|captive|captiv(e|ity)|pet\s?store|collection|terrarium|museum\s?display)\b", re.I)
DATE_PRIORITIES = [
    "eventDate","verbatimEventDate","observed_on","time_observed_at",
    "observation_date","ObservationDate","dateObserved","date_observed",
    "date_collected","collectionDate","verbatim_date","date","Date"
]
DATE_REGEX = re.compile(r"(event|observ(ed|ation)?|collect(ed|ion)?|time).*date|^date$", re.I)
YEAR_TEXT_REGEX = re.compile(r"(?:^|\D)((?:19|20)\d{2})(?:\D|$)")

def is_us_row(row):
    cc = str(row.get("countryCode") or "").strip()
    cn = str(row.get("country") or "").strip()
    if cc in US_COUNTRY_TOKENS or cn in US_COUNTRY_TOKENS:
        return True
    try:
        lat = float(row.get("decimalLatitude")); lon = float(row.get("decimalLongitude"))
        return (18.0 <= lat <= 72.0) and (-179.5 <= lon <= -66.0)
    except Exception:
        return False

def has_any_date(row):
    return (str(row.get("eventDate") or "").strip() != "") or pd.notna(row.get("year"))

def has_full_date(row):
    ev = str(row.get("eventDate") or "").strip()
    if ev:
        try:
            pd.to_datetime(ev, errors="raise", utc=True); return True
        except Exception:
            if "/" in ev:
                try:
                    pd.to_datetime(ev.split("/",1)[0], errors="raise", utc=True); return True
                except Exception:
                    pass
    y = row.get("year"); m = row.get("month"); d = row.get("day")
    try:
        if pd.notna(y) and pd.notna(m) and pd.notna(d):
            pd.Timestamp(int(y), int(m), int(d)); return True
    except Exception:
        pass
    return False

def is_post_2010(row):
    ev = str(row.get("eventDate") or "").strip()
    if ev:
        dt = pd.to_datetime(ev, errors="coerce", utc=True)
        if pd.notna(dt):
            return dt >= pd.Timestamp("2010-01-01", tz="UTC")
        if "/" in ev:
            start = pd.to_datetime(ev.split("/",1)[0], errors="coerce", utc=True)
            return pd.notna(start) and start >= pd.Timestamp("2010-01-01", tz="UTC")
    try:
        return int(row.get("year")) >= 2010
    except Exception:
        return False

def basis_bucket(val):
    v = str(val or "").strip()
    if v in {"HumanObservation","Observation","MachineObservation","PreservedSpecimen","FossilSpecimen"}:
        return v
    su = v.upper().replace(" ", "_").replace("-", "_")
    if "HUMAN" in su:
        return "HumanObservation"
    if "MACHINE" in su:
        return "MachineObservation"
    if "FOSSIL" in su:
        return "FossilSpecimen"
    if "PRESERVED" in su or "SPECIMEN" in su:
        return "PreservedSpecimen"
    if "OBSERVATION" in su:
        return "Observation"
    return "Other"

def valid_coords(row):
    try:
        lat = float(row.get("decimalLatitude")); lon = float(row.get("decimalLongitude"))
        return -90 <= lat <= 90 and -180 <= lon <= 180
    except Exception:
        return False

def uncertainty_le_2km(row):
    try:
        u = float(row.get("coordinateUncertaintyInMeters"))
        return not math.isnan(u) and u <= 2000
    except Exception:
        return False

def gbif_issue_flagged(row):
    iss = str(row.get("issues") or "").upper()
    return any(code in iss for code in GBIF_BAD_ISSUES)

def captive_flagged(row):
    if str(row.get("establishmentMeans") or "").lower() in {"captive","managed","captive/managed"}:
        return True
    text = " ".join([
        str(row.get("occurrenceRemarks") or ""),
        str(row.get("locality") or ""),
        str(row.get("habitat") or "")
    ])
    return bool(CAPTIVE_HINTS.search(text))

def has_media(row):
    return bool(str(row.get("mediaType") or "").strip() or str(row.get("associatedMedia") or "").strip())

def read_table(path: Path):
    name = path.name.lower()
    opener = gzip.open if name.endswith(".gz") else open
    if name.endswith(".ndjson") or name.endswith(".ndjson.gz"):
        with opener(path, "rt", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    yield pd.json_normalize(json.loads(line))
                except Exception:
                    continue
    else:
        sep = "\t" if name.endswith(".tsv") or name.endswith(".tsv.gz") else ","
        for chunk in pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False,
                                 na_values=["","NA","NaN"], chunksize=200000):
            yield chunk

def scan_tabular_files():
    species_rows = []
    overall = {
        "total_records":0,"usa_records":0,"dated_any":0,"dated_full":0,"post_2010":0,
        "basis_counts":{ "HumanObservation":0,"Observation":0,"MachineObservation":0,
                         "PreservedSpecimen":0,"FossilSpecimen":0,"Other":0},
        "captive_flagged":0,"valid_coords":0,"uncertainty_le_2km":0,"gbif_issue_flagged":0,"has_media":0
    }

    files = []
    if OCC_DIR.exists():
        for f in OCC_DIR.rglob("*"):
            if f.is_file() and re.search(r"\.(csv|tsv|txt|ndjson)(\.gz)?$", f.name, re.I):
                files.append(f)
    else:
        print("No occurrence directory found:", OCC_DIR); return []

    for f in sorted(files):
        species_id = re.sub(r"\.(csv|tsv|txt|ndjson)(\.gz)?$", "", f.name, flags=re.I)
        sp = {k:(v.copy() if isinstance(v,dict) else 0) for k,v in overall.items()}

        for chunk in read_table(f):
            cc = chunk.get("countryCode"); cn = chunk.get("country")
            lat = pd.to_numeric(chunk.get("decimalLatitude"), errors="coerce")
            lon = pd.to_numeric(chunk.get("decimalLongitude"), errors="coerce")
            ev  = chunk.get("eventDate")
            year = pd.to_numeric(chunk.get("year"), errors="coerce")
            month = pd.to_numeric(chunk.get("month"), errors="coerce")
            day = pd.to_numeric(chunk.get("day"), errors="coerce")
            basis = chunk.get("basisOfRecord")
            estm  = chunk.get("establishmentMeans")
            issues = chunk.get("issues")
            mediaType = chunk.get("mediaType")
            assocMed  = chunk.get("associatedMedia")
            remarks = chunk.get("occurrenceRemarks")
            locality = chunk.get("locality")
            habitat  = chunk.get("habitat")
            unc = pd.to_numeric(chunk.get("coordinateUncertaintyInMeters"), errors="coerce")

            n = len(chunk)
            overall["total_records"] += n
            sp["total_records"] += n

            us_mask = (
                (cc.isin(US_COUNTRY_TOKENS)) |
                (cn.isin(US_COUNTRY_TOKENS)) |
                (lat.between(18.0,72.0) & lon.between(-179.5,-66.0))
            ).fillna(False)

            dated_any = ev.fillna("").str.strip().ne("") | year.notna()
            full_from_ev = pd.to_datetime(ev, errors="coerce", utc=True).notna()
            full_from_parts = year.notna() & month.notna() & day.notna()
            dated_full = full_from_ev | full_from_parts

            ev_parsed = pd.to_datetime(ev, errors="coerce", utc=True)
            post2010 = (ev_parsed >= pd.Timestamp("2010-01-01", tz="UTC")) | (year >= 2010)

            b = basis.fillna("Other").map(basis_bucket)

            valid = lat.between(-90,90) & lon.between(-180,180)
            unc2k = unc.notna() & (unc <= 2000)
            gbad = issues.fillna("").str.upper().apply(lambda s: any(code in s for code in GBIF_BAD_ISSUES))
            captive = estm.fillna("").str.lower().isin({"captive","managed","captive/managed"})
            text_blob = (remarks.fillna("") + " " + locality.fillna("") + " " + habitat.fillna(""))
            captive = captive | text_blob.str.contains(CAPTIVE_HINTS)
            has_media_flag = mediaType.fillna("").ne("") | assocMed.fillna("").ne("")

            sp["usa_records"]     += int(us_mask.sum());     overall["usa_records"]     += int(us_mask.sum())
            sp["dated_any"]       += int((dated_any & us_mask).sum());       overall["dated_any"]       += int((dated_any & us_mask).sum())
            sp["dated_full"]      += int((dated_full & us_mask).sum());      overall["dated_full"]      += int((dated_full & us_mask).sum())
            sp["post_2010"]       += int((post2010 & us_mask).sum());        overall["post_2010"]       += int((post2010 & us_mask).sum())
            sp["valid_coords"]    += int((valid & us_mask).sum());           overall["valid_coords"]    += int((valid & us_mask).sum())
            sp["uncertainty_le_2km"] += int((unc2k & us_mask).sum());        overall["uncertainty_le_2km"] += int((unc2k & us_mask).sum())
            sp["gbif_issue_flagged"] += int((gbad & us_mask).sum());         overall["gbif_issue_flagged"] += int((gbad & us_mask).sum())
            sp["captive_flagged"] += int((captive & us_mask).sum());         overall["captive_flagged"] += int((captive & us_mask).sum())
            sp["has_media"]       += int((has_media_flag & us_mask).sum());  overall["has_media"]       += int((has_media_flag & us_mask).sum())

            for k in ["HumanObservation","Observation","MachineObservation","PreservedSpecimen","FossilSpecimen","Other"]:
                sp["basis_counts"][k] += int(((b==k) & us_mask).sum())
                overall["basis_counts"][k]   += int(((b==k) & us_mask).sum())

        denom = sp["usa_records"] or 1
        species_rows.append({
            "species_id": species_id,
            **{k:v for k,v in sp.items() if k!="basis_counts"},
            **{f"basis_{k}": v for k,v in sp["basis_counts"].items()},
            "pct_dated_any": round(sp["dated_any"]/denom, 4),
            "pct_dated_full": round(sp["dated_full"]/denom, 4),
            "pct_post_2010": round(sp["post_2010"]/denom, 4),
            "pct_uncertainty_le_2km": round(sp["uncertainty_le_2km"]/denom, 4),
            "pct_captive_flagged": round(sp["captive_flagged"]/denom, 4)
        })

    return species_rows, overall

def scan_excel_combined():
    # Look for combined_records_v*.xlsx and pick the one that actually has date-like columns
    excel_paths = list(OCC_DIR.glob("*.xlsx"))
    if not excel_paths:
        return [], None
    def score_excel(path: Path) -> tuple:
        try:
            hdr = pd.read_excel(path, dtype=str, nrows=1)
        except Exception:
            return (-1, 0)
        cols = list(map(str, hdr.columns))
        score = 0
        for k in DATE_PRIORITIES:
            if k in cols:
                score += 5
        # regex hits
        score += sum(1 for c in cols if DATE_REGEX.search(str(c)))
        # Darwin Core parts
        for k in ["year","month","day"]:
            if any(c.lower()==k for c in cols):
                score += 2
        # prefer more columns slightly
        return (score, len(cols))
    # Rank all xlsx by score; highest first
    ranked = sorted(((score_excel(p), p) for p in excel_paths), key=lambda x: x[0], reverse=True)
    # Fallback: if tie or poor scores, prefer the non-clean file
    p = ranked[0][1]
    if p.name.endswith("_clean.xlsx") and len(ranked) > 1 and ranked[1][0] >= ranked[0][0]:
        p = ranked[1][1]
    df = pd.read_excel(p, dtype=str)
    # Save columns seen for debugging / mapping
    with open(OUT_DIR / 'occurrence_columns_seen.json','w') as fh:
        json.dump(list(map(str, df.columns)), fh, indent=2)
    # Column mapping for this workbook
    col = {c.lower(): c for c in df.columns}
    def getcol(*names):
        for n in names:
            if n.lower() in col:
                return df[col[n.lower()]]
        return pd.Series([None]*len(df))

    species = getcol('final_species','taxonomy_updated_species','scientific_name','ScientificName','species','Species').fillna("")
    species_id = species.astype(str).str.strip().str.replace(r"\s+","_", regex=True)
    country = getcol('country','countryCode')
    lat = pd.to_numeric(getcol('decimalLatitude','latitude'), errors='coerce')
    lon = pd.to_numeric(getcol('decimalLongitude','longitude'), errors='coerce')
    eventDate = getcol('eventDate','date','EventDate','observed_on','ObservationDate','dateObserved','date_observed','date_collected','collectionDate','verbatim_date','time_observed_at')
    year = pd.to_numeric(getcol('year'), errors='coerce')
    month = pd.to_numeric(getcol('month'), errors='coerce')
    day = pd.to_numeric(getcol('day'), errors='coerce')
    basis = getcol('basisOfRecord','source','Source','datasetName','Dataset')
    estm = getcol('establishmentMeans')
    issues = getcol('issues','issue')
    mediaType = getcol('mediaType','associatedMedia')
    remarks = getcol('occurrenceRemarks','remarks','Remarks')
    locality = getcol('locality','Locality')
    habitat = getcol('habitat','Habitat')
    unc = pd.to_numeric(getcol('coordinateUncertaintyInMeters','accuracy_m'), errors='coerce')

    overall = {
        "total_records": len(df),
        "usa_records":0,"dated_any":0,"dated_full":0,"post_2010":0,
        "basis_counts":{ "HumanObservation":0,"Observation":0,"MachineObservation":0,
                         "PreservedSpecimen":0,"FossilSpecimen":0,"Other":0},
        "captive_flagged":0,"valid_coords":0,"uncertainty_le_2km":0,"gbif_issue_flagged":0,"has_media":0
    }

    us_mask = (
        country.isin(US_COUNTRY_TOKENS) |
        (lat.between(18.0,72.0) & lon.between(-179.5,-66.0))
    ).fillna(False)

    # Find candidate date columns in the sheet
    cols_lower = [str(c) for c in df.columns]
    cand = []
    for k in DATE_PRIORITIES:
        if k in df.columns: cand.append(k)
    for c in df.columns:
        if c not in cand and DATE_REGEX.search(str(c)):
            cand.append(c)

    # Build a parsed datetime series using first successful parse among candidates
    dt = pd.Series([pd.NaT]*len(df))
    if cand:
        base = pd.Timestamp('1899-12-30', tz='UTC')
        for c in cand:
            s = df[c]
            # Try textual parse
            parsed = pd.to_datetime(s, errors='coerce', utc=True)
            # Try range split
            needs_range = parsed.isna() & s.fillna('').str.contains('/')
            if needs_range.any():
                start = pd.to_datetime(s.fillna('').str.split('/',1).str[0], errors='coerce', utc=True)
                parsed = parsed.fillna(start)
            # Try Excel serial numbers
            num = pd.to_numeric(s, errors='coerce')
            serial_ok = num.between(50,60000)
            serial_dt = base + pd.to_timedelta(num.where(serial_ok), unit='D')
            parsed = parsed.fillna(serial_dt)
            dt = dt.fillna(parsed)
            if dt.notna().all():
                break
    # If still missing, try to sniff dates/years from free-text fields
    if dt.isna().any():
        text_cols = []
        for name in ['voucher','flag_detailed','issue','issues','locality','Remarks','remarks']:
            if name in df.columns:
                text_cols.append(name)
        if text_cols:
            txt = df[text_cols].astype(str).fillna("").agg(" ".join, axis=1)
            # Try full text parse first (captures many formats)
            parsed_txt = pd.to_datetime(txt, errors='coerce', utc=True)
            # Fallback: extract a 4-digit year and build a date Jan 1 of that year
            years = txt.str.extract(r"((?:19|20)\d{2})", expand=False)
            year_dt = pd.to_datetime(years, format='%Y', errors='coerce', utc=True)
            dt = dt.fillna(parsed_txt).fillna(year_dt)
    # Fallback to Y/M/D if dt still NA
    dated_any = dt.notna() | year.notna()
    full_from_parts = year.notna() & month.notna() & day.notna()
    dated_full = dt.notna() | full_from_parts
    post2010 = (pd.to_datetime(dt, utc=True) >= pd.Timestamp('2010-01-01', tz='UTC')) | (year >= 2010)
    b = basis.fillna('Other').map(basis_bucket)
    valid = lat.between(-90,90) & lon.between(-180,180)
    unc2k = unc.notna() & (unc <= 2000)
    gbad = issues.fillna("").str.upper().apply(lambda s: any(code in s for code in GBIF_BAD_ISSUES))
    captive = estm.fillna("").str.lower().isin({'captive','managed','captive/managed'})
    text_blob = (remarks.fillna("") + " " + locality.fillna("") + " " + habitat.fillna(""))
    captive = captive | text_blob.str.contains(CAPTIVE_HINTS)
    has_media_flag = mediaType.fillna("").ne("")

    # overall accumulations
    overall["usa_records"]         = int(us_mask.sum())
    overall["dated_any"]           = int((dated_any & us_mask).sum())
    overall["dated_full"]          = int((dated_full & us_mask).sum())
    overall["post_2010"]           = int((post2010 & us_mask).sum())
    overall["valid_coords"]        = int((valid & us_mask).sum())
    overall["uncertainty_le_2km"]  = int((unc2k & us_mask).sum())
    overall["gbif_issue_flagged"]  = int((gbad & us_mask).sum())
    overall["captive_flagged"]     = int((captive & us_mask).sum())
    overall["has_media"]           = int((has_media_flag & us_mask).sum())
    for k in ["HumanObservation","Observation","MachineObservation","PreservedSpecimen","FossilSpecimen","Other"]:
        overall["basis_counts"][k] = int(((b==k) & us_mask).sum())

    # per-species groupby
    rows = []
    denom = us_mask
    grouped = df.assign(species_id=species_id).groupby('species_id')
    for sid, idx in grouped.groups.items():
        i = df.index.isin([*idx])
        us = us_mask & i
        counts = {
            "species_id": sid,
            "total_records": int(i.sum()),
            "usa_records": int(us.sum()),
            "dated_any": int((dated_any & us).sum()),
            "dated_full": int((dated_full & us).sum()),
            "post_2010": int((post2010 & us).sum()),
            "valid_coords": int((valid & us).sum()),
            "uncertainty_le_2km": int((unc2k & us).sum()),
            "gbif_issue_flagged": int((gbad & us).sum()),
            "captive_flagged": int((captive & us).sum()),
            "has_media": int((has_media_flag & us).sum()),
        }
        for k in ["HumanObservation","Observation","MachineObservation","PreservedSpecimen","FossilSpecimen","Other"]:
            counts[f"basis_{k}"] = int(((b==k) & us).sum())
        d = counts["usa_records"] or 1
        counts.update({
            "pct_dated_any": round(counts["dated_any"]/d,4),
            "pct_dated_full": round(counts["dated_full"]/d,4),
            "pct_post_2010": round(counts["post_2010"]/d,4),
            "pct_uncertainty_le_2km": round(counts["uncertainty_le_2km"]/d,4),
            "pct_captive_flagged": round(counts["captive_flagged"]/d,4)
        })
        rows.append(counts)
    return rows, overall

def main():
    rows, overall = scan_tabular_files()
    if not rows:
        rows, overall = scan_excel_combined()
        if rows is None or overall is None:
            print("No occurrence files found to scan.")
            return
    df = pd.DataFrame(rows)
    if not df.empty and 'species_id' in df.columns:
        df = df.sort_values('species_id')
    df.to_csv(OUT_DIR / 'occurrence_metrics_by_species.csv', index=False)
    with open(OUT_DIR / 'occurrence_metrics_overall.json','w') as fh:
        json.dump(overall, fh, indent=2)
    print('Wrote:', OUT_DIR / 'occurrence_metrics_by_species.csv', OUT_DIR / 'occurrence_metrics_overall.json')

if __name__ == '__main__':
    main()


