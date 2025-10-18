#!/usr/bin/env python3
"""
add_region_to_forest_csv.py

Reads the provided forest_area_from_1990.csv, adds a 'Region' column
(using simple continental categories: Africa, Asia, Europe, North America,
South America, Oceania, Antarctica), and writes out a new CSV.

This script attempts to use the 'Code' (ISO alpha-3) column in the forest CSV
to match countries to continents using an authoritative mapping CSV downloaded at runtime.

Usage:
    python add_region_to_forest_csv.py \
        --input /path/to/forest_area_from_1990.csv \
        --output /path/to/forest_area_with_region.csv

If no paths are provided, it will default to:
    input:  FIT3179/data/forest_area_from_1990.csv
    output: FIT3179/data/forest_area_with_region.csv
"""

import argparse
import io
import sys
import csv
import urllib.request
import pandas as pd

# -----------------------------------------------------------------------------
# Configuration: mapping CSV URL (authoritative country -> continent list).
# The mapping file contains a column for Three_Letter_Country_Code (ISO3) and
# Continent_Name (e.g., "Asia", "Europe", "Africa", "North America", "South America", "Oceania", "Antarctica").
#
# Source used here (raw gist derived from a standard country/continent list):
# https://gist.githubusercontent.com/stevewithington/20a69c0b6d2ff846ea5d35e5fc47f26c/raw/country-and-continent-codes-list-csv.csv
# (This gist is a common CSV for country <-> continent mapping derived from datahub/UN/ISO lists.)
# -----------------------------------------------------------------------------
MAPPING_CSV_URL = (
    "https://gist.githubusercontent.com/stevewithington/"
    "20a69c0b6d2ff846ea5d35e5fc47f26c/raw/country-and-continent-codes-list-csv.csv"
)

# If mapping download fails, we'll try a fallback URL (another common dataset).
FALLBACK_MAPPING_CSV_URL = (
    "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
)

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def download_text(url, timeout=20):
    """Download text content from URL and return decoded string. Raises on error."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read()
            # Try decode with utf-8 (most likely)
            return raw.decode("utf-8", errors="replace")
    except Exception as e:
        raise RuntimeError(f"Failed to download {url}: {e}")

def build_iso3_to_continent_map(mapping_csv_text):
    """
    Parse the mapping CSV text and return a dict: ISO3 -> Continent (simple names).
    The mapping CSV we're using has headers like:
      Continent_Name Continent_Code Country_Name Two_Letter_Country_Code Three_Letter_Country_Code Country_Number
    Some files may be comma-separated or space-separated; we'll robustly parse with csv.reader.
    """
    # Normalize newlines and attempt to parse
    lines = mapping_csv_text.strip().splitlines()
    # Some gist files use spaces as separators in the view; however the raw CSV is comma-separated.
    # We'll try csv.Sniffer to detect delimiter.
    sample = "\n".join(lines[:10])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t| ")
        reader = csv.reader(lines, dialect)
    except Exception:
        # fallback: split on comma
        reader = csv.reader(lines)

    iso3_index = None
    continent_index = None
    header = next(reader)
    # find likely column names (case-insensitive)
    for i, h in enumerate(header):
        hh = h.strip().lower()
        if "three" in hh and ("alpha" in hh or "letter" in hh or "three_letter" in hh or "three-letter" in hh or "iso3" in hh or "three_letter_country_code" in hh):
            iso3_index = i
        if "continent" in hh and ("name" in hh or "continent_name" in hh):
            continent_index = i
    # If not found, try some fallback heuristics
    if iso3_index is None:
        # try common column labels
        for i, h in enumerate(header):
            if h.strip().upper() in ("THREE_LETTER_COUNTRY_CODE", "THREE_LETTER_CODE", "THREE_LETTER", "ISO_A3", "CODE3", "A3", "THREE_LETTER"):
                iso3_index = i
    if continent_index is None:
        for i, h in enumerate(header):
            if h.strip().upper() in ("CONTINENT_NAME", "CONTINENT", "CONTINENT_CODE"):
                continent_index = i

    # If still None, attempt to detect by position (common formats: continent,name,alpha2,alpha3)
    if iso3_index is None or continent_index is None:
        # try to detect a header that contains an ISO3-like entry in the second line
        # We'll look at first data row and check fields for 3-letter codes and continent names
        # rewind
        reader = csv.reader(lines)
        header = next(reader)
        first_data = next(reader, None)
        if first_data:
            # find candidate iso3 columns that look like 3 uppercase letters
            for i, v in enumerate(first_data):
                if iso3_index is None and isinstance(v, str) and len(v.strip()) == 3 and v.strip().isalpha():
                    iso3_index = i
                if continent_index is None and isinstance(v, str) and v.strip().lower() in (
                    "asia","europe","africa","north america","south america","oceania","antarctica",
                    "asia ","europe ","africa ","north america ","south america ","oceania ","antarctica "
                ):
                    continent_index = i

    if iso3_index is None or continent_index is None:
        raise RuntimeError("Could not determine ISO3 or Continent columns from mapping CSV header: " + ", ".join(header))

    # Build map
    iso3_to_cont = {}
    # Recreate reader to iterate from start (skip header again)
    reader = csv.reader(lines)
    next(reader)  # skip header
    for row in reader:
        if len(row) <= max(iso3_index, continent_index):
            continue
        iso3 = row[iso3_index].strip()
        cont = row[continent_index].strip()
        if not iso3:
            continue
        # Normalize continent names to our simple categories:
        cont_norm = cont
        # handle variants
        cont_norm = cont_norm.replace("Americas", "North America")  # safe default
        # Standard names in source are already like: Asia, Europe, Africa, North America, South America, Oceania, Antarctica
        # Ensure consistent capitalization and trim
        cont_norm = cont_norm.strip()
        # Map short codes (if Continent column contains codes like 'AS', 'EU', etc.)
        if len(cont_norm) == 2 and cont_norm.upper() in ("AS","EU","AF","NA","SA","OC","AN"):
            code_map = {
                "AS": "Asia",
                "EU": "Europe",
                "AF": "Africa",
                "NA": "North America",
                "SA": "South America",
                "OC": "Oceania",
                "AN": "Antarctica",
            }
            cont_norm = code_map.get(cont_norm.upper(), cont_norm)
        # Some rows may list duplicated mapping lines (e.g. a territory twice) — it's fine
        iso3_to_cont[iso3.upper()] = cont_norm
    return iso3_to_cont

def main(input_path, output_path, mapping_url=MAPPING_CSV_URL):
    # Read input CSV
    print(f"Reading input CSV: {input_path}")
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Failed to read input CSV '{input_path}': {e}", file=sys.stderr)
        sys.exit(1)

    # We expect an ISO alpha-3 code column named 'Code' in the forest CSV.
    # If that doesn't exist, attempt to find a sensible column (e.g., "Code", "ISO3", "ISO_A3").
    code_col = None
    for candidate in ("Code", "ISO3", "ISO_A3", "ISO", "code"):
        if candidate in df.columns:
            code_col = candidate
            break
    if code_col is None:
        # try to find a column of 3-letter uppercase strings
        for c in df.columns:
            sample_vals = df[c].dropna().astype(str).head(10).tolist()
            if all(len(s.strip()) == 3 and s.strip().isalpha() for s in sample_vals if s.strip()):
                code_col = c
                break

    if code_col is None:
        print("Could not detect an ISO alpha-3 code column in the input CSV. Please ensure there is a 3-letter 'Code' column (ISO3).", file=sys.stderr)
        sys.exit(1)

    print(f"Using ISO3 code column: '{code_col}'")

    # Download mapping CSV
    print("Downloading country->continent mapping CSV...")
    mapping_text = None
    try:
        mapping_text = download_text(mapping_url)
    except Exception as e:
        print(f"Primary mapping download failed: {e}", file=sys.stderr)
        print("Attempting fallback mapping URL...")
        try:
            mapping_text = download_text(FALLBACK_MAPPING_CSV_URL)
        except Exception as e2:
            print(f"Fallback mapping download also failed: {e2}", file=sys.stderr)
            sys.exit(1)

    # Build ISO3 -> Continent map
    try:
        iso3_to_cont = build_iso3_to_continent_map(mapping_text)
    except Exception as e:
        print(f"Failed to parse mapping CSV: {e}", file=sys.stderr)
        sys.exit(1)

    # Map codes to simple continent names
    def lookup_continent(code, country_name=None):
        if pd.isna(code):
            return ""
        c = str(code).strip().upper()
        if not c:
            return ""
        continent = iso3_to_cont.get(c)
        if continent:
            # Normalize to our expected simple categories
            cont_map = {
                "Asia": "Asia",
                "Europe": "Europe",
                "Africa": "Africa",
                "North America": "North America",
                "South America": "South America",
                "Oceania": "Oceania",
                "Antarctica": "Antarctica",
                # Some mapping sources may use "Americas" or similar — treat as North America by default
                "Americas": "North America",
            }
            # If mapping value looks like 'AS' etc, already normalized in build function
            return cont_map.get(continent, continent)
        # If no mapping found, return empty string (or optionally try to guess from country_name)
        return ""

    # Apply lookup to the dataframe
    df["Region"] = df[code_col].apply(lambda x: lookup_continent(x, None))

    # Check for rows where Region is empty and print some examples
    missing_mask = df["Region"].astype(str).str.strip() == ""
    if missing_mask.any():
        missing_codes = df.loc[missing_mask, code_col].astype(str).unique()
        print(f"Warning: {len(missing_codes)} unique ISO3 code(s) not found in mapping. Examples: {list(missing_codes)[:10]}")
        # Optionally, try to fill by matching country/entity name if the input has an 'Entity' or 'Country' column
        if "Entity" in df.columns:
            print("Attempting to fill missing regions by Entity name (best-effort)...")
            # Build a reverse mapping from normalized country name to continent using the mapping CSV rows
            # For robustness, build a name->continent map from the mapping CSV text
            name_to_cont = {}
            # naive parse: split mapping CSV lines and try to use the Country_Name column (position may differ)
            lines = mapping_text.strip().splitlines()
            reader = csv.reader(lines)
            header = next(reader)
            # detect country_name column index
            cname_index = None
            for i,h in enumerate(header):
                if "country" in h.lower() and "name" in h.lower():
                    cname_index = i
                    break
            # find continent index again
            cont_index = None
            for i,h in enumerate(header):
                if "continent" in h.lower():
                    cont_index = i
                    break
            if cname_index is not None and cont_index is not None:
                for row in reader:
                    if len(row) > max(cname_index, cont_index):
                        nm = row[cname_index].strip().lower()
                        cont = row[cont_index].strip()
                        if nm:
                            # sometimes comma within name; keep full string
                            name_to_cont[nm] = cont
            # try to match by lowercased Entity
            def try_by_name(entity):
                if pd.isna(entity): return ""
                key = str(entity).strip().lower()
                # exact match
                if key in name_to_cont:
                    return name_to_cont[key]
                # try some relaxed matching: startswith, contains
                for k in name_to_cont:
                    if key == k: return name_to_cont[k]
                    if key.startswith(k) or k.startswith(key): return name_to_cont[k]
                    if k in key or key in k: return name_to_cont[k]
                return ""
            # fill
            df.loc[missing_mask, "Region"] = df.loc[missing_mask, "Entity"].apply(try_by_name)
            still_missing = df["Region"].astype(str).str.strip() == ""
            if still_missing.any():
                print(f"After best-effort name matching, {still_missing.sum()} rows still have no Region.")
            else:
                print("All missing regions filled by Entity name matching.")
        else:
            print("No 'Entity' column available for fallback matching by name.")

    # Final write
    print(f"Writing output CSV to: {output_path}")
    try:
        df.to_csv(output_path, index=False)
    except Exception as e:
        print(f"Failed to write output CSV: {e}", file=sys.stderr)
        sys.exit(1)

    print("Done. The output CSV now contains a 'Region' column with simple continental categories.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add Region (continent) column to forest CSV.")
    parser.add_argument("--input", "-i", default="FIT3179/data/ocean-plastic-waste-per-capita-vs-gdp.csv", help="Input CSV path")
    parser.add_argument("--output", "-o", default="FIT3179/data/regional-ocean-plastic-waste-per-capita-vs-gdp.csv", help="Output CSV path")
    parser.add_argument("--mapping-url", "-m", default=MAPPING_CSV_URL, help="URL to country->continent mapping CSV")
    args = parser.parse_args()
    main(args.input, args.output, mapping_url=args.mapping_url)
