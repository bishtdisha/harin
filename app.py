import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from difflib import SequenceMatcher
from pathlib import Path
from openpyxl.utils import get_column_letter
from rapidfuzz import process as rf_process, fuzz as rf_fuzz
from typing import Optional, Tuple

st.set_page_config(page_title="Excel Site Merger", layout="wide")

# --- Helpers ---

def clean_site_name(name):
    """Normalize site names for fuzzy matching."""
    if pd.isna(name): return ""
    name = str(name).lower()
    # Replace punctuation/separators with spaces so first-word detection works for values like "Mampur,Lakathepur"
    name = re.sub(r'[^\w]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def similarity(a, b):
    # Fallback simple similarity if ever needed elsewhere
    return SequenceMatcher(None, a, b).ratio()

def build_candidate_groups(df2: pd.DataFrame, site_col_2: str):
    """Pre-compute candidate lists grouped by first word for fast fuzzy match.
    Returns dict[first_word] -> { 'names': [cleaned_name,...], 'idxs': [row_index,...] }
    """
    groups = {}
    # Precompute a cleaned site column on df2 for optional fallback matching
    df2["__clean_site__"] = df2[site_col_2].fillna("").astype(str).map(clean_site_name)
    site_series = df2["__clean_site__"]
    for row_idx, raw_name in site_series.items():
        cleaned = raw_name
        if not cleaned:
            continue
        first_word = cleaned.split()[0]
        bucket = groups.setdefault(first_word, {"names": [], "idxs": []})
        bucket["names"].append(cleaned)
        bucket["idxs"].append(row_idx)
    return groups

def find_best_match(target_raw: str, groups: dict, df2: pd.DataFrame, mode: str = "first_word", score_cutoff: int = 0):
    """Find best match for a file-1 site name in file-2 names.
    - mode="first_word": require same first word in file 2
    - mode="contains": allow first word from file 1 to appear anywhere in file 2 name
    Returns (row_index_in_df2, score_percent, cleaned_match) or (None, 0, None)
    """
    target_clean = clean_site_name(target_raw)
    if not target_clean:
        return None, 0, None
    first_word = target_clean.split()[0]
    names: list[str] = []
    idxs: list[int] = []

    if mode == "first_word":
        bucket = groups.get(first_word)
        if bucket and bucket["names"]:
            names = bucket["names"]
            idxs = bucket["idxs"]
    elif mode == "contains":
        mask = df2["__clean_site__"].str.contains(fr"\b{re.escape(first_word)}\b", regex=True)
        if mask.any():
            names = df2.loc[mask, "__clean_site__"].tolist()
            idxs = df2.loc[mask].index.tolist()
    elif mode == "site2_in_site1":
        # Direct substring: file-2 site name must be contained within file-1 site name
        clean_series = df2["__clean_site__"].fillna("")
        candidate_mask = clean_series.apply(lambda s: bool(s) and s in target_clean)
        if candidate_mask.any():
            names = clean_series.loc[candidate_mask].tolist()
            idxs = df2.loc[candidate_mask].index.tolist()

    if not names:
        return None, 0, None

    scorer = rf_fuzz.partial_ratio if mode == "site2_in_site1" else rf_fuzz.ratio
    result = rf_process.extractOne(target_clean, names, scorer=scorer, score_cutoff=score_cutoff)
    if result is None:
        # If score cutoff filtered everything, fall back to first candidate
        return idxs[0], 0, names[0]
    _, score, local_idx = result
    row_idx = idxs[local_idx]
    return row_idx, score, names[local_idx]

def find_site_column(df: pd.DataFrame) -> Optional[str]:
    """Prefer a column that clearly denotes site name.
    Priority order:
      1) exact 'site name' or contains both 'site' and 'name'
      2) contains 'site'
      3) fallback: contains 'name'
    """
    cols = list(df.columns)
    scores = []
    for col in cols:
        cl = str(col).strip().lower()
        score = 0
        if 'site' in cl and 'name' in cl:
            score = 3
        elif cl.replace(' ', '') == 'sitename':
            score = 3
        elif 'site' in cl:
            score = 2
        elif 'name' in cl:
            score = 1
        scores.append((score, col))
    scores.sort(reverse=True)
    top_score, top_col = scores[0] if scores else (0, None)
    return top_col if top_score > 0 else None

def find_scheme_column(df: pd.DataFrame) -> Optional[str]:
    cols = [c for c in df.columns if 'scheme' in str(c).lower()]
    if not cols:
        return None
    # Prefer one that has 'id'
    with_id = [c for c in cols if 'id' in str(c).lower()]
    return with_id[0] if with_id else cols[0]

def find_rtu_column(df: pd.DataFrame) -> Optional[str]:
    cols = [c for c in df.columns if 'rtu' in str(c).lower()]
    if not cols:
        return None
    with_id = [c for c in cols if 'id' in str(c).lower()]
    return with_id[0] if with_id else cols[0]

def find_ip_column(df: pd.DataFrame) -> Optional[str]:
    # Prefer OVPN specifically
    candidates = [(c, str(c).lower()) for c in df.columns]
    ovpn = [c for c, cl in candidates if 'ovpn' in cl and 'ip' in cl]
    if ovpn:
        return ovpn[0]
    ovpn_any = [c for c, cl in candidates if 'ovpn' in cl]
    if ovpn_any:
        return ovpn_any[0]
    # Generic IP address
    ip_cols = [c for c, cl in candidates if 'ip' in cl and 'router' not in cl]
    addr_cols = [c for c, cl in candidates if 'address' in cl and 'router' not in cl]
    if ip_cols:
        return ip_cols[0]
    if addr_cols:
        return addr_cols[0]
    return None

def find_agency_column(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        if 'agency' in str(col).lower():
            return col
    return None

def merge_excels(df1, df2, match_mode: str = "first_word", score_cutoff: int = 0):
    """Merge by best fuzzy matching of site names (starting with same first word)."""
    site_col_1 = find_site_column(df1)
    scheme_col_1 = find_scheme_column(df1)
    site_col_2 = find_site_column(df2)
    rtu_col_2 = find_rtu_column(df2)
    ip_col_2 = find_ip_column(df2)
    agency_col_2 = find_agency_column(df2)

    if not all([site_col_1, scheme_col_1, site_col_2, rtu_col_2, ip_col_2]):
        missing = []
        if not site_col_1: missing.append("Site Name in file 1")
        if not scheme_col_1: missing.append("Scheme ID in file 1")
        if not site_col_2: missing.append("Site Name in file 2")
        if not rtu_col_2: missing.append("RTU ID in file 2")
        if not ip_col_2: missing.append("OVPN IP Address in file 2")
        return None, None, missing

    # Pre-compute candidate groups for df2
    candidate_groups = build_candidate_groups(df2, site_col_2)

    merged_data = []
    unmatched = []

    # Iterate efficiently over df1 without DataFrame indexing overhead
    for _, row in df1[[site_col_1, scheme_col_1]].iterrows():
        name_1 = row[site_col_1]
        scheme_id = row[scheme_col_1]
        row_idx_2, score, _ = find_best_match(str(name_1), candidate_groups, df2, mode=match_mode, score_cutoff=score_cutoff)
        if row_idx_2 is not None:
            row2 = df2.loc[row_idx_2]
            merged_data.append({
                "Scheme ID": scheme_id,
                "RTU ID": row2[rtu_col_2],
                "Site Name": name_1,
                "OVPN IP Address": row2[ip_col_2],
                "Agency Name": (row2[agency_col_2] if agency_col_2 else ''),
                "Match Score": round(score / 100.0, 2),
                "Matched Site Name": row2[site_col_2],
            })
        else:
            unmatched.append({"Scheme ID": scheme_id, "Site Name": name_1})

    final_df = (
        pd.DataFrame(merged_data)[["Scheme ID", "RTU ID", "Site Name", "OVPN IP Address", "Agency Name"]]
        if merged_data
        else pd.DataFrame(columns=["Scheme ID", "RTU ID", "Site Name", "OVPN IP Address", "Agency Name"])
    )
    return final_df, unmatched, []

def to_excel(df):
    output = BytesIO()
    # Use openpyxl for writing to avoid extra dependency on XlsxWriter
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Merged Data')
        worksheet = writer.sheets['Merged Data']
        # openpyxl uses 1-based indexing for columns
        for col_idx, col_name in enumerate(df.columns, start=1):
            if df.empty:
                max_cell_len = 0
            else:
                max_cell_len = int(df[col_name].astype(str).str.len().max())
            col_len = max(max_cell_len, len(col_name)) + 2
            worksheet.column_dimensions[get_column_letter(col_idx)].width = min(col_len, 40)
    output.seek(0)
    return output.getvalue()

# --- Streamlit UI ---

st.title("📊 Excel Disconnected Site Merger")

st.write("""
Upload two Excel files:
- **File 1**: Disconnected sites (Site Name, Scheme ID)
- **File 2**: RTU Information (Site Name, RTU ID, OVPN IP Address)
""")

file1 = st.file_uploader("Upload File 1 (Disconnected Sites)", type=['xlsx', 'xls'], help="Must contain Site Name & Scheme ID")
file2 = st.file_uploader("Upload File 2 (RTU Information)", type=['xlsx', 'xls'], help="Must contain Site Name, RTU ID, OVPN IP Address")


def _looks_like_html(content: bytes) -> bool:
    sample = content.strip().lower()[:2048]
    return sample.startswith(b"<html") or sample.startswith(b"<!doctype html") or b"<table" in sample

def _read_html_table(content: bytes, header: int = 0) -> Optional[pd.DataFrame]:
    try:
        tables = pd.read_html(content, header=header)  # requires bs4 + html5lib/lxml
        if not tables:
            return None
        # choose the largest table by area
        return max(tables, key=lambda t: (t.shape[0] * max(t.shape[1], 1)))
    except Exception:
        return None

def _load_excel_file(content: bytes, ext: str):
    """Return a pandas ExcelFile for faster multi-sheet probing."""
    buffer = BytesIO(content)
    if ext in ('.xlsx', '.xlsm'):
        return pd.ExcelFile(buffer, engine='openpyxl')
    elif ext == '.xls':
        return pd.ExcelFile(buffer, engine='xlrd')
    else:
        raise ValueError(f"Unsupported file type: {ext}")

def _score_file1(df: pd.DataFrame) -> Tuple[int, dict]:
    site_col = find_site_column(df)
    scheme_col = find_scheme_column(df)
    score = int(site_col is not None) + int(scheme_col is not None)
    return score, {"site": site_col, "scheme": scheme_col}

def _score_file2(df: pd.DataFrame) -> Tuple[int, dict]:
    site_col = find_site_column(df)
    rtu_col = find_rtu_column(df)
    ip_col = find_ip_column(df)
    agency_col = find_agency_column(df)
    score = (
        3 * int(site_col is not None)
        + 3 * int(rtu_col is not None)
        + 3 * int(ip_col is not None)
        + 1 * int(agency_col is not None)
    )
    return score, {"site": site_col, "rtu": rtu_col, "ip": ip_col, "agency": agency_col}

def read_excel_safely(uploaded_file, header=0, file_kind: str = 'generic'):
    """Read .xls/.xlsx, and gracefully handle HTML files disguised as .xls.
    """
    ext = Path(uploaded_file.name).suffix.lower()
    content = uploaded_file.getvalue()  # bytes

    # If it's actually HTML (common with exported .xls), parse via read_html
    if _looks_like_html(content):
        html_df = _read_html_table(content, header=header)
        if html_df is not None:
            st.info(f"Detected HTML content in {uploaded_file.name}; parsed the largest table.")
            return html_df
        raise ValueError("File appears to be HTML, but no tables could be parsed.")

    # Probe multiple sheets and header rows to pick the best-fit table
    xls = _load_excel_file(content, ext)
    best_df = None
    best_score = -1
    best_meta = (None, None)  # (sheet, header_row)
    scorer = _score_file1 if file_kind == 'file1' else (_score_file2 if file_kind == 'file2' else None)

    for sheet_name in xls.sheet_names:
        for hdr in [0, 1, 2, 3, 4]:
            try:
                df_try = xls.parse(sheet_name=sheet_name, header=hdr)
            except Exception:
                continue
            if df_try is None or df_try.empty:
                continue
            if scorer is None:
                # generic fallback: pick first non-empty
                best_df = df_try
                best_meta = (sheet_name, hdr)
                break
            score, _ = scorer(df_try)
            if score > best_score:
                best_score = score
                best_df = df_try
                best_meta = (sheet_name, hdr)
        if best_score >= (6 if file_kind == 'file2' else 2):
            # good enough: site+rtu+ip or site+scheme found
            break

    if best_df is None:
        raise ValueError("Could not read a usable sheet from the Excel file.")

    st.caption(f"Using sheet '{best_meta[0]}' (header row {best_meta[1]}) from {uploaded_file.name}")
    return best_df

if file1 and file2:
    try:
        df1 = read_excel_safely(file1, file_kind='file1')
        df2 = read_excel_safely(file2, file_kind='file2')

        st.write("Preview Disconnected Sites:")
        st.dataframe(df1.head())
        st.write("Preview RTU Information:")
        st.dataframe(df2.head())

        if st.button("🔄 Merge Files"):
            with st.spinner("Matching and merging..."):
                # Use substring rule: site name from file 2 contained within site name from file 1 (case-insensitive)
                merged, unmatched, missing_cols = merge_excels(df1, df2, match_mode="site2_in_site1", score_cutoff=80)

                if missing_cols:
                    st.error("Could not find required columns: " + ", ".join(missing_cols))
                else:
                    st.success(f"Merged successfully: {len(merged)} sites matched.")

                    tab_merged, tab_unmatched = st.tabs(["Merged Preview", "Unmatched Preview"])

                    with tab_merged:
                        st.dataframe(merged, use_container_width=True)
                        st.download_button(
                            "Download Merged Excel",
                            data=to_excel(merged),
                            file_name="merged.xlsx",
                        )

                    with tab_unmatched:
                        st.markdown(f"**Total unmatched sites:** {len(unmatched)}")
                        if unmatched:
                            unmatched_df = pd.DataFrame(unmatched)
                            st.dataframe(unmatched_df, use_container_width=True)
                            st.download_button(
                                "Download Unmatched Sites",
                                data=to_excel(unmatched_df),
                                file_name="unmatched.xlsx",
                            )
                        else:
                            st.info("No unmatched sites.")

    except Exception as e:
        st.error("Error: " + str(e))
        st.info("Check file formats and ensure required columns are present.")

else:
    st.info("Please upload both files to start merging.")

st.markdown("---")
st.write(
    "This app matches based on the **first word of the site name** and uses fuzzy matching (≥80% similarity). "
    "No sensitive data is stored."
)