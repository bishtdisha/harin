import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from difflib import SequenceMatcher

st.set_page_config(page_title="Excel Site Merger", layout="wide")

# --- Helpers ---

def clean_site_name(name):
    """Normalize site names for fuzzy matching."""
    if pd.isna(name): return ""
    name = str(name).strip().lower()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'[^\w\s]', '', name)
    return name

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def find_best_match(target, candidates, threshold=0.8):
    """Return the best matching site from candidates given a similarity threshold."""
    target_clean = clean_site_name(target)
    best_site = None
    best_score = 0
    for cand in candidates:
        cand_clean = clean_site_name(cand)
        score = similarity(target_clean, cand_clean)
        if score > best_score and score >= threshold:
            best_score = score
            best_site = cand
    return best_site, best_score

def find_column(df, keywords):
    """Find first column containing any of keywords (case-insensitive)."""
    for col in df.columns:
        for word in keywords:
            if word in col.lower():
                return col
    return None

def merge_excels(df1, df2):
    """Merge by best fuzzy matching of site names."""
    site_col_1 = find_column(df1, ['site', 'name'])
    scheme_col_1 = find_column(df1, ['scheme'])
    site_col_2 = find_column(df2, ['site', 'name'])
    rtu_col_2 = find_column(df2, ['rtu'])
    ip_col_2 = find_column(df2, ['ovpn', 'ip', 'address'])

    if not all([site_col_1, scheme_col_1, site_col_2, rtu_col_2, ip_col_2]):
        missing = []
        if not site_col_1: missing.append("Site Name in file 1")
        if not scheme_col_1: missing.append("Scheme ID in file 1")
        if not site_col_2: missing.append("Site Name in file 2")
        if not rtu_col_2: missing.append("RTU ID in file 2")
        if not ip_col_2: missing.append("OVPN IP Address in file 2")
        return None, None, missing

    merged_data = []
    unmatched = []
    candidates = df2[site_col_2].dropna().tolist()

    for _, row in df1.iterrows():
        name_1, scheme_id = row[site_col_1], row[scheme_col_1]
        match, score = find_best_match(name_1, candidates)
        if match:
            row2 = df2[df2[site_col_2]==match].iloc[0]
            merged_data.append({
                "Scheme ID": scheme_id,
                "RTU ID": row2[rtu_col_2],
                "Site Name": name_1,
                "OVPN IP Address": row2[ip_col_2],
                "Match Score": round(score,2),
                "Matched Site Name": match
            })
        else:
            unmatched.append({"Scheme ID": scheme_id, "Site Name": name_1})

    final_df = pd.DataFrame(merged_data)[["Scheme ID", "RTU ID", "Site Name", "OVPN IP Address"]] if merged_data else pd.DataFrame(columns=["Scheme ID", "RTU ID", "Site Name", "OVPN IP Address"])
    return final_df, unmatched, []

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Merged Data')
        workbook = writer.book
        worksheet = writer.sheets['Merged Data']
        for i, col in enumerate(df.columns):
            col_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, min(col_len, 40))
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

if file1 and file2:
    try:
        df1 = pd.read_excel(file1)
        df2 = pd.read_excel(file2)
        st.write("Preview Disconnected Sites:")
        st.dataframe(df1.head())
        st.write("Preview RTU Information:")
        st.dataframe(df2.head())

        if st.button("🔄 Merge Files"):
            with st.spinner("Matching and merging..."):
                merged, unmatched, missing_cols = merge_excels(df1, df2)

                if missing_cols:
                    st.error("Could not find required columns: " + ", ".join(missing_cols))
                else:
                    st.success(f"Merged successfully: {len(merged)} sites matched.")
                    st.dataframe(merged.head(10))

                    excel_data = to_excel(merged)
                    st.download_button("Download Merged Excel", data=excel_data, file_name="merged.xlsx")

                    st.markdown(f"**Total unmatched sites:** {len(unmatched)}")
                    if unmatched:
                        unmatched_df = pd.DataFrame(unmatched)
                        st.dataframe(unmatched_df.head(10))
                        st.download_button("Download Unmatched Sites", data=to_excel(unmatched_df),
                                           file_name="unmatched.xlsx")

    except Exception as e:
        st.error("Error: " + str(e))
        st.info("Check file formats and ensure required columns are present.")

else:
    st.info("Please upload both files to start merging.")

st.markdown("---")
st.write(
    "This app cleans site names and uses fuzzy matching (≥80% similarity) for robust merging. "
    "No sensitive data is stored."
)
