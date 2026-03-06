# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta


IN_PATH  = r"data.xlsx"
OUT_PATH = r"data1.xlsx"


def find_col(cols, patterns):
    pats = [p.lower() for p in patterns]
    for c in cols:
        if any(p in str(c).lower() for p in pats):
            return c
    return None

def normalize_address_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )



def parse_date_series(series: pd.Series) -> pd.Series:
    """Serial + string tarixləri təhlükəsiz çevirir, overflow vermir."""
    
   
    parsed_str = pd.to_datetime(series, dayfirst=True, errors="coerce")

   
    numeric = pd.to_numeric(series, errors="coerce")

    def excel_safe(x):
        if pd.isna(x):
            return None

        try:
            x = float(x)
        except:
            return None

        
        if not (0 <= x <= 60000):
            return None

        base = datetime(1899, 12, 30)
        try:
            return (base + timedelta(days=x)).date()
        except OverflowError:
            return None

    parsed_serial = numeric.apply(excel_safe)

    
    result = []
    for d_txt, d_ser in zip(parsed_str, parsed_serial):
        if pd.notna(d_txt):
            result.append(d_txt.date())
        else:
            result.append(d_ser)

    return pd.Series(result)



WH_24_SET = {"24", "24:00", "00:00-24:00"}

def working_hours_to_diff_int(val):
    if pd.isna(val):
        return np.nan

    s = str(val).strip()

    if s in WH_24_SET or re.fullmatch(r"0{1,2}:?0{0,2}\s*-\s*24:?0{0,2}", s):
        return 24

    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$", s)
    if not m:
        return np.nan

    h1, m1, h2, m2 = map(int, m.groups())
    start = h1*60 + m1
    end   = h2*60 + m2

    diff_min = end - start
    if diff_min < 0:  
        diff_min += 24*60

    return int(diff_min // 60)



df = pd.read_excel(IN_PATH)

term_col = find_col(df.columns, ["terminal_id", "terminal id"])
addr_col = find_col(df.columns, ["address", "ünvan", "unvan"])
date_col = find_col(df.columns, ["displacement_date", "displacemen", "displ", "date of displacement"])
wh_col   = find_col(df.columns, ["working_hours", "working hour", "iş saat"])
sec2_col = find_col(df.columns, ["sec_category_2", "sec category 2"])

if not (term_col and addr_col and date_col):
    raise RuntimeError(
        f"Sütun tapılmadı. Terminal: {term_col}, Address: {addr_col}, Date: {date_col}"
    )



if wh_col:
    df["Working_hours_diff"] = df[wh_col].apply(working_hours_to_diff_int)



df["_addr_norm"] = normalize_address_series(df[addr_col])
df["_date_only"] = parse_date_series(df[date_col])

dup_mask = df.duplicated(
    subset=[term_col, "_addr_norm", "_date_only"],
    keep="first"
)
df_keep = df.loc[~dup_mask].copy()



final_cols_map = {
    term_col: "Terminal_ID",
    addr_col: "Address",
    date_col: "Displacement_date",
}

if wh_col:
    final_cols_map[wh_col] = "Working_hours"

if "Working_hours_diff" in df_keep.columns:
    final_cols_map["Working_hours_diff"] = "Working_hours_diff"

if sec2_col:
    final_cols_map[sec2_col] = "Sec_Category_2"

deduped_out = df_keep[list(final_cols_map.keys())].rename(columns=final_cols_map)



with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as w:
    deduped_out.to_excel(w, index=False, sheet_name="Deduped")
    pd.DataFrame({
        "Metric": ["Input rows", "Kept rows", "Removed as exact dups"],
        "Value":  [len(df), len(df_keep), len(df) - len(df_keep)]
    }).to_excel(w, index=False, sheet_name="Summary")

print("READY:", OUT_PATH)

