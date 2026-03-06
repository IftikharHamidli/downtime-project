import pandas as pd
import numpy as np

# =============================
# PARAMETR
# =============================
MAX_NEAR_DAYS = 2   # 1 → near1, 2 → near2

# =============================
# OXU
# =============================
df_down = pd.read_excel("data.xlsx")
df_disp = pd.read_excel("data1.xlsx")

# =============================
# TARİXLƏR
# =============================
df_down["downtime_start"] = pd.to_datetime(df_down["downtime_start"], errors="coerce")
df_down["_ds"] = df_down["downtime_start"].dt.normalize()

# terminal id normalize (excel type problemləri üçün)
df_down["terminal_id"] = df_down["terminal_id"].astype(str).str.strip()

# =============================
# DISPLACEMENT → LONG
# =============================
disp_cols = ["Displacement_date 1", "Displacement_date 2"]

df_disp_long = (
    df_disp.melt(
        id_vars=["Terminal_ID"],
        value_vars=disp_cols,
        value_name="disp_date"
    )
    .dropna(subset=["disp_date"])
)

df_disp_long["Terminal_ID"] = df_disp_long["Terminal_ID"].astype(str).str.strip()
df_disp_long["disp_date"] = (
    pd.to_datetime(df_disp_long["disp_date"], errors="coerce")
      .dt.normalize()
)

df_disp_long = df_disp_long.dropna(subset=["disp_date"])

# =============================
# NƏTİCƏ MASK
# =============================
mark = np.zeros(len(df_down), dtype=bool)

down_groups = df_down.groupby("terminal_id", sort=False)
disp_groups = df_disp_long.groupby("Terminal_ID", sort=False)

# =============================
# ƏSAS MƏNTİQ
# =============================
for term, disp_g in disp_groups:
    if term not in down_groups.groups:
        continue

    idx = down_groups.groups[term]
    ds_series = df_down.loc[idx, "_ds"]

    # date → downtime row index-ləri
    date_to_rows = {}
    for d, rows in ds_series.groupby(ds_series).groups.items():
        date_to_rows[d] = rows

    disp_dates = disp_g["disp_date"].drop_duplicates()

    for disp_date in disp_dates:

        # 1️⃣ EXACT
        rows = date_to_rows.get(disp_date)
        if rows is not None:
            mark[df_down.index.get_indexer(rows)] = True
            continue   # 👉 DAYAN (near-lara baxma)

        # 2️⃣ NEAR 1, sonra NEAR 2 (pilləli)
        matched = False
        for delta in range(1, MAX_NEAR_DAYS + 1):
            for sign in (-1, 1):
                cand = disp_date + pd.Timedelta(days=sign * delta)
                rows = date_to_rows.get(cand)
                if rows is not None:
                    mark[df_down.index.get_indexer(rows)] = True
                    matched = True
                    break
            if matched:
                break   # 👉 near1 tapsa near2-yə KEÇMİR

# =============================
# YAZ
# =============================
df_down.loc[mark, "Downtime reason"] = "Displacement"

df_down.drop(columns=["_ds"], inplace=True, errors="ignore")
df_down.to_excel("Stop finish_displacement.xlsx", index=False)

print("Displacement yazılan sətrlər:", int(mark.sum()))
