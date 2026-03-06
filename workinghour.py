import pandas as pd
import numpy as np


PROBLEMS_PATH  = r"data.xlsx"              
TERMINALS_PATH = r"data1.xlsx"  
OUT_FILE       = r"data2.xlsx"


def norm_tid(s, width=5):
 
    s = str(s).strip()
    s = s.lstrip("0") or "0"
    return s.zfill(width)


def hours_from_interval(txt):
 
    if pd.isna(txt):
        return np.nan
    s = str(txt).strip()
    
    if "-" not in s and "24" in s:
        return 24.0
    if "-" not in s:
        return np.nan
    try:
        start, end = [x.strip() for x in s.split("-", 1)]

        def to_minutes(x):
            parts = x.split(":")
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
            return h * 60 + m

        s_min = to_minutes(start)
        e_min = to_minutes(end)
        diff = e_min - s_min
        if diff < 0:
            
            diff += 24 * 60
        return round(diff / 60, 2)
    except Exception:
        return np.nan


def norm_addr(s):
    
    if pd.isna(s):
        return s
    s = str(s).lower().strip()
    return " ".join(s.split())



print("📥 Files reading...")
p = pd.read_excel(PROBLEMS_PATH, sheet_name="Stop")
t = pd.read_excel(TERMINALS_PATH, sheet_name="Deduped")


p["downtime_start_raw"] = p["downtime_start"]


print("🔧 Normalization...")

p["downtime_start"] = pd.to_datetime(p["downtime_start"], errors="coerce", dayfirst=True)
p["terminal_id_norm"] = p["terminal_id"].apply(norm_tid)

t["Terminal_ID_norm"] = t["Terminal_ID"].apply(norm_tid)
t["Displacement_date"] = pd.to_datetime(t["Displacement_date"], errors="coerce", dayfirst=True)


t = t.sort_values(["Terminal_ID_norm", "Displacement_date"], na_position="first").copy()
t["Start_ts"] = t.groupby("Terminal_ID_norm", group_keys=False)["Displacement_date"]\
    .apply(lambda s: s.where(~s.isna(), pd.Timestamp(1900, 1, 1)))
t["Next_Start"] = t.groupby("Terminal_ID_norm")["Start_ts"].shift(-1)
t["End_ts"] = (t["Next_Start"] - pd.Timedelta(seconds=1)).fillna(
    pd.Timestamp(2100, 12, 31, 23, 59, 59)
)

hist = t[["Terminal_ID_norm", "Start_ts", "End_ts", "Working_hours"]]\
    .rename(columns={"Terminal_ID_norm": "terminal_id_norm"})


print("🔗 Interval merge və fallback...")

p = p.reset_index(drop=False).rename(columns={"index": "orig_idx"})
mergeable = p[p["downtime_start"].notna()].copy()
nonmerge = p[p["downtime_start"].isna()].copy()
mergeable["Working_hours_at_open"] = np.nan

for tid, g in mergeable.groupby("terminal_id_norm", group_keys=False):
    left = g.sort_values("downtime_start").copy()
    right = hist[hist["terminal_id_norm"] == tid].copy()
    if right.empty:
        continue

    
    back = pd.merge_asof(
        left,
        right,
        left_on="downtime_start",
        right_on="Start_ts",
        direction="backward",
        allow_exact_matches=True
    )
    valid_current = back["downtime_start"] <= back["End_ts"]
    back["WH_current"] = back["Working_hours"].where(valid_current)

 
    fwd = pd.merge_asof(
        left[["downtime_start"]],
        right[["Start_ts", "Working_hours"]],
        left_on="downtime_start",
        right_on="Start_ts",
        direction="forward",
        allow_exact_matches=False
    ).rename(columns={"Start_ts": "Next_Start_ts", "Working_hours": "Working_hours_next"})

    delta = fwd["Next_Start_ts"] - left["downtime_start"]
    use_next = (
        back["WH_current"].isna()
        & delta.notna()
        & (delta <= pd.Timedelta(hours=24))
        & (delta > pd.Timedelta(0))
    )

    result = back["WH_current"].copy()
    result[use_next] = fwd.loc[use_next, "Working_hours_next"]

    mergeable.loc[left.index, "Working_hours_at_open"] = result.values


out = pd.concat([mergeable, nonmerge], ignore_index=False).sort_values("orig_idx")
out = out.drop(columns=["orig_idx"]).reset_index(drop=True)


print("📊 Filling Working_hours due to groups ...")

out["__tid"] = out["terminal_id_norm"]
out["__addr_norm"] = out["address"].apply(norm_addr)


non_na = out[out["Working_hours_at_open"].notna()].copy()
mode_df = (
    non_na
    .groupby(["__tid", "__addr_norm"])["Working_hours_at_open"]
    .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan)
    .reset_index()
    .rename(columns={"Working_hours_at_open": "WH_mode"})
)


out = out.merge(mode_df, on=["__tid", "__addr_norm"], how="left")


out["Working_hours_at_open"] = out["Working_hours_at_open"].fillna(out["WH_mode"])


out = out.drop(columns=["__tid", "__addr_norm", "WH_mode"])


print("⏱ Working and non-working hour...")

out["Working_hours_len"] = out["Working_hours_at_open"].map(hours_from_interval)
out["Non_working_hours_len"] = out["Working_hours_len"].map(
    lambda x: round(24 - x, 2) if pd.notna(x) else np.nan
)


mask_restore = out["downtime_start"].isna() & out["downtime_start_raw"].notna()
out.loc[mask_restore, "downtime_start"] = out.loc[mask_restore, "downtime_start_raw"]


print("📤 Excel file creating:", OUT_FILE)

with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
    out.to_excel(writer, sheet_name="Stop_with_hours", index=False)

print("✅ Ready!")
print("Row counts:", len(out))
print("Working hour rows:", out["Working_hours_at_open"].notna().sum())

