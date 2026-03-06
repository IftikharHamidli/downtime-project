import pandas as pd
import numpy as np

# ==========================
# Fayl yollarını BURADA dəyiş
# ==========================
PROBLEMS_PATH  = r"9.Aprel texniki 2025.xlsx"              # Stop sheet olan fayl
TERMINALS_PATH = r"aprel-location-cleaned-sec2-small.xlsx"  # Deduped sheet olan fayl
OUT_FILE       = r"9.Aprel texniki 2025 - with working hours (FINAL).xlsx"


def norm_tid(s, width=5):
    """Terminal ID-ni soldakı sıfırları atıb sonra zfill edir."""
    s = str(s).strip()
    s = s.lstrip("0") or "0"
    return s.zfill(width)


def hours_from_interval(txt):
    """
    Working_hours text-dən (məs: '08:00-23:00' və ya '24 saat') saat sayını çıxarır.
    """
    if pd.isna(txt):
        return np.nan
    s = str(txt).strip()
    # 24 saat
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
            # Gecə yarısını keçən intervallar üçün
            diff += 24 * 60
        return round(diff / 60, 2)
    except Exception:
        return np.nan


def norm_addr(s):
    """Adresləri lower-case və tək boşluqlu formaya salır."""
    if pd.isna(s):
        return s
    s = str(s).lower().strip()
    return " ".join(s.split())


# ==========================
# 1) Faylları oxu
# ==========================
print("📥 Fayllar oxunur...")
p = pd.read_excel(PROBLEMS_PATH, sheet_name="Stop")
t = pd.read_excel(TERMINALS_PATH, sheet_name="Deduped")

# 👉 ORİJİNAL downtime_start-ı saxla (itirməmək üçün)
p["downtime_start_raw"] = p["downtime_start"]

# ==========================
# 2) Normalizasiya
# ==========================
print("🔧 Sahələr normallaşdırılır...")

p["downtime_start"] = pd.to_datetime(p["downtime_start"], errors="coerce", dayfirst=True)
p["terminal_id_norm"] = p["terminal_id"].apply(norm_tid)

t["Terminal_ID_norm"] = t["Terminal_ID"].apply(norm_tid)
t["Displacement_date"] = pd.to_datetime(t["Displacement_date"], errors="coerce", dayfirst=True)

# Terminal həyat intervalları
t = t.sort_values(["Terminal_ID_norm", "Displacement_date"], na_position="first").copy()
t["Start_ts"] = t.groupby("Terminal_ID_norm", group_keys=False)["Displacement_date"]\
    .apply(lambda s: s.where(~s.isna(), pd.Timestamp(1900, 1, 1)))
t["Next_Start"] = t.groupby("Terminal_ID_norm")["Start_ts"].shift(-1)
t["End_ts"] = (t["Next_Start"] - pd.Timedelta(seconds=1)).fillna(
    pd.Timestamp(2100, 12, 31, 23, 59, 59)
)

hist = t[["Terminal_ID_norm", "Start_ts", "End_ts", "Working_hours"]]\
    .rename(columns={"Terminal_ID_norm": "terminal_id_norm"})

# ==========================
# 3) Interval merge + fallback məntiqi
# ==========================
print("🔗 Interval merge və fallback tətbiq olunur...")

p = p.reset_index(drop=False).rename(columns={"index": "orig_idx"})
mergeable = p[p["downtime_start"].notna()].copy()
nonmerge = p[p["downtime_start"].isna()].copy()
mergeable["Working_hours_at_open"] = np.nan

for tid, g in mergeable.groupby("terminal_id_norm", group_keys=False):
    left = g.sort_values("downtime_start").copy()
    right = hist[hist["terminal_id_norm"] == tid].copy()
    if right.empty:
        continue

    # 3.1. Backward merge (aktuallıq üçün interval check)
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

    # 3.2. Forward merge (fallback üçün, növbəti displacement-ə ≤ 24 saat)
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

# Birləşdir
out = pd.concat([mergeable, nonmerge], ignore_index=False).sort_values("orig_idx")
out = out.drop(columns=["orig_idx"]).reset_index(drop=True)

# ==========================
# 4) Eyni (terminal_id, address) qrupunda mode ilə doldurma (optimizə)
# ==========================
print("📊 Qruplar üzrə mode ilə boş Working_hours doldurulur...")

out["__tid"] = out["terminal_id_norm"]
out["__addr_norm"] = out["address"].apply(norm_addr)

# NA olmayan sətirlərdən mode hesablanır
non_na = out[out["Working_hours_at_open"].notna()].copy()
mode_df = (
    non_na
    .groupby(["__tid", "__addr_norm"])["Working_hours_at_open"]
    .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan)
    .reset_index()
    .rename(columns={"Working_hours_at_open": "WH_mode"})
)

# Mode dəyərlərini əsas dataframə merge edirik
out = out.merge(mode_df, on=["__tid", "__addr_norm"], how="left")

# Əgər Working_hours_at_open boşdursa → WH_mode ilə doldur
out["Working_hours_at_open"] = out["Working_hours_at_open"].fillna(out["WH_mode"])

# Texniki sütunları sil
out = out.drop(columns=["__tid", "__addr_norm", "WH_mode"])

# ==========================
# 5) Working / Non-working saatların hesablanması
# ==========================
print("⏱ İşlənən və işlənməyən saatlar hesablanır...")

out["Working_hours_len"] = out["Working_hours_at_open"].map(hours_from_interval)
out["Non_working_hours_len"] = out["Working_hours_len"].map(
    lambda x: round(24 - x, 2) if pd.notna(x) else np.nan
)

# 👉 SONA YAXIN: parse edilməyən tarixləri orijinal textlə bərpa edirik
mask_restore = out["downtime_start"].isna() & out["downtime_start_raw"].notna()
out.loc[mask_restore, "downtime_start"] = out.loc[mask_restore, "downtime_start_raw"]

# ==========================
# 6) Nəticəni Excel-ə yaz
# ==========================
print("📤 Excel faylı yazılır:", OUT_FILE)

with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
    out.to_excel(writer, sheet_name="Stop_with_hours", index=False)

print("✅ Hazır!")
print("Sətir sayı:", len(out))
print("İş saatı tapılan sətir:", out["Working_hours_at_open"].notna().sum())