import pandas as pd
import numpy as np

# =========================
# CONFIG (burda path-ları yaz)
# =========================
DOWNTIME_PATH = r"Main-report-processed-final_with_qalan tickets finish 2_UPDATED loss_with_downtime_hours.xlsx"

AVG_1_PATH = r"yanvar - iyun dovriyye.xlsx"   # 1-6 aylar
AVG_2_PATH = r"iyul - dekabr dovriyye.xlsx"   # 7-12 aylar

OUT_PATH = r"Main-report-processed-final_with_qalan tickets finish 2_UPDATED_with_downtime_hours loss.xlsx"

# Sütun adların fərqlidirsə buranı dəyiş
DT_TERMINAL_COL = "terminal_id"
DT_START_COL    = "downtime_start"
DT_END_COL      = "downtime_end"

AVG_TERMINAL_COL = "terminal_id"
AVG_MONTH_COL    = "month"
AVG_HOUR_COL     = "hour_interval"   # səndə 1..24 kimidir
AVG_AMOUNT_COL   = "average_amount"


# =========================
# HELPERS
# =========================
def norm_tid(x) -> str:
    """
    terminal_id normalizasiya:
    - 70.0 kimi gələrsə -> 70
    - 00070 -> 70
    """
    s = "" if pd.isna(x) else str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if "." in s:
        s = s.split(".")[0]
    s2 = s.lstrip("0")
    return s2 if s2 != "" else "0"


# =========================
# CORE FUNCTION (ROW SAYI DEYISMIr, LOSS DUZGUN)
# =========================
def compute_loss_overlap_hours_keep_rows(downtime_df: pd.DataFrame, avg_df: pd.DataFrame) -> pd.DataFrame:
    """
    Row sayı DEYİŞMİR.
    - Problemli interval (NaT / end<=start) sətrlərdə Loss = 0 qalır.
    - Düzgün interval sətrlərdə Loss hesablanır:
      Interval hansı saatlara toxunursa (hour bin), həmin saatların average_amount-u TAM cəmlənir.
      Yarım saat / çəki yoxdur.
    Qeyd: AVG hour_interval səndə 1..24 formatındadır:
      1->01:00? (səndə praktik olaraq 7..23 görünür), 23->23:00-24:00, 24->00:00-01:00 kimi istifadə edirik.
      Bu mapping: dt.hour==0 => 24, əks halda dt.hour.
    """
    df = downtime_df.copy()
    a = avg_df.copy()

    # ---- normalize terminal_id hər iki tərəfdə ----
    df[DT_TERMINAL_COL] = df[DT_TERMINAL_COL].map(norm_tid)
    a[AVG_TERMINAL_COL] = a[AVG_TERMINAL_COL].map(norm_tid)

    # ---- parse datetime ----
    df[DT_START_COL] = pd.to_datetime(df[DT_START_COL], errors="coerce")
    df[DT_END_COL]   = pd.to_datetime(df[DT_END_COL], errors="coerce")

    # ---- default: heç nə itməsin ----
    df["Loss"] = 0.0

    # ---- yalnız düzgün interval-lar hesablanacaq ----
    valid = (
        df[DT_START_COL].notna()
        & df[DT_END_COL].notna()
        & (df[DT_END_COL] > df[DT_START_COL])
    )
    if not valid.any():
        return df

    dfv = df.loc[valid].copy()

    # ---- types & cleanup avg ----
    a[AVG_MONTH_COL] = pd.to_numeric(a[AVG_MONTH_COL], errors="coerce")
    a[AVG_HOUR_COL]  = pd.to_numeric(a[AVG_HOUR_COL], errors="coerce")
    a[AVG_AMOUNT_COL] = pd.to_numeric(a[AVG_AMOUNT_COL], errors="coerce").fillna(0.0)

    # yalnız düzgün month/hour saxla
    a = a.dropna(subset=[AVG_MONTH_COL, AVG_HOUR_COL]).copy()
    a[AVG_MONTH_COL] = a[AVG_MONTH_COL].astype(int)
    a[AVG_HOUR_COL]  = a[AVG_HOUR_COL].astype(int)

    # ====== build touched hours for each downtime row ======
    dfv["_start_floor"] = dfv[DT_START_COL].dt.floor("h")
    dfv["_end_ceil"]    = dfv[DT_END_COL].dt.ceil("h")

    n = ((dfv["_end_ceil"] - dfv["_start_floor"]) / pd.Timedelta(hours=1)).astype(int)
    n = n.clip(lower=0)

    rep_idx = np.repeat(dfv.index.to_numpy(), n.to_numpy())
    exp = dfv.loc[rep_idx, [DT_TERMINAL_COL, "_start_floor"]].copy()
    exp["row_id"] = rep_idx

    offsets = (
        np.concatenate([np.arange(k) for k in n.to_numpy()])
        if n.sum() > 0 else np.array([], dtype=int)
    )
    exp["_hour_dt"] = exp["_start_floor"].to_numpy() + pd.to_timedelta(offsets, unit="h")

    # join keys
    exp[AVG_MONTH_COL] = exp["_hour_dt"].dt.month.astype(int)

    # *** ƏSAS FIX: hour_interval 1..24 mapping ***
    # dt.hour: 0..23
    # avg hour_interval: 1..24 (səndə gecə 00:xx üçün 24 istifadə edirik)
    h = exp["_hour_dt"].dt.hour.to_numpy()
    exp[AVG_HOUR_COL] = np.where(h == 0, 24, h).astype(int)

    # merge with avg
    exp = exp.merge(
        a[[AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL, AVG_AMOUNT_COL]],
        left_on=[DT_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL],
        right_on=[AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL],
        how="left",
    )

    exp[AVG_AMOUNT_COL] = exp[AVG_AMOUNT_COL].fillna(0.0)

    # sum per downtime row
    loss_by_row = exp.groupby("row_id")[AVG_AMOUNT_COL].sum()

    # 👉 Loss-u əsas df-ə geri yaz (row sayı dəyişmədən)
    df.loc[loss_by_row.index, "Loss"] = loss_by_row.values

    # cleanup helper cols
    df.drop(columns=["_start_floor", "_end_ceil"], inplace=True, errors="ignore")
    return df


# =========================
# LOADERS (2 avg excel)
# =========================
def load_avg_excel(path: str) -> pd.DataFrame:
    usecols = [AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL, AVG_AMOUNT_COL]
    return pd.read_excel(path, usecols=usecols, engine="openpyxl")


def main():
    # --- load downtime ---
    downtime_df = pd.read_excel(DOWNTIME_PATH, engine="openpyxl")

    # --- load avg from two excels and concat ---
    avg1 = load_avg_excel(AVG_1_PATH)
    avg2 = load_avg_excel(AVG_2_PATH)
    avg_df = pd.concat([avg1, avg2], ignore_index=True)

    # RAM azaltmaq: yalnız downtime-da olan terminal-lar
    term_set = set(downtime_df[DT_TERMINAL_COL].map(norm_tid).unique())
    avg_df[AVG_TERMINAL_COL] = avg_df[AVG_TERMINAL_COL].map(norm_tid)
    avg_df = avg_df[avg_df[AVG_TERMINAL_COL].isin(term_set)].copy()

    # təkrar varsa cəmlə
    avg_df[AVG_AMOUNT_COL] = pd.to_numeric(avg_df[AVG_AMOUNT_COL], errors="coerce").fillna(0.0)
    avg_df[AVG_MONTH_COL]  = pd.to_numeric(avg_df[AVG_MONTH_COL], errors="coerce")
    avg_df[AVG_HOUR_COL]   = pd.to_numeric(avg_df[AVG_HOUR_COL], errors="coerce")

    avg_df = avg_df.dropna(subset=[AVG_MONTH_COL, AVG_HOUR_COL]).copy()
    avg_df[AVG_MONTH_COL] = avg_df[AVG_MONTH_COL].astype(int)
    avg_df[AVG_HOUR_COL]  = avg_df[AVG_HOUR_COL].astype(int)

    avg_df = (
        avg_df.groupby([AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL], as_index=False)[AVG_AMOUNT_COL]
        .sum()
    )

    # --- compute loss (ROW SAYI DEYISMIr) ---
    out = compute_loss_overlap_hours_keep_rows(downtime_df, avg_df)

    # --- save ---
    out.to_excel(OUT_PATH, index=False)
    print("DONE:", OUT_PATH)
    print("Input rows :", len(downtime_df))
    print("Output rows:", len(out))


if __name__ == "__main__":
    main()
