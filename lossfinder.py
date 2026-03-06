import pandas as pd
import numpy as np


DOWNTIME_PATH = r"data.xlsx"

AVG_1_PATH = r"data1.xlsx"   
AVG_2_PATH = r"data2.xlsx"   

OUT_PATH = r"output data.xlsx"


DT_TERMINAL_COL = "terminal_id"
DT_START_COL    = "downtime_start"
DT_END_COL      = "downtime_end"

AVG_TERMINAL_COL = "terminal_id"
AVG_MONTH_COL    = "month"
AVG_HOUR_COL     = "hour_interval"   
AVG_AMOUNT_COL   = "average_amount"



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

    
    df[DT_TERMINAL_COL] = df[DT_TERMINAL_COL].map(norm_tid)
    a[AVG_TERMINAL_COL] = a[AVG_TERMINAL_COL].map(norm_tid)

   
    df[DT_START_COL] = pd.to_datetime(df[DT_START_COL], errors="coerce")
    df[DT_END_COL]   = pd.to_datetime(df[DT_END_COL], errors="coerce")

   
    df["Loss"] = 0.0

   
    valid = (
        df[DT_START_COL].notna()
        & df[DT_END_COL].notna()
        & (df[DT_END_COL] > df[DT_START_COL])
    )
    if not valid.any():
        return df

    dfv = df.loc[valid].copy()

    
    a[AVG_MONTH_COL] = pd.to_numeric(a[AVG_MONTH_COL], errors="coerce")
    a[AVG_HOUR_COL]  = pd.to_numeric(a[AVG_HOUR_COL], errors="coerce")
    a[AVG_AMOUNT_COL] = pd.to_numeric(a[AVG_AMOUNT_COL], errors="coerce").fillna(0.0)

    
    a = a.dropna(subset=[AVG_MONTH_COL, AVG_HOUR_COL]).copy()
    a[AVG_MONTH_COL] = a[AVG_MONTH_COL].astype(int)
    a[AVG_HOUR_COL]  = a[AVG_HOUR_COL].astype(int)

   
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

    
    exp[AVG_MONTH_COL] = exp["_hour_dt"].dt.month.astype(int)

    
    h = exp["_hour_dt"].dt.hour.to_numpy()
    exp[AVG_HOUR_COL] = np.where(h == 0, 24, h).astype(int)

  
    exp = exp.merge(
        a[[AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL, AVG_AMOUNT_COL]],
        left_on=[DT_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL],
        right_on=[AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL],
        how="left",
    )

    exp[AVG_AMOUNT_COL] = exp[AVG_AMOUNT_COL].fillna(0.0)

    
    loss_by_row = exp.groupby("row_id")[AVG_AMOUNT_COL].sum()

    
    df.loc[loss_by_row.index, "Loss"] = loss_by_row.values

   
    df.drop(columns=["_start_floor", "_end_ceil"], inplace=True, errors="ignore")
    return df



def load_avg_excel(path: str) -> pd.DataFrame:
    usecols = [AVG_TERMINAL_COL, AVG_MONTH_COL, AVG_HOUR_COL, AVG_AMOUNT_COL]
    return pd.read_excel(path, usecols=usecols, engine="openpyxl")


def main():
    
    downtime_df = pd.read_excel(DOWNTIME_PATH, engine="openpyxl")

    
    avg1 = load_avg_excel(AVG_1_PATH)
    avg2 = load_avg_excel(AVG_2_PATH)
    avg_df = pd.concat([avg1, avg2], ignore_index=True)

    
    term_set = set(downtime_df[DT_TERMINAL_COL].map(norm_tid).unique())
    avg_df[AVG_TERMINAL_COL] = avg_df[AVG_TERMINAL_COL].map(norm_tid)
    avg_df = avg_df[avg_df[AVG_TERMINAL_COL].isin(term_set)].copy()

    
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

    
    out = compute_loss_overlap_hours_keep_rows(downtime_df, avg_df)

  
    out.to_excel(OUT_PATH, index=False)
    print("DONE:", OUT_PATH)
    print("Input rows :", len(downtime_df))
    print("Output rows:", len(out))


if __name__ == "__main__":
    main()

