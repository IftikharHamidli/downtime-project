## Downtime Analysis Toolkit

This project contains a small set of Python scripts that I use to analyse terminal/ATM downtime based on Excel reports. The goal is to clean raw operational data, enrich downtime events with terminal metadata, and finally estimate financial loss during each downtime window.

The code is written for real-world files (internal Excel reports), so the repository does **not** include any sample data. Instead, it shows the full data-processing logic I use in my daily work.

### What this project does

- **Normalize terminal metadata**: Clean and deduplicate terminal/location records and standardize working hours information.
- **Attach working hours to downtime**: For each downtime event, find the correct working-hours schedule that was active at that time.
- **Classify downtime reasons**: Tag downtime events that are linked to displacement (terminal relocation).
- **Estimate financial loss**: For every downtime interval, estimate potential loss based on historical average hourly amounts.

All of this is done in plain Python using `pandas` and `numpy`, working directly with Excel files.

### Scripts and pipeline

The typical pipeline for a given month looks like this:

1. **`nonworking.py` – Clean terminals & working hours**
   - Input: raw terminals/location Excel file (e.g. `terminals location aprel for working hours.xlsx`).
   - What it does:
     - Normalizes addresses and terminal IDs.
     - Parses displacement dates from mixed formats (Excel serials and text dates).
     - Parses `Working_hours` text like `08:00-23:00` or `24 saat` into numeric hour lengths.
     - Deduplicates rows by `(Terminal_ID, Address, Displacement_date)`.
   - Output: a cleaned, compact terminals file like `aprel-location-cleaned-sec2-small.xlsx` with sheets:
     - `Deduped` – one row per unique terminal/location/date.
     - `Summary` – simple stats on how many rows were kept/removed.

2. **`workinghour.py` – Attach working/non-working hours to downtime**
   - Inputs:
     - A downtime/problem Excel file with a `Stop` sheet (e.g. `9.Aprel texniki 2025.xlsx`).
     - The cleaned terminals file from the previous step (e.g. `aprel-location-cleaned-sec2-small.xlsx`).
   - What it does:
     - Normalizes terminal IDs.
     - Builds a “lifetime history” for each terminal based on `Displacement_date` (from first installation up to future moves).
     - For each downtime event, finds the active working-hours interval at that moment.
     - Falls back to the most common `Working_hours` inside the same `(terminal, address)` group if direct matching fails.
     - Calculates:
       - `Working_hours_len` – number of working hours per day.
       - `Non_working_hours_len` – the remaining hours out of 24.
   - Output: an enriched downtime file like `9.Aprel texniki 2025 - with working hours (FINAL).xlsx` with all original columns plus working/non-working hour features.

3. **`stop_displacement.py` – Mark displacement-related downtime**
   - Inputs:
     - A `Stop`/downtime Excel file (e.g. `Stop finish.xlsx`).
     - A displacement file (e.g. `Displacement full.xlsx`) with one or more displacement dates per terminal.
   - What it does:
     - Normalizes terminal IDs.
     - Unpivots displacement dates into a long format.
     - For each terminal and displacement date, finds matching downtime rows:
       - First tries exact date matches.
       - Then searches near dates within a configurable window (±`MAX_NEAR_DAYS`, default 2 days).
     - Sets `"Downtime reason" = "Displacement"` for matched rows.
   - Output: an updated downtime file `Stop finish_displacement.xlsx` where displacement-related rows are clearly marked.

4. **`lossfinder.py` – Estimate financial loss during downtime**
   - Inputs:
     - A downtime file with `downtime_start` / `downtime_end` and terminal IDs (e.g. `Main-report-processed-final_with_qalan tickets finish 2_UPDATED loss_with_downtime_hours.xlsx`).
     - One or more Excel files with average hourly amounts per terminal (e.g. `yanvar - iyun dovriyye.xlsx`, `iyul - dekabr dovriyye.xlsx`).
   - What it does:
     - Normalizes terminal IDs on both sides.
     - Cleans and aggregates the average-amount data by `(terminal_id, month, hour_interval)`.
     - For each downtime row:
       - Builds the list of hourly bins that the downtime touches.
       - Joins those hours with historical averages.
       - Sums the amounts to compute a `Loss` value.
     - Keeps the original row count: invalid intervals (missing dates or end ≤ start) simply get `Loss = 0`.
   - Output: a downtime file with an extra `Loss` column (e.g. `Main-report-processed-final_with_qalan tickets finish 2_UPDATED_with_downtime_hours loss.xlsx`).

### How to run the scripts

1. **Install dependencies**

   Create and activate a virtual environment (optional but recommended), then install:

   ```bash
   pip install -r requirements.txt
   ```

2. **Place your Excel files**

   Put your Excel reports in the same folder as the scripts (or anywhere you like).
   Then open each `.py` file and update the `*_PATH` constants at the top so they point to your own filenames, for example:

   - In `nonworking.py`: `IN_PATH`, `OUT_PATH`.
   - In `workinghour.py`: `PROBLEMS_PATH`, `TERMINALS_PATH`, `OUT_FILE`.
   - In `stop_displacement.py`: the input file names in `read_excel`.
   - In `lossfinder.py`: `DOWNTIME_PATH`, `AVG_1_PATH`, `AVG_2_PATH`, `OUT_PATH`.

3. **Run step-by-step**

   From a terminal inside this folder:

   ```bash
   python nonworking.py
   python workinghour.py
   python stop_displacement.py
   python lossfinder.py
   ```

   Depending on your own reporting flow, you may not need every step for every month, but the typical pattern is:

   1. Prepare clean terminals/working-hours data.
   2. Attach working/non-working hours to downtime.
   3. Tag displacement-related downtime.
   4. Estimate financial loss for each downtime interval.

### Tech stack

- **Language**: Python
- **Libraries**: `pandas`, `numpy`, `openpyxl`
- **Input/Output**: Excel files (`.xlsx`)

### Notes for reviewers (portfolio context)

- This repository shows how I work with messy operational data:
  - Handling mixed date formats and Excel serial numbers safely.
  - Normalizing identifiers and addresses.
  - Building time-based interval logic for real terminals that move and change over time.
  - Designing practical rules to link downtime to business impact (lost amounts).
- The code is optimized for clarity over general-purpose reuse; file paths are kept as simple constants at the top of each script so they can be easily swapped for a new reporting period.

