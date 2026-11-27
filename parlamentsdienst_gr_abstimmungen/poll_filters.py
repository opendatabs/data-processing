"""Rules to detect test polls and invalid polls of the Grosser Rat.

The voting system of the Grosser Rat has no way to flag test polls. Following the rules provided by the
Parlamentsdienst, a poll is treated as follows:

- Session time (local time) is the union of
  (a) the standard windows incl. 30 min buffer for overrunning sessions: 09:00-12:30, 15:00-18:30, 20:00-23:30, and
  (b) the single sessions of that day in the session calendar, from start to end + 30 min
      (cancelled sessions, "abgesagt: ...", are ignored).
  (b) covers sessions with other times, e.g. during Covid (2020: 08:00-12:00, 14:00-18:00) or 2023-12-06 (14:00-16:00).
  (a) is kept because the calendar is not always correct: from 2022-11 to 2023-03 its entries are one hour early
  (08:00-11:00 / 14:00-17:00), although the sessions took place 09:00-12:00 / 15:00-18:00.
- The parliament has a quorum only if at least 50 members are present, so a poll with more than 50 absent
  members is not valid.

  outside session time and > 50 absent  -> test poll, removed
  inside session time and > 50 absent   -> failed poll (repeated afterwards), Typ set to "ungültig"
  anything else                         -> regular poll

The local time of a poll is taken from columns "Datum" (YYYY-MM-DD) and "Zeit" (HH:MM:SS...). For the JSON system,
the "started" field (used for "Zeitstempel") is sometimes off by one or two hours, while "Zeit" is correct.
Polls without a known time of day (Congress Center data only contains the date, i.e. 00:00:00) are never removed.
"""

import logging
from datetime import date, datetime, time

import pandas as pd

SESSION_WINDOWS = [
    (time(9, 0), time(12, 30)),
    (time(15, 0), time(18, 30)),
    (time(20, 0), time(23, 30)),
]
OVERRUN_BUFFER = pd.Timedelta(minutes=30)
MAX_ABSENT = 50
TYP_INVALID = "ungültig"
TZ = "Europe/Zurich"
# Columns that describe a removed test poll (see poll_report.py)
REMOVED_POLL_COLUMNS = [
    "session_date",
    "Datum",
    "Zeit",
    "Abst_Nr",
    "Anz_J",
    "Anz_N",
    "Anz_E",
    "Anz_A",
    "Anz_P",
    "Typ",
    "Geschaeft",
    "signatur_ges",
    "Traktandum",
    "Subtraktandum",
]


def _seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def _time_of_day_seconds(zeit: pd.Series) -> pd.Series:
    return pd.to_timedelta(zeit.astype(str).str[:8], errors="coerce").dt.total_seconds()


def _is_all_day(value) -> bool:
    """All-day events (e.g. 'Grossrats-Reise') have a date without time: datetime.date or 'YYYY-MM-DD'."""
    if isinstance(value, datetime):
        return False
    if isinstance(value, date):
        return True
    return isinstance(value, str) and len(value.strip()) <= 10


def calendar_windows(df_cal: pd.DataFrame) -> pd.DataFrame:
    """Session windows from the session calendar: one row per single session with columns
    Datum (YYYY-MM-DD, local), start_s and end_s (seconds since local midnight, end incl. buffer).
    Cancelled sessions ('abgesagt: ...') and all-day events are ignored."""
    is_cancelled = df_cal["summary"].astype(str).str.strip().str.lower().str.startswith("abgesagt")
    is_all_day = df_cal["dtstart"].map(_is_all_day)
    cal = df_cal[~is_cancelled & ~is_all_day]
    start = pd.to_datetime(cal["dtstart"], utc=True, errors="coerce").dt.tz_convert(TZ)
    end = pd.to_datetime(cal["dtend"], utc=True, errors="coerce").dt.tz_convert(TZ)
    valid = start.notna() & end.notna() & (end > start)
    start, end = start[valid], end[valid]
    midnight = start.dt.normalize()
    windows = pd.DataFrame(
        {
            "Datum": start.dt.strftime("%Y-%m-%d"),
            "start_s": (start - midnight).dt.total_seconds(),
            "end_s": (end + OVERRUN_BUFFER - midnight).dt.total_seconds(),
        }
    )
    return windows.drop_duplicates().reset_index(drop=True)


def is_in_standard_window(zeit: pd.Series) -> pd.Series:
    """True if the local time of day in 'zeit' (e.g. '09:24:56.000') lies within a standard session window."""
    secs = _time_of_day_seconds(zeit)
    in_window = pd.Series(False, index=zeit.index)
    for start, end in SESSION_WINDOWS:
        in_window |= (secs >= _seconds(start)) & (secs < _seconds(end))
    return in_window


def is_in_calendar_window(df: pd.DataFrame, windows: pd.DataFrame | None) -> pd.Series:
    """True if the poll (Datum, Zeit) lies within a single session of that day in the session calendar."""
    if windows is None or windows.empty:
        return pd.Series(False, index=df.index)
    polls = pd.DataFrame({"Datum": df["Datum"].astype(str), "secs": _time_of_day_seconds(df["Zeit"])}, index=df.index)
    polls["_row"] = range(len(polls))
    merged = polls.merge(windows, on="Datum", how="inner")
    hit = merged.loc[(merged["secs"] >= merged["start_s"]) & (merged["secs"] < merged["end_s"]), "_row"].unique()
    return pd.Series(polls["_row"].isin(hit).to_numpy(), index=df.index)


def has_known_time(zeit: pd.Series) -> pd.Series:
    """False for missing or midnight times (no time of day available, e.g. Congress Center data)."""
    secs = _time_of_day_seconds(zeit)
    return secs.notna() & (secs > 0)


def is_without_quorum(anz_a: pd.Series) -> pd.Series:
    return pd.to_numeric(anz_a, errors="coerce") > MAX_ABSENT


def removed_poll_rows(df: pd.DataFrame) -> pd.DataFrame:
    """One row per poll (member level data is collapsed), only the columns needed to describe a poll."""
    return df.reindex(columns=REMOVED_POLL_COLUMNS).drop_duplicates(subset=["Datum", "Zeit", "Abst_Nr"])


def clean_polls(
    df: pd.DataFrame, windows: pd.DataFrame | None = None, removed_polls: list[pd.DataFrame] | None = None
) -> pd.DataFrame:
    """Remove test polls and mark failed polls as invalid. Works on poll level and member level dataframes.

    windows: result of calendar_windows(); without it, only the standard windows are used.
    removed_polls: if given, one dataframe with the removed test polls (one row per poll) is appended to it.
    """
    if df.empty:
        return df
    in_window = is_in_standard_window(df["Zeit"]) | is_in_calendar_window(df, windows)
    no_quorum = is_without_quorum(df["Anz_A"])
    is_test = no_quorum & ~in_window & has_known_time(df["Zeit"])
    is_failed = no_quorum & ~is_test

    if is_test.any():
        removed = removed_poll_rows(df.loc[is_test])
        logging.info(
            f"Removing {len(removed)} test poll(s):\n"
            f"{removed[['Datum', 'Zeit', 'Abst_Nr', 'Anz_A']].to_string(index=False)}"
        )
        if removed_polls is not None:
            removed_polls.append(removed)
    if is_failed.any():
        flagged = df.loc[is_failed, ["Datum", "Zeit", "Abst_Nr", "Anz_A"]].drop_duplicates()
        logging.info(
            f"Marking {len(flagged)} poll(s) without quorum as '{TYP_INVALID}':\n{flagged.to_string(index=False)}"
        )

    df = df.loc[~is_test].copy()
    df.loc[is_failed[~is_test], "Typ"] = TYP_INVALID
    return df.reset_index(drop=True)
