from collections.abc import Sequence
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _as_bool(v) -> bool:
    if v is pd.NA or v is None:
        return False
    if isinstance(v, (list, tuple, np.ndarray)):
        return bool(np.any(v))  # True if any element truthy
    return bool(v)


def availability_histogram(
    df: pd.DataFrame,
    output: str | Path = "availability_histogram.png",
    show: bool = False,
) -> Path:
    """
    Saves a horizontal bar-chart of all *_avail flag combinations.
    Each bar is annotated with "count (pct%)".
    """
    flags = ["Koordinaten_avail", "Strasse_avail", "Hausnummer_avail", "Ort_avail"]

    subset = df[flags].applymap(_as_bool)

    counts = subset.value_counts().sort_values(ascending=False).rename_axis(flags).reset_index(name="n")
    total = counts["n"].sum()
    counts["pct"] = 100 * counts["n"] / total

    # readable x-axis labels
    counts["label"] = counts.apply(lambda r: ", ".join(f"{f}" if r[f] else f"no {f}" for f in flags), axis=1)

    ax = counts.plot.barh(y="n", x="label", legend=False, figsize=(10, 6), width=0.8)

    # annotate each bar with count + pct
    x_pad = counts["n"].max() * 0.01  # 1 % of the longest bar
    for idx, (n, pct) in counts[["n", "pct"]].iterrows():
        ax.text(
            n + x_pad,
            idx,  # a tiny bump to the right
            f"{n:,.0f}  ({pct:.1f}%)",
            va="center",
            ha="left",
            fontsize=9,
        )

    ax.set_xlabel("Number of entries")
    ax.set_title("Availability combinations of location data")
    plt.tight_layout()

    out = Path(output)
    plt.savefig(out, dpi=150)
    if show:
        plt.show()
    plt.close()
    return out


# --------------------------------------------------------------------- #
# helper: bucket label
# --------------------------------------------------------------------- #
def _bin_label(lo: float, hi: float) -> str:
    if np.isinf(hi):
        return f">{int(lo)} m"
    if lo == 0:
        return f"0 – {int(hi)} m"
    return f"{int(lo)} – {int(hi)} m"


# --------------------------------------------------------------------- #
# main function
# --------------------------------------------------------------------- #
def distance_histogram(
    df: pd.DataFrame,
    cols: Sequence[str] = ("Dist_Strasse_vs_ortxy", "Dist_Georef_vs_ortxy"),
    *,
    bins: Sequence[float] = (0, 25, 50, 100, 250, 500, 1_000, np.inf),
    output: str | Path = "distance_histogram.png",
    show: bool = False,
) -> Path:
    """
    A grouped horizontal bar-chart of distance distributions.

    • Distances are bucketed into *bins* (metres).
    • One bar per bin per column.
    • Each bar is annotated with "count (pct%)".

    Parameters
    ----------
    df      : DataFrame that holds the distance columns.
    cols    : pair of column names to visualise.
    bins    : monotonically increasing edges (right-closed).
    output  : PNG file path.
    show    : if True pop up an interactive window.
    """
    # -------------------------------------------------------------- #
    # 1.  bucket both distance columns
    # -------------------------------------------------------------- #
    labels = [_bin_label(bins[i], bins[i + 1]) for i in range(len(bins) - 1)]

    agg = {}
    for col in cols:
        counts = (
            pd.cut(
                df[col].dropna(),
                bins=bins,
                right=False,
                labels=labels,
            )
            .value_counts()
            .reindex(labels, fill_value=0)
        )
        agg[col] = counts

    bar = pd.DataFrame(agg)

    # -------------------------------------------------------------- #
    # 2.  plot
    # -------------------------------------------------------------- #
    ax = bar.plot.barh(figsize=(10, 6), width=0.8)

    # annotate each segment with n + pct (per column basis)
    for col_idx, col in enumerate(bar.columns):
        total = bar[col].sum()
        for row_idx, n in enumerate(bar[col]):
            if n == 0:
                continue
            pct = 100 * n / total
            x_pos = n + bar.values.max() * 0.01
            ax.text(
                x_pos,
                row_idx + col_idx * 0.25 - 0.125,  # small offset so texts don't overlap
                f"{n:,} ({pct:.1f}%)",
                va="center",
                ha="left",
                fontsize=8,
            )

    ax.set_xlabel("Number of entries")
    ax.set_ylabel("Distance bucket")
    ax.set_title("Distance to reference point (ort_x / ort_y)")
    plt.tight_layout()

    out = Path(output)
    plt.savefig(out, dpi=150)
    if show:
        plt.show()
    plt.close()
    return out


def percentage_histogram(
    df: pd.DataFrame,
    cols: Sequence[str] = ("Pct_Strasse_Wohnviertel", "Pct_Strasse_PLZ"),
    *,
    bins: Sequence[float] = (0, 0.2, 0.4, 0.6, 0.8, 1.0),
    output: str | Path = "percentage_histogram.png",
    show: bool = False,
) -> Path:
    """
    Bar-chart of how much of each street lies in its 'dominant' polygon.
    """
    labels = [f"{int(low * 100)}–{int(high * 100)}%" for low, high in zip(bins[:-1], bins[1:], strict=False)]

    agg = {}
    for col in cols:
        counts = (
            pd.cut(
                df[col].dropna().clip(0, 1),
                bins=bins,
                labels=labels,
                right=False,
            )
            .value_counts()
            .reindex(labels, fill_value=0)
        )
        agg[col] = counts

    bar = pd.DataFrame(agg)

    ax = bar.plot.barh(figsize=(8, 5), width=0.8)

    for (row, col), n in np.ndenumerate(bar.values):
        if n == 0:
            continue
        total = bar.iloc[:, col].sum()
        pct = 100 * n / total
        ax.text(
            n + bar.values.max() * 0.01,
            row + col * 0.25 - 0.125,
            f"{n:,} ({pct:.1f}%)",
            va="center",
            ha="left",
            fontsize=8,
        )

    ax.set_xlabel("Number of streets")
    ax.set_ylabel("Overlap share (bucket)")
    ax.set_title("How much of each street lies in its assigned polygon")
    plt.tight_layout()

    out = Path(output)
    plt.savefig(out, dpi=150)
    if show:
        plt.show()
    plt.close()
    return out


def pct_histogram(
    df: pd.DataFrame,
    cols=("Pct_Strasse_Wohnviertel", "Pct_Strasse_PLZ"),
    output="pct_histogram.png",
    show=False,
):
    """
    Buckets 0–20–40–60–80–100 % and annotates bars with n (pct %).
    """
    import matplotlib.pyplot as plt
    import numpy as np

    bins = np.linspace(0, 1, 6)  # 0,0.2,…1
    labels = [f"{int(low * 100)}–{int(high * 100)}%" for low, high in zip(bins[:-1], bins[1:], strict=False)]

    counts = {
        c: pd.cut(df[c].dropna().clip(0, 1), bins, labels=labels, right=False)
        .value_counts()
        .reindex(labels, fill_value=0)
        for c in cols
    }

    bar = pd.DataFrame(counts)
    ax = bar.plot.barh(figsize=(8, 5), width=0.8)

    for (row, col), n in np.ndenumerate(bar.values):
        if n:
            pct = 100 * n / bar.iloc[:, col].sum()
            ax.text(
                n + bar.values.max() * 0.01,
                row + col * 0.25 - 0.125,
                f"{n:,} ({pct:.1f}%)",
                va="center",
                ha="left",
                fontsize=8,
            )

    ax.set_xlabel("Number of streets")
    ax.set_ylabel("Overlap share bucket")
    ax.set_title("Street-length fully inside dominant polygon")
    plt.tight_layout()
    plt.savefig(output, dpi=150)
    if show:
        plt.show()
        plt.close()
