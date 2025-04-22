import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm


def remove_extreme_outliers(data, percentile=0.05):
    lower_bound = np.percentile(data, percentile)
    upper_bound = np.percentile(data, 100 - percentile)
    filtered_data = data[(data >= lower_bound) & (data <= upper_bound)]
    return filtered_data


def create_histogram_plot(
    curr_dir,
    id_standort,
    street_data,
    phase_order,
    phase_colors,
    street_name,
    zyklus,
    geschw,
):
    plt.figure(figsize=(12, 6))
    plt.title(f"Zyklus: {zyklus} - {street_name} Histogram ({geschw}km/h)")
    base_bins = np.arange(np.max([geschw - 50, 0]), geschw + 50, 10)
    plt.axvline(x=geschw - 5, color="black", linewidth=2)  # Bold line at 'geschw'

    bar_width = 10 / (len(phase_order) * 2 + 1)  # Adjust bar width
    initial_offset = -bar_width * len(phase_order)
    offset = initial_offset

    # Plot histograms for each phase for V_Einfahrt and V_Ausfahrt
    for phase in phase_order:
        # V_Einfahrt
        phase_data_einfahrt = street_data[street_data["Phase"] == phase]["V_Einfahrt"]
        counts_einfahrt, _ = np.histogram(phase_data_einfahrt, bins=base_bins)
        if counts_einfahrt.sum() == 0:
            continue
        counts_einfahrt = counts_einfahrt / counts_einfahrt.sum()  # Normalize
        plt.bar(
            base_bins[:-1] + offset,
            counts_einfahrt,
            width=bar_width,
            label=f"{phase} (Einfahrt)",
            alpha=0.5,
            color=phase_colors[phase],
            edgecolor="black",
            align="edge",
        )
        offset += bar_width

        # V_Ausfahrt
        phase_data_ausfahrt = street_data[street_data["Phase"] == phase]["V_Ausfahrt"]
        counts_ausfahrt, _ = np.histogram(phase_data_ausfahrt, bins=base_bins)
        counts_ausfahrt = counts_ausfahrt / counts_ausfahrt.sum()  # Normalize
        plt.bar(
            base_bins[:-1] + offset,
            counts_ausfahrt,
            width=bar_width,
            label=f"{phase} (Ausfahrt)",
            alpha=0.5,
            color=phase_colors[phase],
            edgecolor="black",
            align="edge",
            hatch="//",
        )
        offset += bar_width

    bin_centers = np.arange(np.max([geschw - 50, 0]), geschw + 30, 10)
    bin_labels = [f"{int(x)} - {int(x + 10)} km/h" for x in bin_centers]
    plt.xticks(bin_centers, bin_labels, rotation=45, ha="right")

    plt.ylabel("Prozentuale Verteilung")
    plt.xlabel("Geschwindigkeit")
    plt.grid(
        True,
        which="both",
        axis="y",
        linestyle="-",
        linewidth=0.5,
        color="grey",
        alpha=0.7,
    )
    y_ticks = plt.gca().get_yticks()
    plt.gca().set_yticks(y_ticks)
    plt.gca().set_yticklabels(["{:.0f}%".format(y * 100) for y in y_ticks])
    plt.gca().xaxis.grid(False)  # Turn off x-axis grid
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.20)
    plt.legend()
    plt.savefig(
        os.path.join(
            curr_dir,
            "plots\\hist_Zyk"
            + zyklus
            + "_ID"
            + str(id_standort)
            + "_"
            + str(street_name)
            + ".png",
        )
    )
    plt.close()


def create_box_violin_plot(
    curr_dir,
    id_standort,
    street_data,
    phase_order,
    einfahrt_color,
    ausfahrt_color,
    street_name,
    zyklus,
    geschw,
):
    plt.figure(figsize=(15, 6))
    plt.title(
        f"Zyklus: {zyklus} - {street_name} Boxplot and Violin Plot ({geschw}km/h)"
    )
    box_data = []
    colors = []
    for phase in phase_order:
        # Einfahrt
        einfahrt_data = street_data[street_data["Phase"] == phase]["V_Einfahrt"]
        if not einfahrt_data.empty:
            box_data.append(einfahrt_data)
            colors.append(einfahrt_color)

        # Ausfahrt
        ausfahrt_data = street_data[street_data["Phase"] == phase]["V_Ausfahrt"]
        if not ausfahrt_data.empty:
            box_data.append(ausfahrt_data)
            colors.append(ausfahrt_color)

    for i in range(len(box_data)):
        box_data[i] = remove_extreme_outliers(box_data[i])

    positions = np.arange(1, len(box_data) + 1)

    # Boxplot
    box = plt.boxplot(
        box_data, positions=positions - 0.2, widths=0.25, patch_artist=True
    )
    # Set box colors
    for patch, color in zip(box["boxes"], colors):
        patch.set_facecolor(color)

    # Violinplots
    violin = plt.violinplot(
        box_data, positions=positions + 0.2, showmeans=True, showmedians=False
    )
    # Set violin plot colors
    for pc, color in zip(violin["bodies"], colors):
        pc.set_facecolor(color)
        pc.set_edgecolor("black")
        pc.set_alpha(0.5)

    # Bold line at 'geschw' on y-axis
    plt.axhline(y=geschw, color="black", linewidth=2)  # Bold line at 'geschw'
    max_value = max([np.max(data) for data in box_data if not data.empty])
    for line in range(0, max_value, 10):
        plt.axhline(y=line, color="grey", linewidth=0.5, alpha=0.5)

    # Set tick labels
    tick_positions = np.arange(1, len(box_data) + 1, step=2)
    plt.xticks(tick_positions + 0.5, phase_order[: len(tick_positions)])
    plt.xlabel("Phasen")
    plt.ylabel("Geschwindigkeit (km/h)")
    plt.grid(True, axis="y", linestyle="-", linewidth=0.5, color="grey", alpha=0.7)
    plt.gca().set_ylim(bottom=0)

    # Legend
    plt.legend(
        [box["boxes"][0], box["boxes"][1]], ["Einfahrt", "Ausfahrt"], loc="upper right"
    )

    plt.tight_layout()
    plt.savefig(
        os.path.join(
            curr_dir,
            "plots\\box_violin_Zyk"
            + zyklus
            + "_ID"
            + str(id_standort)
            + "_"
            + str(street_name)
            + ".png",
        )
    )
    plt.close()


def main():
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    dataset = pd.read_csv(os.path.join(curr_dir, "data\\all_data.csv"))
    streets = dataset[
        ["Zyklus", "Strassenname", "id_standort", "Geschwindigkeit"]
    ].drop_duplicates()
    phase_order = ["Vormessung", "Betrieb", "Nachmessung"]
    phase_colors = {"Vormessung": "blue", "Betrieb": "green", "Nachmessung": "red"}
    einfahrt_color = "royalblue"
    ausfahrt_color = "mediumseagreen"

    # Iterate over all streets
    for i in tqdm(range(len(streets)), desc="Processing streets"):
        zyklus = str(int(streets.iloc[i]["Zyklus"]))
        street_name = streets.iloc[i]["Strassenname"]
        id_standort = streets.iloc[i]["id_standort"]
        geschw = int(streets.iloc[i]["Geschwindigkeit"])
        street_data = dataset[
            (dataset["Strassenname"] == street_name)
            & (dataset["Phase"].isin(phase_order))
        ]

        create_histogram_plot(
            curr_dir,
            id_standort,
            street_data,
            phase_order,
            phase_colors,
            street_name,
            zyklus,
            geschw,
        )
        create_box_violin_plot(
            curr_dir,
            id_standort,
            street_data,
            phase_order,
            einfahrt_color,
            ausfahrt_color,
            street_name,
            zyklus,
            geschw,
        )


if __name__ == "__main__":
    main()
