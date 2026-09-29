from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


OUTPUT_DIR = Path(__file__).with_name("optimizer_benchmark_rev3_15min")
CURVES_FILE = OUTPUT_DIR / "optimizer_mse_over_time.csv"
SUMMARY_FILE = OUTPUT_DIR / "optimizer_summary.csv"
CHART_FILE = OUTPUT_DIR / "optimizer_mse_over_time.png"

curves = pd.read_csv(CURVES_FILE)
summary = pd.read_csv(SUMMARY_FILE).sort_values("best_mse")

palette = {
    "Optim.jl SAMIN": "#2F6B9A",
    "Optim.jl Particle Swarm": "#D49137",
    "BlackBoxOptim Adaptive DE": "#6F7F3F",
    "BlackBoxOptim DX-NES (threaded)": "#B45A75",
}
styles = {
    "Optim.jl SAMIN": "-",
    "Optim.jl Particle Swarm": "--",
    "BlackBoxOptim Adaptive DE": "-.",
    "BlackBoxOptim DX-NES (threaded)": ":",
}

fig, ax = plt.subplots(figsize=(11, 6.5), dpi=160)
for optimizer, data in curves.groupby("optimizer", sort=False):
    ordered = data.sort_values("elapsed_seconds")
    ax.step(
        ordered["elapsed_seconds"] / 60.0,
        ordered["best_mse"],
        where="post",
        label=optimizer,
        color=palette[optimizer],
        linestyle=styles[optimizer],
        linewidth=2.2,
    )

winner = summary.iloc[0]
ax.scatter(
    [winner["elapsed_seconds"] / 60.0],
    [winner["best_mse"]],
    color=palette[winner["optimizer"]],
    edgecolor="#252525",
    linewidth=0.8,
    s=55,
    zorder=5,
)
fig.suptitle(
    "Rev3 Global Optimizer Benchmark",
    x=0.105,
    y=0.98,
    ha="left",
    fontsize=16,
    weight="bold",
)
fig.text(
    0.105,
    0.935,
    "Best-so-far standardized moment MSE | 11,551 households | 15-minute budget per method",
    fontsize=9.5,
    color="#555555",
)
ax.set_xlabel("Elapsed time (minutes)")
ax.set_ylabel("Best standardized mean squared error (log scale)")
ax.set_yscale("log")
ax.grid(axis="both", color="#D9D9D9", linewidth=0.7, alpha=0.75)
ax.spines[["top", "right"]].set_visible(False)
ax.spines[["left", "bottom"]].set_color("#666666")
ax.legend(frameon=False, loc="best", fontsize=9)
fig.text(
    0.01,
    0.01,
    "Source: Fake_Merged_Data_with_cex_education_expenditure.csv. Stage-one Rev3 SMM objective; errors are standardized.",
    fontsize=8.5,
    color="#555555",
)
fig.tight_layout(rect=(0, 0.035, 1, 0.91))
fig.savefig(CHART_FILE, bbox_inches="tight", facecolor="white")
print(CHART_FILE)
