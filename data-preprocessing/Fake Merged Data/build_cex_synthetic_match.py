#!/usr/bin/env python3
"""Synthetic-match CEX household education spending onto child-period records.

The program follows the BLS CE statistical-matching design where it is
transportable to this application: separate annual income-quintile strata, a
survey-weighted expenditure regression, and random hot-deck draws from the 20
donors with the closest predicted expenditure. See CEX_SYNTHETIC_MATCH_METHODS.md.
"""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT / "outputs" / "Fake_Merged_Data_with_adjusted_headstart_expenditure.csv"
DEFAULT_RAW = ROOT / "data" / "cex" / "raw"
DEFAULT_OUTPUT = ROOT / "outputs" / "Fake_Merged_Data_with_cex_education_expenditure.csv"
DEFAULT_MI_OUTPUT = ROOT / "outputs" / "cex_education_multiple_imputations.csv"
DEFAULT_DONOR_OUTPUT = ROOT / "outputs" / "cex_annual_education_donors_1988_2010.csv"
DEFAULT_QA_OUTPUT = ROOT / "outputs" / "cex_synthetic_match_qa.csv"

YEARS = range(1988, 2011)
AGE_RANGES = {0: (0, 5), 1: (6, 9), 2: (10, 14), 3: (15, 19)}
UCC_COMPONENTS = {
    "660210": "school_supplies",
    "660901": "preschool_supplies",
    "670210": "k12_tuition",
    "670310": "preschool_daycare",
    "670903": "tutoring_test_prep",
}
COMPONENTS = [
    "k12_tuition",
    "school_supplies",
    "preschool_daycare",
    "preschool_supplies",
    "tutoring_test_prep",
]
TOTAL = "household_education_total"
MATCH_FEATURES = [
    "region",
    "urban",
    "adult_age",
    "education_level",
    "race_group",
    "number_children",
    "has_child_0_5",
    "has_child_6_9",
    "has_child_10_14",
    "has_child_15_19",
    "log_income",
    "negative_income",
]


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def first_existing(frame: pd.DataFrame, names: list[str], default=np.nan) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return frame[name]
    return pd.Series(default, index=frame.index)


def mode_or_nan(series: pd.Series):
    values = series.dropna()
    if values.empty:
        return np.nan
    return values.mode().iloc[0]


def quarter_code(name: str, stem: str) -> str | None:
    match = re.match(rf"{stem}(\d{{3}})x?\.sas7bdat$", PurePosixPath(name).name.lower())
    return match.group(1) if match else None


def archive_path(raw_dir: Path, year: int) -> Path:
    path = raw_dir / f"intrvw{year % 100:02d}.zip"
    if not path.exists():
        raise FileNotFoundError(f"Missing CEX archive: {path}")
    return path


def locate_quarter_files(raw_dir: Path, year: int, stem: str) -> list[tuple[Path, str]]:
    yy = year % 100
    next_yy = (year + 1) % 100
    wanted = {f"{yy:02d}2", f"{yy:02d}3", f"{yy:02d}4", f"{next_yy:02d}1"}
    found: dict[str, tuple[Path, str]] = {}
    candidates = [archive_path(raw_dir, year)]
    next_path = raw_dir / f"intrvw{(year + 1) % 100:02d}.zip"
    if next_path.exists():
        candidates.append(next_path)
    for zip_path in candidates:
        with zipfile.ZipFile(zip_path) as archive:
            for name in archive.namelist():
                code = quarter_code(name, stem)
                if code in wanted and code not in found:
                    found[code] = (zip_path, name)
    missing = sorted(wanted - set(found))
    if missing:
        raise FileNotFoundError(f"Missing {stem.upper()} calendar-year quarters for {year}: {missing}")
    return [found[code] for code in sorted(wanted)]


def read_sas_member(zip_path: Path, member: str) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive, tempfile.TemporaryDirectory() as tmp:
        extracted = archive.extract(member, tmp)
        return pd.read_sas(extracted, format="sas7bdat", encoding="latin1")


def education_level_from_cex(values: pd.Series) -> pd.Series:
    codes = numeric(values)
    mapping = {
        0: 0,
        # Pre-1996 education codes.
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 6,
        6: 7,
        7: 0,
        # 1996-forward education codes.
        10: 1,
        11: 2,
        12: 3,
        13: 4,
        14: 5,
        15: 6,
        16: 7,
        17: 8,
    }
    return codes.map(mapping)


def education_level_from_years(values: pd.Series) -> pd.Series:
    years = numeric(values)
    conditions = [
        years <= 0,
        years.between(1, 8),
        years.between(9, 11),
        years == 12,
        years.between(13, 13.99),
        years.between(14, 15.99),
        years.between(16, 16.99),
        years.between(17, 18.99),
        years >= 19,
    ]
    return pd.Series(np.select(conditions, range(9), default=np.nan), index=values.index)


def cex_race_group(frame: pd.DataFrame) -> pd.Series:
    race = numeric(first_existing(frame, ["REF_RACE"]))
    hispanic = pd.Series(False, index=frame.index)
    if "HISP_REF" in frame:
        hispanic |= numeric(frame["HISP_REF"]).eq(1)
    if "HORREF1" in frame:
        hispanic |= frame["HORREF1"].astype(str).str.strip().replace("nan", "").ne("")
    if "ORIGIN1" in frame:
        # Through 2002, ORIGIN1 code 2 denotes Spanish origin/ancestry.
        hispanic |= numeric(frame["ORIGIN1"]).eq(2)
    return pd.Series(np.where(hispanic, 1, np.where(race.eq(2), 2, 3)), index=frame.index)


def load_cpi() -> tuple[dict[int, float], float, str]:
    files = [
        ROOT / "sources" / "bls_cpi_1988_1997.json",
        ROOT / "sources" / "bls_cpi_1998_2007.json",
        ROOT / "sources" / "bls_cpi_2008_2017.json",
        ROOT / "sources" / "bls_cpi_2018_2026.json",
    ]
    monthly: dict[int, dict[str, float]] = {}
    for path in files:
        data = json.loads(path.read_text())
        for item in data["Results"]["series"][0]["data"]:
            if item["value"] == "-" or not re.fullmatch(r"M\d{2}", item["period"]):
                continue
            monthly.setdefault(int(item["year"]), {})[item["period"]] = float(item["value"])
    annual = {year: float(np.mean(list(values.values()))) for year, values in monthly.items() if len(values) == 12}
    latest_period = max(monthly[2026])
    return annual, monthly[2026][latest_period], latest_period


def selected_quarter_data(raw_dir: Path, year: int, stem: str) -> dict[str, pd.DataFrame]:
    result = {}
    for zip_path, member in locate_quarter_files(raw_dir, year, stem):
        code = quarter_code(member, stem)
        assert code is not None
        result[code] = read_sas_member(zip_path, member)
    return result


def prepare_cex_year(raw_dir: Path, year: int, cpi: dict[int, float], reference_cpi: float) -> pd.DataFrame:
    fmli_by_q = selected_quarter_data(raw_dir, year, "fmli")
    memi_by_q = selected_quarter_data(raw_dir, year, "memi") if year >= 1990 else {}
    mtbi_by_q = selected_quarter_data(raw_dir, year, "mtbi") if year >= 1990 else {}
    donor_quarters = []

    for code, full_fmli in fmli_by_q.items():
        fmli = pd.DataFrame(index=full_fmli.index)
        fmli["NEWID"] = numeric(full_fmli["NEWID"])
        fmli["region"] = numeric(first_existing(full_fmli, ["REGION"]))
        fmli["urban"] = numeric(first_existing(full_fmli, ["BLS_URBN"])).eq(1).astype(float)
        fmli["adult_age"] = numeric(first_existing(full_fmli, ["AGE_REF"]))
        fmli["education_level"] = education_level_from_cex(first_existing(full_fmli, ["EDUC_REF"]))
        fmli["race_group"] = cex_race_group(full_fmli)
        fmli["family_size"] = numeric(first_existing(full_fmli, ["FAM_SIZE"]))
        fmli["income"] = numeric(first_existing(full_fmli, ["FINCBTXM", "FINCBTAX"]))
        fmli["weight"] = numeric(first_existing(full_fmli, ["FINLWT21"])).clip(lower=0)
        if "TOTEXPPQ" in full_fmli and "TOTEXPCQ" in full_fmli:
            quarterly_total = numeric(full_fmli["TOTEXPPQ"]).fillna(0) + numeric(
                full_fmli["TOTEXPCQ"]
            ).fillna(0)
        else:
            # The 1988-1989 FMLI layout uses a single quarterly summary field.
            quarterly_total = numeric(first_existing(full_fmli, ["ZTOTAL", "ZTOTALX4"]))
        fmli["annual_total_expenditure"] = 4 * quarterly_total

        if year < 1990:
            fmli["number_children"] = numeric(first_existing(full_fmli, ["PERSLT18"]))
            for component in COMPONENTS:
                fmli[component] = np.nan
            if "ZEDUCATN" in full_fmli:
                quarterly_education = numeric(full_fmli["ZEDUCATN"])
            else:
                quarterly_education = numeric(first_existing(full_fmli, ["EDUCAPQ"])).fillna(0) + numeric(
                    first_existing(full_fmli, ["EDUCACQ"])
                ).fillna(0)
            fmli[TOTAL] = 4 * quarterly_education
            fmli["early_aggregate_measure"] = 1
        else:
            memi = memi_by_q[code].copy()
            memi["NEWID"] = numeric(memi["NEWID"])
            memi["AGE_NUM"] = numeric(memi["AGE"])
            member_counts = memi.groupby("NEWID", as_index=False).agg(
                children_0_5=("AGE_NUM", lambda x: int(x.between(0, 5).sum())),
                children_6_9=("AGE_NUM", lambda x: int(x.between(6, 9).sum())),
                children_10_14=("AGE_NUM", lambda x: int(x.between(10, 14).sum())),
                children_15_19=("AGE_NUM", lambda x: int(x.between(15, 19).sum())),
            )
            member_counts["number_children"] = member_counts[
                ["children_0_5", "children_6_9", "children_10_14", "children_15_19"]
            ].sum(axis=1)
            for age_band in ["0_5", "6_9", "10_14", "15_19"]:
                member_counts[f"has_child_{age_band}"] = member_counts[f"children_{age_band}"].gt(0).astype(float)
            fmli = fmli.merge(member_counts, on="NEWID", how="left")

            mtbi = mtbi_by_q[code].copy()
            mtbi["NEWID"] = numeric(mtbi["NEWID"])
            mtbi["REF_YEAR_NUM"] = numeric(mtbi["REF_YR"])
            mtbi.loc[mtbi["REF_YEAR_NUM"].between(0, 99), "REF_YEAR_NUM"] += 1900
            mtbi["UCC_CODE"] = mtbi["UCC"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
            mtbi["COST_NUM"] = numeric(mtbi["COST"])
            mtbi = mtbi[mtbi["REF_YEAR_NUM"].eq(year)]
            education = mtbi[mtbi["UCC_CODE"].isin(UCC_COMPONENTS)].copy()
            education["component"] = education["UCC_CODE"].map(UCC_COMPONENTS)
            component_wide = (
                education.groupby(["NEWID", "component"])["COST_NUM"]
                .sum()
                .unstack(fill_value=0)
                .reset_index()
            )
            fmli = fmli.merge(component_wide, on="NEWID", how="left")
            for component in COMPONENTS:
                if component not in fmli:
                    fmli[component] = 0.0
                fmli[component] = numeric(fmli[component]).fillna(0) * 4
            fmli[TOTAL] = fmli[COMPONENTS].sum(axis=1)
            fmli["early_aggregate_measure"] = 0

        for name in ["children_0_5", "children_6_9", "children_10_14", "children_15_19"]:
            if name not in fmli:
                fmli[name] = np.nan
        for name in ["has_child_0_5", "has_child_6_9", "has_child_10_14", "has_child_15_19"]:
            if name not in fmli:
                fmli[name] = np.nan
        fmli["calendar_quarter"] = code
        donor_quarters.append(fmli)

    donors = pd.concat(donor_quarters, ignore_index=True)
    donors = donors[
        donors["NEWID"].notna()
        & donors["weight"].gt(0)
        & donors["number_children"].fillna(0).gt(0)
        & donors["annual_total_expenditure"].gt(0)
    ].copy()
    donors["year"] = year
    donors["log_income"] = np.log1p(donors["income"].clip(lower=0))
    donors["negative_income"] = donors["income"].lt(0).astype(float)
    inflation_factor = reference_cpi / cpi[year]
    for outcome in COMPONENTS + [TOTAL]:
        donors[outcome] = donors[outcome] * inflation_factor
    donors["annual_total_expenditure"] *= inflation_factor
    return donors.reset_index(drop=True)


def fips_region(values: pd.Series) -> pd.Series:
    state = numeric(values).floordiv(1000)
    northeast = {9, 23, 25, 33, 34, 36, 42, 44, 50}
    midwest = {17, 18, 19, 20, 26, 27, 29, 31, 38, 39, 46, 55}
    # Puerto Rico (72) is assigned to South solely for broad-region matching.
    south = {1, 5, 10, 11, 12, 13, 21, 22, 24, 28, 37, 40, 45, 47, 48, 51, 54, 72}
    west = {2, 4, 6, 8, 15, 16, 30, 32, 35, 41, 49, 53, 56}
    return pd.Series(
        np.select(
            [state.isin(northeast), state.isin(midwest), state.isin(south), state.isin(west)],
            [1, 2, 3, 4],
            default=np.nan,
        ),
        index=values.index,
    )


def expand_target_to_household_years(target: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    target = target.copy()
    # These maternal characteristics are sparsely repeated across child-period rows.
    # Carrying observed values within mother prevents artificial period-specific gaps.
    for name in ["MOTHER_BIRTH_YEAR", "MOM_RACE", "FIPS", "URBAN_RURAL", "HGC_REV_MOM"]:
        filled = target.groupby("MPUBID_XRND")[name].transform("median")
        target[name] = target[name].fillna(filled)
    pieces = []
    for period, (age_start, age_end) in AGE_RANGES.items():
        rows = target[target["period"].eq(period)].copy()
        for age in range(age_start, age_end + 1):
            expanded = rows.copy()
            expanded["year"] = numeric(expanded["CYRB_XRND"]) + age
            expanded["child_age"] = age
            pieces.append(expanded)
    period_years = pd.concat(pieces, ignore_index=True)
    period_years = period_years[period_years["year"].between(min(YEARS), max(YEARS))].copy()
    period_years["year"] = period_years["year"].astype(int)

    mother_birth = numeric(period_years["MOTHER_BIRTH_YEAR"])
    mother_birth = np.where(mother_birth.between(50, 99), mother_birth + 1900, mother_birth)
    period_years["adult_age_derived"] = period_years["year"] - mother_birth
    period_years.loc[~period_years["adult_age_derived"].between(15, 80), "adult_age_derived"] = np.nan
    period_years["region_derived"] = fips_region(period_years["FIPS"])
    period_years["urban_derived"] = numeric(period_years["URBAN_RURAL"])
    period_years["education_derived"] = education_level_from_years(period_years["HGC_REV_MOM"])
    period_years["income_derived"] = numeric(period_years["TNFI_TRUNC"])
    period_years["race_derived"] = numeric(period_years["MOM_RACE"])

    demographic = period_years.groupby(["MPUBID_XRND", "year"], as_index=False).agg(
        adult_age=("adult_age_derived", "median"),
        region=("region_derived", mode_or_nan),
        urban=("urban_derived", "median"),
        education_level=("education_derived", "median"),
        income=("income_derived", "median"),
        race_group=("race_derived", mode_or_nan),
        reported_number_children=("NUM_CHILDREN", "median"),
    )

    unique_children = period_years.drop_duplicates(["MPUBID_XRND", "year", "id"])
    counts = unique_children.groupby(["MPUBID_XRND", "year"], as_index=False).agg(
        children_0_5=("child_age", lambda x: int(pd.Series(x).between(0, 5).sum())),
        children_6_9=("child_age", lambda x: int(pd.Series(x).between(6, 9).sum())),
        children_10_14=("child_age", lambda x: int(pd.Series(x).between(10, 14).sum())),
        children_15_19=("child_age", lambda x: int(pd.Series(x).between(15, 19).sum())),
        observed_children=("id", "nunique"),
    )
    households = demographic.merge(counts, on=["MPUBID_XRND", "year"], how="left")
    households["number_children"] = households["observed_children"]
    households["family_size"] = households["number_children"] + 1
    households["urban"] = households["urban"].ge(0.5).where(households["urban"].notna()).astype(float)
    for age_band in ["0_5", "6_9", "10_14", "15_19"]:
        households[f"has_child_{age_band}"] = households[f"children_{age_band}"].gt(0).astype(float)
    households["log_income"] = np.log1p(households["income"].clip(lower=0))
    households["negative_income"] = households["income"].lt(0).astype(float)
    households["recipient_id"] = np.arange(len(households), dtype=int)

    period_map = period_years[["_row_id", "id", "period", "MPUBID_XRND", "year"]].drop_duplicates()
    period_map = period_map.merge(
        households[["MPUBID_XRND", "year", "recipient_id"]], on=["MPUBID_XRND", "year"], how="left"
    )
    return households, period_map


def weighted_quintiles(values: pd.Series, weights: pd.Series) -> pd.Series:
    valid = values.notna() & weights.gt(0)
    result = pd.Series(np.nan, index=values.index)
    if not valid.any():
        return result
    order = np.argsort(values[valid].to_numpy(), kind="mergesort")
    valid_index = values[valid].index.to_numpy()[order]
    sorted_weights = weights.loc[valid_index].to_numpy(float)
    cumulative = (np.cumsum(sorted_weights) - 0.5 * sorted_weights) / sorted_weights.sum()
    result.loc[valid_index] = np.minimum(5, np.floor(cumulative * 5) + 1)
    return result


def unweighted_quintiles(values: pd.Series) -> pd.Series:
    ranked = values.rank(method="average", pct=True)
    return np.minimum(5, np.floor((ranked.fillna(0) - 1e-12) * 5) + 1).where(values.notna())


def design_matrix(donors: pd.DataFrame, recipients: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    combined = pd.concat(
        [donors[MATCH_FEATURES].assign(_sample="donor"), recipients[MATCH_FEATURES].assign(_sample="recipient")],
        ignore_index=True,
    )
    numeric_features = [
        "adult_age",
        "education_level",
        "number_children",
        "has_child_0_5",
        "has_child_6_9",
        "has_child_10_14",
        "has_child_15_19",
        "log_income",
        "negative_income",
    ]
    columns = []
    for name in numeric_features:
        values = numeric(combined[name])
        donor_values = values.iloc[: len(donors)]
        if not donor_values.notna().any():
            # A donor regression cannot estimate a feature unavailable in that year.
            continue
        median = donor_values.median()
        filled = values.fillna(median)
        scale = filled.iloc[: len(donors)].std()
        if not np.isfinite(scale) or scale == 0:
            scale = 1.0
        columns.append(((filled - median) / scale).rename(name))
        if values.isna().any():
            columns.append(values.isna().astype(float).rename(f"{name}_missing"))
    for name in ["region", "urban", "race_group"]:
        category = combined[name].fillna("missing").astype(str)
        dummies = pd.get_dummies(category, prefix=name, dtype=float)
        columns.extend([dummies[col] for col in dummies.columns[1:]])
    matrix = pd.concat(columns, axis=1).to_numpy(float)
    matrix = np.column_stack([np.ones(len(matrix)), matrix])
    return matrix[: len(donors)], matrix[len(donors) :]


def predict_expenditure(donors: pd.DataFrame, recipients: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    donor_x, recipient_x = design_matrix(donors, recipients)
    y = np.log1p(donors["annual_total_expenditure"].clip(lower=0).to_numpy(float))
    weights = donors["weight"].to_numpy(float)
    root_weight = np.sqrt(np.clip(weights, 0, np.inf))
    beta = np.linalg.lstsq(donor_x * root_weight[:, None], y * root_weight, rcond=None)[0]
    return donor_x @ beta, recipient_x @ beta


def match_year(
    donors: pd.DataFrame,
    recipients: pd.DataFrame,
    imputations: int,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, dict]:
    donors = donors.copy()
    recipients = recipients.copy()
    rural_region_observed = donors.loc[donors["urban"].eq(0), "region"].notna().mean()
    region_suppression_applied = bool(pd.notna(rural_region_observed) and rural_region_observed < 0.05)
    if region_suppression_applied:
        # Mirror legacy CEX disclosure suppression so recipient combinations
        # remain inside the donor data's observed support.
        recipients.loc[recipients["urban"].eq(0), "region"] = np.nan
    donors["income_quintile"] = weighted_quintiles(donors["income"], donors["weight"])
    recipients["income_quintile"] = unweighted_quintiles(recipients["income"])
    missing_income = recipients["income_quintile"].isna()
    recipients.loc[missing_income, "income_quintile"] = 3
    donors["match_score"], recipients["match_score"] = predict_expenditure(donors, recipients)

    records = []
    distances = []
    pool_sizes = []
    outcomes = COMPONENTS + [TOTAL]
    for quintile in range(1, 6):
        donor_subset = donors[donors["income_quintile"].eq(quintile)].sort_values("match_score")
        recipient_subset = recipients[recipients["income_quintile"].eq(quintile)]
        if donor_subset.empty:
            donor_subset = donors.sort_values("match_score")
        scores = donor_subset["match_score"].to_numpy(float)
        donor_indices = donor_subset.index.to_numpy()
        for recipient_index, score in recipient_subset["match_score"].items():
            insertion = int(np.searchsorted(scores, score))
            lo = max(0, insertion - 30)
            hi = min(len(scores), insertion + 30)
            local = np.arange(lo, hi)
            nearest_local = local[np.argsort(np.abs(scores[local] - score))[: min(20, len(local))]]
            nearest = donor_indices[nearest_local]
            chosen = rng.choice(nearest, size=imputations, replace=True)
            chosen_rows = donors.loc[chosen]
            for imp, (_, donor) in enumerate(chosen_rows.iterrows(), start=1):
                row = {
                    "recipient_id": int(recipients.loc[recipient_index, "recipient_id"]),
                    "imputation": imp,
                    "donor_NEWID": int(donor["NEWID"]),
                    "match_distance": abs(float(donor["match_score"]) - float(score)),
                }
                row.update({outcome: donor[outcome] for outcome in outcomes})
                records.append(row)
            distances.append(float(np.mean(np.abs(chosen_rows["match_score"].to_numpy() - score))))
            pool_sizes.append(len(nearest))
    matched = pd.DataFrame(records)
    qa = {
        "year": int(donors["year"].iloc[0]),
        "donors": len(donors),
        "recipients": len(recipients),
        "mean_match_distance": float(np.mean(distances)),
        "p95_match_distance": float(np.quantile(distances, 0.95)),
        "minimum_nearest_pool": int(np.min(pool_sizes)),
        "early_aggregate_measure": int(donors["early_aggregate_measure"].max()),
        "rural_region_suppression_applied": int(region_suppression_applied),
    }
    return matched, qa


def cumulative_imputations(
    target: pd.DataFrame,
    period_map: pd.DataFrame,
    annual_matches: pd.DataFrame,
    imputations: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    outcomes = COMPONENTS + [TOTAL]
    long_parts = []
    row_keys = target[["_row_id", "id", "period"]]
    for imp in range(1, imputations + 1):
        annual = annual_matches[annual_matches["imputation"].eq(imp)][["recipient_id"] + outcomes]
        merged = period_map.merge(annual, on="recipient_id", how="left")
        cumulative = merged.groupby(["_row_id", "id", "period"], as_index=False)[outcomes].sum(min_count=1)
        cumulative = row_keys.merge(cumulative, on=["_row_id", "id", "period"], how="left")
        cumulative["imputation"] = imp
        long_parts.append(cumulative)
    long = pd.concat(long_parts, ignore_index=True)

    means = long.groupby("_row_id")[outcomes].mean()
    total_sd = long.groupby("_row_id")[TOTAL].std()
    matched_years = period_map.merge(
        annual_matches.groupby("recipient_id", as_index=False)[TOTAL].count().rename(columns={TOTAL: "draw_count"}),
        on="recipient_id",
        how="left",
    )
    matched_years["year_matched"] = matched_years["draw_count"].fillna(0).gt(0)
    coverage = matched_years.groupby("_row_id").agg(
        cex_years_in_range=("year", "nunique"),
        cex_years_matched=("year_matched", "sum"),
    )

    result = target.copy().set_index("_row_id")
    output_names = {
        "k12_tuition": "CEX Cumulative K-12 Tuition (Real 2026 Dollars)",
        "school_supplies": "CEX Cumulative K-12 School Supplies (Real 2026 Dollars)",
        "preschool_daycare": "CEX Cumulative Preschool and Day Care (Real 2026 Dollars)",
        "preschool_supplies": "CEX Cumulative Preschool Supplies (Real 2026 Dollars)",
        "tutoring_test_prep": "CEX Cumulative Tutoring and Test Prep (Real 2026 Dollars)",
        TOTAL: "CEX Cumulative Household Education Expenditure (Real 2026 Dollars)",
    }
    for source, output in output_names.items():
        result[output] = means[source].round(2)
    result["CEX Cumulative Total Imputation SD (Real 2026 Dollars)"] = total_sd.round(2)
    expected = result["period"].map({period: end - start + 1 for period, (start, end) in AGE_RANGES.items()})
    result["CEX Period Years Expected"] = expected
    result["CEX Years Within 1988-2010"] = coverage["cex_years_in_range"].reindex(result.index).fillna(0).astype(int)
    result["CEX Years Successfully Matched"] = coverage["cex_years_matched"].reindex(result.index).fillna(0).astype(int)
    result["CEX Coverage Share"] = (result["CEX Years Successfully Matched"] / expected).round(4)
    result["CEX Period Fully Covered"] = result["CEX Years Successfully Matched"].eq(expected).astype(int)

    long = long.rename(columns={source: output for source, output in output_names.items()})
    return result.reset_index(drop=True), long


def run(args: argparse.Namespace) -> None:
    target = pd.read_csv(args.input)
    required = {
        "id", "period", "MPUBID_XRND", "CYRB_XRND", "MOTHER_BIRTH_YEAR", "FIPS",
        "URBAN_RURAL", "HGC_REV_MOM", "TNFI_TRUNC", "MOM_RACE", "NUM_CHILDREN",
    }
    missing = required - set(target.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {sorted(missing)}")
    if target.duplicated(["id", "period"]).any():
        raise ValueError("Input must have one row per id-period")
    target["_row_id"] = np.arange(len(target), dtype=int)
    households, period_map = expand_target_to_household_years(target)
    rng = np.random.default_rng(args.seed)

    if args.donor_input is not None:
        donor_panel = pd.read_csv(args.donor_input)
        required_donor_columns = {
            "NEWID", "year", "region", "urban", "adult_age", "education_level",
            "race_group", "income", "weight", "annual_total_expenditure",
            "number_children", "has_child_0_5", "has_child_6_9",
            "has_child_10_14", "has_child_15_19", "log_income",
            "negative_income", "early_aggregate_measure", TOTAL, *COMPONENTS,
        }
        missing_donor_columns = required_donor_columns - set(donor_panel.columns)
        if missing_donor_columns:
            raise ValueError(
                f"Donor file is missing required columns: {sorted(missing_donor_columns)}"
            )
        donor_panel["year"] = numeric(donor_panel["year"])
        available_years = sorted(donor_panel["year"].dropna().astype(int).unique())
        if available_years != list(YEARS):
            raise ValueError(
                "Donor file must contain every year from 1988 through 2010; "
                f"found {available_years}"
            )
        print(f"Loaded {len(donor_panel):,} prepared CEX donor observations from {args.donor_input}")
    else:
        cpi, reference_cpi, _ = load_cpi()
        donor_parts = []
        for year in YEARS:
            print(f"Preparing CEX {year}...")
            donor_parts.append(prepare_cex_year(args.raw_dir, year, cpi, reference_cpi))
        donor_panel = pd.concat(donor_parts, ignore_index=True)

    annual_match_parts = []
    qa_rows = []
    for year in YEARS:
        print(f"Matching CEX {year}...")
        donors = donor_panel[donor_panel["year"].eq(year)].copy().reset_index(drop=True)
        recipients = households[households["year"].eq(year)].copy()
        if recipients.empty:
            continue
        matches, qa = match_year(donors, recipients, args.imputations, rng)
        matches["year"] = year
        annual_match_parts.append(matches)
        qa_rows.append(qa)

    if not annual_match_parts:
        raise ValueError("No NLSY household-years overlap the donor years 1988-2010")
    annual_matches = pd.concat(annual_match_parts, ignore_index=True)
    output, cumulative_long = cumulative_imputations(target, period_map, annual_matches, args.imputations)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.mi_output.parent.mkdir(parents=True, exist_ok=True)
    args.qa_output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False)
    cumulative_long.to_csv(args.mi_output, index=False)
    if args.donor_input is None:
        args.donor_output.parent.mkdir(parents=True, exist_ok=True)
        donor_panel.to_csv(args.donor_output, index=False)
    pd.DataFrame(qa_rows).to_csv(args.qa_output, index=False)

    assert len(output) == len(target)
    assert not output.duplicated(["id", "period"]).any()
    print(f"Wrote {args.output}")
    print(f"Wrote {args.mi_output}")
    if args.donor_input is None:
        print(f"Wrote {args.donor_output}")
    print(f"Wrote {args.qa_output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument(
        "--donor-input",
        type=Path,
        help=(
            "Prepared CEX donor CSV. When supplied, raw CEX archives and CPI files "
            "are not read, making this the portable VDE mode."
        ),
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--mi-output", type=Path, default=DEFAULT_MI_OUTPUT)
    parser.add_argument("--donor-output", type=Path, default=DEFAULT_DONOR_OUTPUT)
    parser.add_argument("--qa-output", type=Path, default=DEFAULT_QA_OUTPUT)
    parser.add_argument("--imputations", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20260721)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
