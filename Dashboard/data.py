import pandas as pd

MONTH_ORDER = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
               "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

FUEL_GROUPS = {
    "PURE EV": "EV",
    "STRONG HYBRID EV": "EV",
    "DIESEL/HYBRID": "EV",
    "PETROL(E20)/HYBRID": "EV",
    "PETROL/CNG": "CNG",
    "PETROL(E20)/CNG": "CNG",
    "DIESEL": "Diesel",
    "PETROL": "Petrol",
    "PETROL/ETHANOL": "Flex Fuel",
    "PETROL(E20)": "Flex Fuel",
}


def apply_filters(df: pd.DataFrame, rtos: list, years: list, months: list) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if rtos:
        mask &= df["rto"].isin(rtos)
    if years:
        mask &= df["year"].isin(years)
    if months:
        mask &= df["month"].isin(months)
    return df[mask]


def fuel_type_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("fuel")["count"]
        .sum()
        .reset_index()
        .rename(columns={"fuel": "Fuel Type", "count": "Registrations"})
        .sort_values("Registrations", ascending=False)
    )


def fuel_group_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    temp["group"] = temp["fuel"].map(FUEL_GROUPS).fillna("Other")
    return (
        temp.groupby("group")["count"]
        .sum()
        .reset_index()
        .rename(columns={"group": "Category", "count": "Registrations"})
        .sort_values("Registrations", ascending=False)
    )


def monthly_trend(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby(["year", "month"])["count"]
        .sum()
        .reset_index()
    )
    result["month"] = pd.Categorical(result["month"], categories=MONTH_ORDER, ordered=True)
    result["period"] = result["month"].astype(str) + " " + result["year"].astype(str)
    return result.sort_values(["year", "month"])


def monthly_trend_by_fuel_group(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    temp["group"] = temp["fuel"].map(FUEL_GROUPS).fillna("Other")
    result = (
        temp.groupby(["year", "month", "group"])["count"]
        .sum()
        .reset_index()
    )
    result["month"] = pd.Categorical(result["month"], categories=MONTH_ORDER, ordered=True)
    result["period"] = result["month"].astype(str) + " " + result["year"].astype(str)
    return result.sort_values(["year", "month"])


def top_brands(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    return (
        df.groupby("brand")["count"]
        .sum()
        .reset_index()
        .rename(columns={"brand": "Brand", "count": "Registrations"})
        .sort_values("Registrations", ascending=False)
        .head(n)
    )


def rto_summary(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    return (
        df.groupby("rto")["count"]
        .sum()
        .reset_index()
        .rename(columns={"rto": "RTO", "count": "Registrations"})
        .sort_values("Registrations", ascending=False)
        .head(n)
    )


def ev_adoption_trend(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    temp["is_ev"] = temp["fuel"].isin([k for k, v in FUEL_GROUPS.items() if v == "EV"])
    monthly = (
        temp.groupby(["year", "month"])
        .apply(lambda g: pd.Series({
            "total": g["count"].sum(),
            "ev": g.loc[g["is_ev"], "count"].sum(),
        }))
        .reset_index()
    )
    monthly["ev_share_%"] = (monthly["ev"] / monthly["total"] * 100).round(2)
    monthly["month"] = pd.Categorical(monthly["month"], categories=MONTH_ORDER, ordered=True)
    monthly["period"] = monthly["month"].astype(str) + " " + monthly["year"].astype(str)
    return monthly.sort_values(["year", "month"])
