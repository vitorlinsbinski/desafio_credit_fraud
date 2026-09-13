import pandas as pd


STRING_NORMALIZABLE_COLUMNS = [
    "transaction_type",
    "location_region",
    "purchase_pattern",
    "age_group",
    "anomaly",
]

STRING_IDENTIFIER_COLUMNS = [
    "sending_address",
    "receiving_address",
    "ip_prefix",
]

NUMERIC_COLUMNS = [
    "amount",
    "login_frequency",
    "session_duration",
    "risk_score",
]

TIMESTAMP_COLUMNS = [
    "transaction_timestamp",
]

NULL_VALUES = {
    "",
    "null",
    "none",
    "nan",
    "n/a",
    "na",
    "undefined",
    "missing",
}


def clean_credit_fraud_df(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df_cleaned = cast_types(
        df=df,
        string_columns=(
            STRING_NORMALIZABLE_COLUMNS
            + STRING_IDENTIFIER_COLUMNS
        ),
        numeric_columns=NUMERIC_COLUMNS,
        timestamp_columns=TIMESTAMP_COLUMNS,
    )

    df_cleaned = normalize_str_columns(
        df=df_cleaned,
        columns=STRING_NORMALIZABLE_COLUMNS,
    )

    for col in STRING_IDENTIFIER_COLUMNS:
        df_cleaned[col] = (
            df_cleaned[col]
            .astype("string")
            .str.strip()
        )

    string_columns = (
        STRING_NORMALIZABLE_COLUMNS
        + STRING_IDENTIFIER_COLUMNS
    )

    for col in string_columns:
        null_mask = (
            df_cleaned[col]
            .astype("string")
            .str.strip()
            .str.lower()
            .isin(NULL_VALUES)
        )

        df_cleaned.loc[null_mask, col] = pd.NA
    
    df_final = df_cleaned.drop_duplicates(keep="first")

    return df_final


def cast_types(
    df: pd.DataFrame,
    string_columns: list[str],
    numeric_columns: list[str],
    timestamp_columns: list[str],
) -> pd.DataFrame:
    df_casted = df.copy()

    for col in string_columns:
        df_casted[col] = df_casted[col].astype("string")

    for col in numeric_columns:
        df_casted[col] = pd.to_numeric(
            df_casted[col],
            errors="coerce",
        )

    for col in timestamp_columns:
        df_casted[col] = pd.to_datetime(
            df_casted[col],
            unit="s",
            errors="coerce",
        )

    return df_casted


def normalize_str_columns(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    df_normalized = df.copy()

    for col in columns:
        df_normalized[col] = (
            df_normalized[col]
            .astype("string")
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
        )

    return df_normalized