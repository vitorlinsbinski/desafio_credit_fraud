import pandas as pd

def validate_risk_score_per_location(
    df: pd.DataFrame
) -> pd.DataFrame:
    SOURCE_TABLE = "transformed.credit_fraud"
    TARGET_TABLE = "risk_score_per_location"

    VALID_REGIONS = {
        "africa",
        "asia",
        "europe",
        "north america",
        "south america",
        "oceania",
    }

    errors = []

    # location_region NULL
    mask = df["location_region"].isna()

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "location_region",
            "error_type": "NULL_VALUE",
            "error_message": "location_region is NULL"
        })

    # location_region fora do domínio esperado
    mask = (
        df["location_region"].notna()
        & ~df["location_region"].isin(VALID_REGIONS)
    )

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "location_region",
            "error_type": "INVALID_DOMAIN",
            "error_message": (
                f"Invalid location_region: {row['location_region']}"
            )
        })

    # risk_score NULL
    mask = df["risk_score"].isna()

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "risk_score",
            "error_type": "NULL_VALUE",
            "error_message": "risk_score is NULL"
        })

    # risk_score fora do intervalo
    mask = (
        df["risk_score"].notna()
        & ~df["risk_score"].between(0, 100)
    )

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "risk_score",
            "error_type": "OUT_OF_RANGE",
            "error_message": "risk_score must be between 0 and 100"
        })

    return pd.DataFrame(errors)
    
def validate_top_receiving_address(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    SOURCE_TABLE = "transformed.credit_fraud"
    TARGET_TABLE = "top_receiving_address"

    errors = []

    # receiving_address NULL
    mask = df["receiving_address"].isna()

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "receiving_address",
            "error_type": "NULL_VALUE",
            "error_message": "receiving_address is NULL"
        })
        
    # transaction_type NULL
    mask = df["transaction_type"].isna()

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "transaction_type",
            "error_type": "NULL_VALUE",
            "error_message": "transaction_type is NULL"
        })
        
    # amount NULL
    mask = df["amount"].isna()

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "amount",
            "error_type": "NULL_VALUE",
            "error_message": "amount is NULL"
        })
        
    # transaction_timestamp NULL
    mask = df["transaction_timestamp"].isna()

    for _, row in df[mask].iterrows():
        errors.append({
            "execution_id": row["execution_id"],
            "source_hash": row["source_hash"],
            "source_row_number": row["source_row_number"],
            "source_table": SOURCE_TABLE,
            "target_table": TARGET_TABLE,
            "column_name": "transaction_timestamp",
            "error_type": "NULL_VALUE",
            "error_message": "transaction_timestamp is NULL"
        })

    return pd.DataFrame(errors)