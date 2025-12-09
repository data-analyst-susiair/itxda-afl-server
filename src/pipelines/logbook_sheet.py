import pandas as pd
from datetime import date
from src.db.connections import db_manager
from src.config.settings import settings
import pymysql


def get_raw_flight_logs():
    last_week = (date.today() - pd.DateOffset(days=8)).strftime("%Y-%m-%d")
    query = f"""
    SELECT *
    FROM public.raw_flight_log
    WHERE date < '{last_week}'
    """
    return pd.read_sql(query, db_manager.get_postgres_engine())


def get_aircraft_details():
    query = """
    SELECT *
    FROM analytics.aircraft_detail
    """
    return pd.read_sql(query, db_manager.get_postgres_engine())


def transform_logbook_data(raw_logbook_df, aircraft_df):
    # Add formatted_serial_number
    raw_logbook_df["formatted_serial_number"] = (
        raw_logbook_df["year"].astype(str)
        + "_"
        + raw_logbook_df["ac"]
        + "_"
        + raw_logbook_df["fl_serial"]
    )

    # Aggregate
    agg_df = (
        raw_logbook_df.groupby("formatted_serial_number")
        .agg(
            flight_date=("date", "min"),
            hobbs_start=("start", "min"),
            hobbs_end=("end", "max"),
            total_flight_hours_decimal=("hours", "sum"),
            total_legs=("landings", "sum"),
        )
        .reset_index()
    )

    # Extract aircraft registration and raw serial number
    agg_df["aircraft_registration"] = agg_df["formatted_serial_number"].str.split(
        "_", expand=True
    )[1]
    agg_df["raw_serial_number"] = agg_df["formatted_serial_number"].str.split(
        "_", expand=True
    )[2]

    # Prepare final dataframe structure
    logbook_df_columns = [
        "id",
        "doc_img",
        "flight_date",
        "hobbs_start",
        "hobbs_end",
        "total_flight_hours_decimal",
        "legs_start",
        "legs_end",
        "total_legs",
        "created",
        "updated",
        "is_verified",
        "status",
        "created_by_id",
        "updated_by_id",
        "verified_by_id",
        "raw_serial_number",
        "formatted_serial_number",
    ]

    logbook_df = pd.DataFrame(columns=logbook_df_columns)
    logbook_df = pd.concat([logbook_df, agg_df], ignore_index=True, sort=False)

    logbook_df["total_flight_hours_decimal"] = (
        logbook_df["total_flight_hours_decimal"].astype(float).round(3)
    )

    # Merge aircraft details
    logbook_df = logbook_df.merge(
        aircraft_df[["aircraft_registration", "dev_id"]],
        how="left",
        on="aircraft_registration",
    ).rename({"dev_id": "aircraft_id"}, axis=1)

    return logbook_df


def filter_new_records(cursor, new_data_df):
    get_data_query = "SELECT distinct id, flight_date, formatted_serial_number FROM itxda_logbook_sheet"
    cursor.execute(get_data_query)
    existing_records = cursor.fetchall()

    # existing_records is a list of dicts because of DictCursor
    existing_serials = {
        record["formatted_serial_number"] for record in existing_records
    }

    mask = ~new_data_df["formatted_serial_number"].isin(existing_serials)

    print(f"Existing records count: {len(existing_records)}")
    print(f"New records count: {len(new_data_df[mask])}")

    return new_data_df[mask]


def load_logbook_sheets(logbook_df):
    insert_columns = [
        "flight_date",
        "hobbs_start",
        "hobbs_end",
        "total_flight_hours_decimal",
        "total_legs",
        "raw_serial_number",
        "formatted_serial_number",
        "aircraft_id",
    ]

    insert_df = logbook_df[insert_columns].copy()
    insert_df = insert_df.assign(
        hobbs_start=insert_df["hobbs_start"].astype(float).round(3),
        hobbs_end=insert_df["hobbs_end"].astype(float).round(3),
    )

    insert_query = f"INSERT INTO itxda_logbook_sheet ({', '.join(insert_columns)}) VALUES ({', '.join(['%s'] * len(insert_columns))})"

    with db_manager.mysql_connection() as conn:
        with conn.cursor() as cursor:
            new_records_df = filter_new_records(cursor, insert_df)

            new_data = list(new_records_df.itertuples(index=False, name=None))

            if new_data:
                print(f"Inserting {len(new_data)} records into itxda_logbook_sheet...")
                cursor.executemany(insert_query, new_data)
                conn.commit()
                print("Insertion complete.")
            else:
                print("No new records to insert into itxda_logbook_sheet.")


def run_logbook_sheet_pipeline():
    print("Running Logbook Sheet Pipeline...")
    raw_logs = get_raw_flight_logs()
    aircraft_details = get_aircraft_details()
    processed_data = transform_logbook_data(raw_logs, aircraft_details)
    load_logbook_sheets(processed_data)
    print("Logbook Sheet Pipeline Finished.")


if __name__ == "__main__":
    run_logbook_sheet_pipeline()
