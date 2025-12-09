import pandas as pd
import numpy as np
from datetime import date
from src.db.connections import db_manager
from src.config.settings import settings


def get_postgres_data():
    engine = db_manager.get_postgres_engine()
    last_week = (date.today() - pd.DateOffset(days=8)).strftime("%Y-%m-%d")

    raw_logbook_query = f"""
    SELECT *
    FROM public.raw_flight_log
    WHERE date < '{last_week}'
    """
    raw_logbook_df = pd.read_sql(raw_logbook_query, engine).assign(
        formatted_serial_number=lambda x: x["year"].astype(str)
        + "_"
        + x["ac"]
        + "_"
        + x["fl_serial"]
    )

    aircraft_df = pd.read_sql("SELECT * FROM analytics.aircraft_detail", engine)
    pilot_df = pd.read_sql(
        "SELECT distinct on(name) name, dev_id, id as p_id FROM analytics.pilot", engine
    )
    airport_df = pd.read_sql(
        "SELECT distinct dev_id, iata_code, icao_code FROM analytics.airport", engine
    )
    customer_df = pd.read_sql("SELECT * FROM analytics.customer", engine)

    return raw_logbook_df, aircraft_df, pilot_df, airport_df, customer_df


def get_mysql_data(conn):
    read_logbook_sheet_query = "SELECT id as logsheet_id, formatted_serial_number, flight_date FROM itxda_logbook_sheet"
    logbook_sheet_df = pd.read_sql_query(read_logbook_sheet_query, conn)

    existing_flight_query = """
    SELECT DISTINCT formatted_serial_number
    FROM itxda_logbook_entry
    """
    # Note: The original notebook joined with logbook_sheet, but here we can just check logbook_entry directly if formatted_serial_number is there.
    # However, the notebook query was:
    # SELECT DISTINCT ls.formatted_serial_number FROM itxda_logbook_entry AS le JOIN itxda_logbook_sheet AS ls ON le.logsheet_id = ls.id
    # This implies formatted_serial_number is in logbook_sheet, not logbook_entry (or at least that's where they trusted it from).
    # But later in the notebook: "get_data_query = 'SELECT distinct formatted_serial_number FROM itxda_logbook_entry'"
    # So I will use the simpler query from the function `get_db_data` in the notebook.

    existing_flight_df = pd.read_sql_query(existing_flight_query, conn)
    return logbook_sheet_df, existing_flight_df


def map_airport_codes(df, source_col, target_col, airport_ref_df):
    result = df.copy()
    icao_matches = df.merge(
        airport_ref_df[["icao_code", "dev_id"]],
        how="left",
        left_on=source_col,
        right_on="icao_code",
    )
    iata_matches = df.merge(
        airport_ref_df[["iata_code", "dev_id"]],
        how="left",
        left_on=source_col,
        right_on="iata_code",
    )
    result[target_col] = icao_matches["dev_id"].combine_first(iata_matches["dev_id"])
    return result


def build_note(row):
    parts = []
    if pd.isna(row["departure_id"]):
        parts.append("dep_code=" + str(row["from"]))
        parts.append("dep_name=" + str(row["dep"]))
    if pd.isna(row["arrival_id"]):
        parts.append("arr_code=" + str(row["to"]))
        parts.append("arr_name=" + str(row["arr"]))
    if pd.isna(row["pilot_id"]):
        parts.append("pilot=" + str(row["pic"]))
    if pd.isna(row["copilot_id"]) and not pd.isna(row["sic"]):
        parts.append("copilot=" + str(row["sic"]))
    return ";".join(parts) if parts else np.nan


def timedelta_to_hhmmss(col, accumulate_days: bool = True):
    s = pd.to_timedelta(col, errors="coerce")
    if accumulate_days:
        total_secs = s.dt.total_seconds().fillna(0).astype(int)
        hours = total_secs // 3600
        mins = (total_secs % 3600) // 60
        secs = total_secs % 60
    else:
        total_secs = (s.dt.total_seconds() % 86400).fillna(0).astype(int)
        hours = total_secs // 3600
        mins = (total_secs % 3600) // 60
        secs = total_secs % 60
    return (
        hours.map(lambda x: f"{int(x):02d}")
        + ":"
        + mins.map(lambda x: f"{int(x):02d}")
        + ":"
        + secs.map(lambda x: f"{int(x):02d}")
    )


def transform_entry_data(
    raw_logbook_df,
    existing_flight_df,
    aircraft_df,
    customer_df,
    pilot_df,
    airport_df,
    logbook_sheet_df,
):
    existing_serials = set(existing_flight_df["formatted_serial_number"].unique())
    input_raw_logbook_df = raw_logbook_df[
        ~raw_logbook_df["formatted_serial_number"].isin(existing_serials)
    ].copy()

    print(f"New flights found: {len(input_raw_logbook_df)}")
    if input_raw_logbook_df.empty:
        return pd.DataFrame()

    new_logbook_df = input_raw_logbook_df.copy()

    # Map aircraft
    new_logbook_df = new_logbook_df.merge(
        aircraft_df[["aircraft_registration", "dev_id"]],
        how="left",
        left_on="ac",
        right_on="aircraft_registration",
    ).rename({"dev_id": "aircraft_id"}, axis=1)

    # Map customer/flight type
    new_logbook_df = new_logbook_df.merge(
        customer_df[["dev_id", "customer"]], how="left", on="customer"
    ).rename({"dev_id": "flight_type_id"}, axis=1)

    # Map pilots
    new_logbook_df = (
        new_logbook_df.merge(
            pilot_df[["name", "dev_id"]],
            how="left",
            left_on="pic",
            right_on="name",
            suffixes=("", "_pic"),
        )
        .rename({"dev_id": "pilot_id"}, axis=1)
        .drop(columns=["name"], errors="ignore")
        .merge(
            pilot_df[["name", "dev_id"]],
            how="left",
            left_on="sic",
            right_on="name",
            suffixes=("", "_sic"),
        )
        .rename({"dev_id": "copilot_id"}, axis=1)
        .drop(columns=["name"], errors="ignore")
    )

    # Map airports
    new_logbook_df = map_airport_codes(
        new_logbook_df, "from", "departure_id", airport_df
    )
    new_logbook_df = map_airport_codes(new_logbook_df, "to", "arrival_id", airport_df)

    # Merge with logbook sheet
    new_logbook_df = new_logbook_df.merge(
        logbook_sheet_df,
        how="left",
        on="formatted_serial_number",
    ).drop(columns=["flight_date"])

    # Rename columns
    new_logbook_df.rename(
        columns={
            "fl_serial": "raw_serial_number",
            "date": "flight_date",
            "adult": "pax_adult",
            "child": "pax_child",
            "infant": "pax_infant",
            "crew": "pax_crew",
            "start": "hobbs_before",
            "end": "hobbs_after",
            "hours": "flight_hours_decimal",
            "landings": "legs",
            "fuel_return": "fuel_arrive",
            "refuelling": "fuel_uplift",
            "kg": "cargo_kg",
        },
        inplace=True,
    )

    new_logbook_df = new_logbook_df.assign(
        is_refueled=new_logbook_df["fuel_uplift"].notna().astype(int),
        created_by_user_id=settings.DATA_ANALYST_USER_ID,
    )

    new_logbook_df["notes"] = new_logbook_df.apply(build_note, axis=1)

    return new_logbook_df


def load_entries_and_schedules(new_logbook_df):
    if new_logbook_df.empty:
        print("No new entries to load.")
        return

    logbook_df_columns = [
        "raw_serial_number",
        "formatted_serial_number",
        "flight_date",
        "pax_adult",
        "pax_child",
        "pax_infant",
        "pax_crew",
        "cargo_kg",
        "total_weight_kg",
        "take_off_utc",
        "land_utc",
        "block_on_utc",
        "block_off_utc",
        "hobbs_before",
        "hobbs_after",
        "flight_hours_decimal",
        "taxi_time",
        "legs",
        "eng1_cycle",
        "eng2_cycle",
        "tach_time",
        "fuel_depart",
        "fuel_arrive",
        "fuel_uplift",
        "is_refueled",
        "refuel_before_departure",
        "refuel_after_arrival",
        "logsheet_id",
        "pilot_id",
        "aircraft_id",
        "departure_id",
        "arrival_id",
        "flight_type_id",
        "verified_by_id",
        "copilot_id",
        "created_by_user_id",
        "notes",
    ]

    insert_columns = new_logbook_df.columns.intersection(logbook_df_columns).tolist()

    insert_entry_df = (
        new_logbook_df[insert_columns].query("~flight_type_id.isna()")
    ).copy()

    insert_entry_df = insert_entry_df.assign(
        hobbs_before=insert_entry_df["hobbs_before"].astype(float).round(3),
        hobbs_after=insert_entry_df["hobbs_after"].astype(float).round(3),
        flight_hours_decimal=insert_entry_df["flight_hours_decimal"]
        .astype(float)
        .round(3),
        flight_type_id=lambda row: row["flight_type_id"].astype(int),
    )
    insert_entry_df["fuel_uplift"] = insert_entry_df["fuel_uplift"].fillna(0)

    insert_entry_query = f"INSERT INTO itxda_logbook_entry ({', '.join(insert_columns)}) VALUES ({', '.join(['%s'] * len(insert_columns))})"
    insert_entry_values = (
        insert_entry_df.replace({pd.NA: None, np.nan: None}).to_numpy().tolist()
    )

    with db_manager.mysql_connection() as conn:
        with conn.cursor() as cursor:
            if not settings.IS_DEBUGGING:
                print(
                    f"Inserting {len(insert_entry_values)} entries into itxda_logbook_entry..."
                )
                cursor.executemany(insert_entry_query, insert_entry_values)
                conn.commit()

            # Now handle schedules
            # We need to fetch the IDs of the inserted entries.
            # The notebook does this by querying the latest created entries.
            # "SELECT * FROM itxda_logbook_entry WHERE DATE(created) = (SELECT MAX(DATE(created)) FROM itxda_logbook_entry)"
            # This is risky if other processes are inserting, but I'll follow the notebook logic for now.

            entry_data_query = """
            SELECT * FROM itxda_logbook_entry
            WHERE DATE(created) = (
                SELECT MAX(DATE(created)) FROM itxda_logbook_entry
            )
            """
            test_entry_df = pd.read_sql(entry_data_query, conn)

            if test_entry_df.empty:
                print("No entries found after insertion.")
                return

            test_entry_df["take_off_utc"] = timedelta_to_hhmmss(
                test_entry_df["take_off_utc"]
            )
            test_entry_df["land_utc"] = timedelta_to_hhmmss(test_entry_df["land_utc"])

            insert_schedule_df = (
                test_entry_df[
                    [
                        "id",
                        "flight_date",
                        "take_off_utc",
                        "land_utc",
                        "flight_hours_decimal",
                        "flight_type_id",
                        "departure_id",
                        "arrival_id",
                        "aircraft_id",
                        "pilot_id",
                        "copilot_id",
                        "notes",
                    ]
                ]
                .copy()
                .rename(
                    columns={
                        "flight_date": "flight_date_lt",
                        "take_off_utc": "etd_utc",
                        "land_utc": "eta_utc",
                        "flight_hours_decimal": "flight_time_decimal",
                    }
                )
            )

            schedule_columns = [
                "id",
                "flight_date_lt",
                "etd_utc",
                "eta_utc",
                "flight_time_decimal",
                "flight_type_id",
                "departure_id",
                "arrival_id",
                "aircraft_id",
                "pilot_id",
                "copilot_id",
                "notes",
            ]

            schedule_insert_sql = f"""
            INSERT INTO itxda_schedule ({", ".join(schedule_columns)})
            VALUES ({", ".join(["%s"] * len(schedule_columns))})
            """

            schedule_values_df = insert_schedule_df[schedule_columns]
            insert_schedule_values = (
                schedule_values_df.replace({pd.NA: None, np.nan: None})
                .to_numpy()
                .tolist()
            )

            if not settings.IS_DEBUGGING:
                print(f"Inserting {len(insert_schedule_values)} schedules...")
                cursor.executemany(schedule_insert_sql, insert_schedule_values)
                conn.commit()

                # Link table
                entry_schedule_insert_sql = """
                INSERT INTO itxda_entry_x_schedule (log_entry_id, schedule_id)
                VALUES (%s, %s)
                """
                # Since id == id, we just insert (id, id)
                entry_schedule_values = [
                    (x, x) for x in insert_schedule_df["id"].values
                ]
                cursor.executemany(entry_schedule_insert_sql, entry_schedule_values)
                conn.commit()

            run_post_process_updates(cursor, conn)


def run_post_process_updates(cursor, conn):
    queries = [
        """
        UPDATE itxda_schedule AS is2
        LEFT JOIN flight_airport dep_fa ON dep_fa.id = is2.departure_id AND dep_fa.tz_offset IS NOT NULL
        LEFT JOIN flight_airport arr_fa ON arr_fa.id = is2.arrival_id AND arr_fa.tz_offset IS NOT NULL
        SET
            is2.etd_lt = SEC_TO_TIME(MOD(TIME_TO_SEC(is2.etd_utc) + dep_fa.tz_offset * 3600, 86400)),
            is2.eta_lt = SEC_TO_TIME(MOD(TIME_TO_SEC(is2.eta_utc) + arr_fa.tz_offset * 3600, 86400))
        WHERE (is2.etd_utc IS NOT NULL OR is2.eta_utc IS NOT NULL)
          AND (is2.etd_lt IS NULL or is2.eta_lt IS NULL)
          AND (is2.departure_id IS NOT NULL or is2.arrival_id IS NOT NULL);
        """,
        "UPDATE itxda_logbook_entry SET is_locked = 1 WHERE is_locked != 1;",
        """
        UPDATE itxda_schedule ixs
        JOIN flight_airport fa ON fa.id = ixs.departure_id
        SET ixs.base_id = fa.base, ixs.area_id = fa.area;
        """,
        "UPDATE itxda_logbook_sheet ils SET is_verified = 1 WHERE ils.is_verified != 1;",
        "UPDATE itxda_logbook_entry ile SET is_verified = 1 WHERE ile.is_verified != 1;",
        "UPDATE itxda_schedule is2 SET flight_status_id = 1;",
    ]

    for q in queries:
        cursor.execute(q)
    conn.commit()

    # Complex insert for flight_pilot_schedule
    cursor.execute("TRUNCATE TABLE flight_pilot_schedule;")
    conn.commit()

    update_flight_pilot_schedule_query = """
    INSERT INTO flight_pilot_schedule (pilot, duty_date, base, status, notes, created)
    SELECT u.pilot, u.flight_date_lt, u.base_id, 2, NULL, CURRENT_TIMESTAMP
    FROM (
        SELECT 
            p.pilot,
            p.flight_date_lt,
            s.base_id,
            s.etd_lt,
            ROW_NUMBER() OVER (
                PARTITION BY p.pilot, p.flight_date_lt
                ORDER BY s.etd_lt ASC
            ) AS rn
        FROM (
            SELECT pilot_id AS pilot, flight_date_lt, etd_lt, base_id, id
            FROM itxda_schedule
            WHERE pilot_id IS NOT NULL AND flight_date_lt IS NOT NULL AND etd_lt IS NOT NULL
            UNION ALL
            SELECT copilot_id AS pilot, flight_date_lt, etd_lt, base_id, id
            FROM itxda_schedule
            WHERE copilot_id IS NOT NULL AND flight_date_lt IS NOT NULL AND etd_lt IS NOT NULL
        ) p
        JOIN itxda_schedule s ON s.id = p.id
    ) u
    WHERE u.rn = 1
      AND NOT EXISTS (
          SELECT 1
          FROM flight_pilot_schedule fps
          WHERE fps.pilot = u.pilot
            AND fps.duty_date = u.flight_date_lt
      );
    """
    cursor.execute(update_flight_pilot_schedule_query)
    conn.commit()


def run_logbook_entry_pipeline():
    print("Running Logbook Entry Pipeline...")
    raw_logs, aircraft_df, pilot_df, airport_df, customer_df = get_postgres_data()

    with db_manager.mysql_connection() as conn:
        logbook_sheet_df, existing_flight_df = get_mysql_data(conn)

        new_logbook_df = transform_entry_data(
            raw_logs,
            existing_flight_df,
            aircraft_df,
            customer_df,
            pilot_df,
            airport_df,
            logbook_sheet_df,
        )

        load_entries_and_schedules(new_logbook_df)
    print("Logbook Entry Pipeline Finished.")


if __name__ == "__main__":
    run_logbook_entry_pipeline()
