from sqlalchemy import text
from utils import  create_engine_from_config


# This script is used to clean the tables :
# labsticc_sensor_raw and veloclimatmeter_meteo_raw

# Config file structure to connect to the database
# Please set a valid file
# {
#     "database": {
#         "host": "localhost",
#         "port": 5432,
#         "user": "user_name",
#         "password": "password",
#         "database": "database_name"
#     }
# }

def clean_veloclimatmeter_data(conn):
    """
    Clean and create a new table called veloclimatmeter_meteo_preprocess

    - Exclut les données de "Saint-Jean La Poterie"
    - Conserve les données entre 27 juin 06:00 et 3 juillet 23:30
    - Exclu les points avec des vitesses < 1 m/s
    - Set a unique_id_track based on sensor_name, thermo_name and id_track

    Args:
        conn: connexion SQLAlchemy
    """
    print("\n📊 Clean veloclimatmeter_meteo_raw data...")

    # Create and populate table veloclimatmeter_preprocess
    # filter with speed value. To be computed before
    query = """
            DROP TABLE IF EXISTS veloclimat.veloclimatmeter_meteo_preprocess;
            CREATE TABLE veloclimat.veloclimatmeter_meteo_preprocess AS
            SELECT
                max(id) as id,
                id_track,
                thermo_name,
                sensor_name,
                "timestamp",
                st_centroid(st_collect(THE_GEOM)) as THE_GEOM,
                avg(altitude) as altitude,
                avg(vitesse) as vitesse,
                avg(vitesse)/3.6 as speed_m_s,
                avg(direction) as direction,
                avg(temperature) as temperature,
                avg(humidite) as humidite,
                avg(pression) as pression,
                avg(temperature_bot) as temperature_bot,
                avg(temperature_top) as temperature_top,
                avg(pm_1_ug_m3) as pm_1_ug_m3,
                avg(pm_2_5_ug_m3) as pm_2_5_ug_m3,
                avg(pm_10_ug_m3) as pm_10_ug_m3,
                avg(niveau_sonore_db_a) as niveau_sonore_db_a,
                avg(distancegauche) as distancegauche,
                avg(distancedroite) as distancedroite,
                avg(elevation) as elevation
            FROM (select * from veloclimat.veloclimatmeter_meteo_raw where vitesse/3.6 >= 1) AS FOO
            WHERE "timestamp" > CAST('2025-06-27 06:00:00.000 +0200' as timestamp)
              AND "timestamp" < CAST('2025-07-03 23:00:00.000 +0200' as timestamp)
              AND thermo_name != 'Saint-Jean La Poterie'
            GROUP BY "timestamp", sensor_name, thermo_name, id_track;
                
            ALTER TABLE veloclimat.veloclimatmeter_meteo_preprocess ADD COLUMN unique_id_track TEXT;

            UPDATE veloclimat.veloclimatmeter_meteo_preprocess
            SET unique_id_track = encode(digest(
                                                 id_track::TEXT || '|' || sensor_name || '|' || thermo_name,
                                                 'md5'
                                         ), 'hex');
            CREATE INDEX idx_veloclimatmeter_meteo_preprocess_unique_id_track
                ON veloclimat.veloclimatmeter_meteo_preprocess (unique_id_track);

            CREATE INDEX idx_veloclimatmeter_meteo_preprocess_id
                ON veloclimat.veloclimatmeter_meteo_preprocess (id);

            CREATE INDEX idx_veloclimatmeter_meteo_preprocess_timestamp
                ON veloclimat.veloclimatmeter_meteo_preprocess ("timestamp");

            CREATE INDEX idx_veloclimatmeter_meteo_preprocess_the_geom
                ON veloclimat.veloclimatmeter_meteo_preprocess using GIST (the_geom);
            """

    conn.execute(text(query))
    conn.commit()

    print("✅ Table veloclimatmeter_meteo_preprocess created !")


def clean_labsticc_sensors_data(conn):
    """
    Clean and create two tables: labsticc_sensors_preprocess and labsticc_sensors_reference_preprocess.

    labsticc_sensors_preprocess contains the data collected during mobile thermal measurements.

    The following processes are applied:
    - Remove duplicate entries in the input data.
    - Aggregate data by second (using DATE_TRUNC).
    - Exclude data with GPS accuracy > 25 meters.
    - Calculate speeds between consecutive points and using a sliding window.

    labsticc_sensors_reference_preprocess stores the data for reference sensors (fixed stations).
    The following processes are applied:
    - Aggregate data by second (using DATE_TRUNC).

    Args:
    conn: SQLAlchemy connection.
    """
    print("\n📊 Clean labsticc_sensors_raw...")

    query = """
            -- 1. Drop temporary tables if they exist
            DROP TABLE IF EXISTS veloclimat.labsticc_sensors_unique;
            DROP TABLE IF EXISTS veloclimat.labsticc_sensors_preprocess;

            -- 2. First step: Deduplication and aggregation of raw data
            CREATE TABLE veloclimat.labsticc_sensors_unique AS
            SELECT
                max(id) as id,
                sensor_name,
                thermo_name,
                id_track,
                DATE_TRUNC('second', "timestamp") as "timestamp",
                avg(temperature) as temperature,
                avg(humidity) as humidity,
                st_centroid(st_collect(THE_GEOM)) as THE_GEOM,
                avg(accuracy) as accuracy,
                avg(elevation) as elevation
            FROM veloclimat.labsticc_sensors_raw
            WHERE accuracy <= 25 AND thermo_name NOT ILIKE '%reference%' and temperature is not null
            GROUP BY DATE_TRUNC('second', "timestamp"), sensor_name, thermo_name, id_track;

            -- 3. Second step: Remove exact duplicates and stationary points
            CREATE TABLE veloclimat.labsticc_sensors_preprocess AS
            WITH unique_rows AS (
                SELECT
                    id,
                    id_track,
                    sensor_name,
                    thermo_name,
                    the_geom,
                    "timestamp",
                    temperature,
                    humidity,
                    accuracy,
                    elevation,
                    ROW_NUMBER() OVER (
            PARTITION BY sensor_name, thermo_name, the_geom, "timestamp"
            ORDER BY id) AS row_num
                FROM veloclimat.labsticc_sensors_unique
                WHERE thermo_name NOT ILIKE '%reference%'),

            -- 4. Calculate speeds between consecutive points
            ranked_data AS (
                SELECT
                    id,
                    id_track,
                    sensor_name,
                    thermo_name,
                    the_geom,
                    "timestamp",
                    temperature,
                    humidity,
                    accuracy,
                    elevation,
                    LAG(the_geom) OVER (PARTITION BY sensor_name, thermo_name, id_track ORDER BY "timestamp") AS prev_the_geom,
                    LAG("timestamp") OVER (PARTITION BY sensor_name, thermo_name, id_track ORDER BY "timestamp") AS prev_timestamp
                FROM unique_rows
                WHERE row_num = 1),

            speed_data AS (
            SELECT
                id,
                id_track,
                sensor_name,
                thermo_name,
                the_geom,
                "timestamp",
                temperature,
                humidity,
                accuracy,
                elevation,
                prev_the_geom,
                CASE
                    WHEN prev_the_geom IS NOT NULL AND prev_timestamp IS NOT NULL
                        AND EXTRACT(EPOCH FROM ("timestamp" - prev_timestamp)) > 0
                    THEN ST_Distance(the_geom, prev_the_geom, TRUE) / EXTRACT(EPOCH FROM ("timestamp" - prev_timestamp))
                    ELSE NULL
                END AS speed_m_s
            FROM ranked_data),

            -- 5. Remove stationary points (identical geometry and speed = 0)
        filtered_data AS (
            SELECT *
            FROM speed_data
            WHERE NOT (ST_Equals(the_geom, prev_the_geom) AND (speed_m_s = 0 OR speed_m_s IS NULL))
        )
            SELECT
                id,
                id_track,
                sensor_name,
                thermo_name,
                the_geom,
                "timestamp",
                temperature,
                humidity,
                accuracy,
                elevation,
                speed_m_s
            FROM filtered_data;

            -- 6. Add unique key column and create indexes
            ALTER TABLE veloclimat.labsticc_sensors_preprocess ADD COLUMN unique_id_track TEXT;

            UPDATE veloclimat.labsticc_sensors_preprocess
            SET unique_id_track = encode(digest(
                                                 id_track::TEXT || '|' || sensor_name || '|' || thermo_name,
                                                 'md5'
                                         ), 'hex');

            CREATE INDEX idx_labsticc_sensors_preprocess_unique_id_track
                ON veloclimat.labsticc_sensors_preprocess (unique_id_track);

            CREATE INDEX idx_labsticc_sensors_preprocess_timestamp
                ON veloclimat.labsticc_sensors_preprocess ("timestamp");

            CREATE INDEX idx_labsticc_sensors_preprocess_the_geom
                ON veloclimat.labsticc_sensors_preprocess using GIST (the_geom);

            CREATE INDEX idx_labsticc_sensors_preprocess_id
                ON veloclimat.labsticc_sensors_preprocess (id);

            -- Update the speed_m_s column with the new points
            -- We use the unique_id_track as identifier
            UPDATE veloclimat.labsticc_sensors_preprocess AS target
            SET speed_m_s = speed_data.speed_m_s
                FROM (
                -- Calculate speeds between consecutive points
                WITH ranked_data AS (
                    SELECT
                        id,
                        id_track,
                        the_geom,
                        "timestamp",
                        LAG(the_geom) OVER (PARTITION BY unique_id_track ORDER BY "timestamp") AS prev_the_geom,
                        LAG("timestamp") OVER (PARTITION BY unique_id_track ORDER BY "timestamp") AS prev_timestamp
                    FROM veloclimat.labsticc_sensors_preprocess
                ),
                speed_data AS (
                    SELECT
                        id,
                        CASE
                            WHEN prev_the_geom IS NOT NULL AND prev_timestamp IS NOT NULL
                                AND EXTRACT(EPOCH FROM ("timestamp" - prev_timestamp)) > 0
                            THEN ST_Distance(the_geom, prev_the_geom, TRUE) / EXTRACT(EPOCH FROM ("timestamp" - prev_timestamp))
                            ELSE NULL
                        END AS speed_m_s
                    FROM ranked_data
                )
                SELECT id, speed_m_s FROM speed_data
            ) AS speed_data WHERE target.id = speed_data.id;

            -- Add a column to compute the smoothed speed
            ALTER TABLE veloclimat.labsticc_sensors_preprocess ADD COLUMN speed_m_s_smooth DOUBLE PRECISION;

            -- Update the speed_m_s_smooth column with a sliding window average
            -- We use the unique_id_track as identifier
            -- 5 points : ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            UPDATE veloclimat.labsticc_sensors_preprocess AS target
            SET speed_m_s_smooth = speed_smooth.speed_m_s_smooth
                FROM (
                SELECT
                    id,
                    AVG(speed_m_s) OVER (
                        PARTITION BY unique_id_track
                        ORDER BY "timestamp"
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS speed_m_s_smooth
                FROM veloclimat.labsticc_sensors_preprocess
            ) AS speed_smooth
                        WHERE target.id = speed_smooth.id;
                
            --Clean db
            DROP TABLE IF EXISTS veloclimat.labsticc_sensors_unique;            
            """

    conn.execute(text(query))
    conn.commit()

    # Create the reference table
    # data are merge to second
    # Keep reference sensors
    query = """
            DROP TABLE IF EXISTS veloclimat.labsticc_sensors_reference_preprocess;
            CREATE TABLE veloclimat.labsticc_sensors_reference_preprocess AS
            SELECT
                max(id) as id,
                sensor_name, thermo_name, id_track,
                DATE_TRUNC('second', "timestamp") as "timestamp",
                avg(temperature) as temperature,
                avg(humidity) as humidity,
                st_centroid(st_collect(THE_GEOM)) as THE_GEOM,
                avg(accuracy) as accuracy,
                avg(elevation) as elevation
            FROM veloclimat.labsticc_sensors_raw
            WHERE  thermo_name  ilike '%reference%' and temperature is not null
            GROUP BY DATE_TRUNC('second', "timestamp"), sensor_name, thermo_name, id_track;

            -- Add unique key column and create indexes
            ALTER TABLE veloclimat.labsticc_sensors_reference_preprocess ADD COLUMN unique_id_track TEXT;
            UPDATE veloclimat.labsticc_sensors_reference_preprocess
            SET unique_id_track = encode(digest(
                                                 id_track::TEXT || '|' || sensor_name || '|' || thermo_name,
                                                 'md5'
                                         ), 'hex');

            CREATE INDEX idx_labsticc_sensors_reference_preprocess_unique_id_track
                ON veloclimat.labsticc_sensors_reference_preprocess (unique_id_track);

            CREATE INDEX idx_labsticc_sensors_reference_preprocess_timestamp
                ON veloclimat.labsticc_sensors_reference_preprocess ("timestamp");

            CREATE INDEX idx_labsticc_sensors_reference_preprocess_the_geom
                ON veloclimat.labsticc_sensors_reference_preprocess using GIST (the_geom);

            CREATE INDEX idx_labsticc_sensors_reference_preprocess_id
                ON veloclimat.labsticc_sensors_reference_preprocess (id);
            """

    conn.execute(text(query))
    conn.commit()

    print("✅ Tables labsticc_sensors_preprocess and labsticc_sensors_reference_preprocess created !")


def main():
    """
    Fonction principale pour nettoyer les données des capteurs
    """

    # Créer l'engine
    engine = create_engine_from_config("config.json")

    try:
        with engine.connect() as conn:
            # Tester la connexion
            conn.execute(text("SELECT 1"))
            print("✅ Connexion à PostgreSQL réussie !")

            # Nettoyer les deux tables
            clean_veloclimatmeter_data(conn)
            clean_labsticc_sensors_data(conn)

            print("\n" + "=" * 70)
            print("✅ Nettoyage des données terminé avec succès !")
            print("=" * 70)
            return True

    except Exception as e:
        print(f"❌ Erreur SQL : {e}")
        return False

    finally:
        engine.dispose()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)