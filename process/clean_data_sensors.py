from sqlalchemy import text
from utils import  create_engine_from_config


# This script is used to clean the sensors data
# Two tables are saved veloclimatmeter_preprocess and labsticc_sensor_preprocess

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
    Crée et remplit la table veloclimatmeter_preprocess

    Filtre:
    - Exclut les données de "Saint-Jean La Poterie"
    - Conserve les données entre 27 juin 06:00 et 30 juin 22:00

    Args:
        conn: connexion SQLAlchemy
    """
    print("\n📊 Nettoyage des données veloclimatmeter...")

    # Drop and recreate table
    conn.execute(text("DROP TABLE IF EXISTS veloclimat.veloclimatmeter_preprocess"))
    conn.commit()

    # Create and populate table veloclimatmeter_preprocess
    query = """
            CREATE TABLE veloclimat.veloclimatmeter_preprocess AS
            SELECT
                max(id) as id,
                "timestamp",
                id_track,
                st_centroid(st_collect(THE_GEOM)) as THE_GEOM,
                avg(altitude) as altitude,
                avg(vitesse) as vitesse,
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
                thermo_name,
                sensor_name,
                avg(elevation) as elevation
            FROM veloclimat.veloclimatmeter
            WHERE "timestamp" > CAST('2025-06-27 06:00:00.000 +0200' as timestamp)
              AND "timestamp" < CAST('2025-06-30 22:00:00.000 +0200' as timestamp)
              AND thermo_name != 'Saint-Jean La Poterie'
    GROUP BY "timestamp", thermo_name, sensor_name, id_track \
            """

    conn.execute(text(query))
    conn.commit()
    print("✅ Table veloclimatmeter_preprocess créée avec succès !")


def clean_labsticc_sensor_data(conn):
    """
    Crée et remplit la table labsticc_sensor_preprocess

    Filtre:
    - Agrège les données à la seconde (DATE_TRUNC)
    - Exclut les données avec une précision GPS > 25 m

    Args:
        conn: connexion SQLAlchemy
    """
    print("\n📊 Nettoyage des données labsticc_sensor...")

    # Drop and recreate table
    conn.execute(text("DROP TABLE IF EXISTS veloclimat.labsticc_sensor_preprocess"))
    conn.commit()

    query = """
            CREATE TABLE veloclimat.labsticc_sensor_preprocess AS
            SELECT
                max(id) as id,
                DATE_TRUNC('second', "timestamp") as "timestamp",
                avg(temperature) as temperature,
                avg(humidity) as humidity,
                st_centroid(st_collect(THE_GEOM)) as THE_GEOM,
                avg(accuracy) as accuracy,
                sensor_name,
                id_track,
                thermo_name,
                avg(elevation) as elevation
            FROM veloclimat.labsticc_sensor
            WHERE accuracy <= 25
            GROUP BY DATE_TRUNC('second', "timestamp"), thermo_name, sensor_name, id_track \
            """

    conn.execute(text(query))
    conn.commit()
    print("✅ Table labsticc_sensor_preprocess créée avec succès !")


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
            clean_labsticc_sensor_data(conn)

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