from sqlalchemy import  text

from process.utils import create_engine_from_config


# Config file structure
# {
#     "database": {
#         "host": "localhost",
#         "port": 5432,
#         "user": "user_name",
#         "password": "password",
#         "database": "database_name"
#     }
# }

def compute_stats_multiple_hours(config_path, table_name, columns, hours_ranges, output_table=None):
    """
    Calcule les stats pour plusieurs plages horaires en une seule requête

    Args:
        config_path: chemin vers le fichier config.json
        table_name: nom de la table (ex: 'schema.table')
        columns: liste des colonnes (ex: ['temperature', 'humidity'])
        hours_ranges: liste de tuples (start_hour, end_hour)
                     ex: [(8, 12), (14, 18), (20, 24)]
                     ex: [(21, 6)] → capture 21:00-23:59 ET 00:00-05:59
        output_table: nom optionnel de la table de sortie. Si None, affiche seulement les résultats.

    Returns:
        Row object avec les statistiques, ou None en cas d'erreur
    """

    # Charger la configuration
    engine = create_engine_from_config(config_path)

    # Valider table_name pour éviter SQL injection
    table_parts = table_name.split('.')
    for part in table_parts:
        if not part.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

    # Valider output_table si fourni
    if output_table:
        output_parts = output_table.split('.')
        for part in output_parts:
            if not part.isidentifier():
                raise ValueError(f"Invalid output table name: {output_table}")

    valid_cols = [col.strip() for col in columns if col.strip().isidentifier()]
    if not valid_cols:
        raise ValueError("Aucune colonne valide spécifiée")

    select_clauses = []

    # Ajouter les stats globales UNE SEULE FOIS (en dehors de la boucle)
    # Utiliser 'Europe/Paris' pour les heures et les dates
    select_clauses.append('COUNT(DISTINCT CAST("timestamp" AT TIME ZONE \'Europe/Paris\' AS DATE)) as nombre_jours')
    select_clauses.append('MIN(EXTRACT(HOUR FROM "timestamp" AT TIME ZONE \'Europe/Paris\')) as heure_min')
    select_clauses.append('MAX(EXTRACT(HOUR FROM "timestamp" AT TIME ZONE \'Europe/Paris\')) as heure_max')

    # Pour chaque plage horaire
    for start_hour, end_hour in hours_ranges:
        # Validation: heures entre 0 et 24
        if not (0 <= start_hour <= 24 and 0 <= end_hour <= 24):
            raise ValueError(f"Heures invalides: {start_hour}-{end_hour} (doivent être entre 0 et 24)")

        if start_hour == end_hour:
            raise ValueError(f"start_hour ne peut pas être égal à end_hour: {start_hour}")

        range_name = f"{start_hour:02d}h_{end_hour:02d}h"

        # Prise en compte des plages cross-midnight (ex: 21-6 = 21h à 23h59 ET 0h à 5h59)
        if start_hour < end_hour:
            # Plage normale (ex: 12-18)
            where_clause = f'EXTRACT(HOUR FROM "timestamp" AT TIME ZONE \'Europe/Paris\') >= {start_hour} AND EXTRACT(HOUR FROM "timestamp" AT TIME ZONE \'Europe/Paris\') < {end_hour}'
        else:
            # Plage cross-midnight (ex: 21-6 = 21h ou plus OU moins de 6h)
            where_clause = f'(EXTRACT(HOUR FROM "timestamp" AT TIME ZONE \'Europe/Paris\') >= {start_hour} OR EXTRACT(HOUR FROM "timestamp" AT TIME ZONE \'Europe/Paris\') < {end_hour})'

        for col in valid_cols:
            # Ajouter un filtre supplémentaire pour TEMPERATURE_TOP et TEMPERATURE_BOT (ignorer <= 0)
            if col in ['temperature_top', 'temperature_bot']:
                col_where_clause = f'{where_clause} AND "{col}" > 0'
            else:
                col_where_clause = where_clause

            select_clauses.append(f'max("{col}") FILTER (WHERE {col_where_clause}) as max_{col}_{range_name}')
            select_clauses.append(f'min("{col}") FILTER (WHERE {col_where_clause}) as min_{col}_{range_name}')
            select_clauses.append(f'avg("{col}") FILTER (WHERE {col_where_clause}) as avg_{col}_{range_name}')

        select_clauses.append(f'count(*) FILTER (WHERE {where_clause}) as count_{range_name}')

    query = f"SELECT {', '.join(select_clauses)} FROM {table_name}"

    try:
        with engine.connect() as conn:

            # Si une table de sortie est spécifiée, créer et remplir la table
            if output_table:
                print(f"📝 Création de la table {output_table}...")
                conn.execute(text(f"DROP TABLE IF EXISTS {output_table}"))
                conn.execute(text(f"CREATE TABLE {output_table} AS {query}"))
                conn.commit()
                print(f"✅ Table {output_table} créée avec succès")

                # Récupérer les données pour affichage
                result = conn.execute(text(f"SELECT * FROM {output_table}"))
            else:
                # Sinon, exécuter la requête directement
                result = conn.execute(text(query))

            row = result.mappings().fetchone()

            if row is None:
                print("⚠️ Aucune donnée trouvée")
                return None

            # Affichage formaté
            print("\n" + "=" * 70)
            print("📊 STATISTIQUES PAR PLAGE HORAIRE")
            print("=" * 70)

            # Afficher les stats globales
            nombre_jours = row['nombre_jours']
            heure_min = int(row['heure_min']) if row['heure_min'] is not None else None
            heure_max = int(row['heure_max']) if row['heure_max'] is not None else None

            # Calcule l'amplitude horaire
            if heure_min is not None and heure_max is not None:
                amplitude_horaire = heure_max - heure_min + 1
            else:
                amplitude_horaire = None

            print(f"\n📅 {table_name}")
            print(f"   Jours distincts: {nombre_jours}")
            if heure_min is not None:
                print(f"   Heure minimum: {heure_min:02d}h")
            if heure_max is not None:
                print(f"   Heure maximum: {heure_max:02d}h")
            if amplitude_horaire is not None:
                print(f"   Amplitude horaire: {amplitude_horaire}h (de {heure_min:02d}h à {heure_max:02d}h)")

            # Afficher les stats par plage horaire
            for start_hour, end_hour in hours_ranges:
                range_name = f"{start_hour:02d}h_{end_hour:02d}h"
                print(f"\n⏰ Plage {range_name}")
                print("-" * 70)

                for col in valid_cols:
                    max_val = row[f'max_{col}_{range_name}']
                    min_val = row[f'min_{col}_{range_name}']
                    avg_val = row[f'avg_{col}_{range_name}']

                    print(f"  {col.upper()}:")
                    print(f"    Max: {max_val:.2f}" if max_val is not None else f"    Max: N/A")
                    print(f"    Min: {min_val:.2f}" if min_val is not None else f"    Min: N/A")
                    print(f"    Moyenne: {avg_val:.2f}" if avg_val is not None else f"    Moyenne: N/A")

                count = row[f'count_{range_name}']
                print(f"  Nombre de mesures: {count if count else 0}")

            print("\n" + "=" * 70)
            return row

    except Exception as e:
        print(f"❌ Erreur : {e}")
        return None

    finally:
        engine.dispose()


# Run
if __name__ == "__main__":

    # Seuils Meteo-France
    # Periode de surveillance : Du 1er juin au 15 septembre
    # Plages horaires pour les seuils de température
    # Température maximale (jour) : Mesurée généralement entre 12h et 18h (période la plus chaude de la journée).
    # Température maximale diurne : 31 à 33°C
    # Température minimale (nuit) : Mesurée entre 21h et 6h (période nocturne).
    # Température minimale nocturne : 18 à 19°C

    print("🔍 Analyse des statistiques par plages horaires")
    print("=" * 70)

    stats_labsticc_sensor = compute_stats_multiple_hours(
        config_path="config.json",
        table_name="veloclimat.labsticc_sensors_raw",
        columns=["temperature", "humidity", "accuracy"],
        hours_ranges=[(0,24)],
        output_table="veloclimat.labsticc_sensor_stats"
    )

    print("\n")

    stats_veloclimatmeter = compute_stats_multiple_hours(
        config_path="config.json",
        table_name="veloclimat.veloclimatmeter_meteo_raw",
        columns=["temperature", "humidite", "vitesse", "temperature_bot", "temperature_top"],
        hours_ranges=[(0,24)],
        output_table="veloclimat.veloclimatmeter_meteo_stats"
    )

    #Stats par plage

    stats_labsticc_sensor = compute_stats_multiple_hours(
        config_path="config.json",
        table_name="veloclimat.labsticc_sensors_raw",
        columns=["temperature", "humidity", "accuracy"],
        hours_ranges=[(12,18), (18,24)],
        output_table="veloclimat.labsticc_sensor_stats_plages"
    )

    print("\n")

    stats_veloclimatmeter = compute_stats_multiple_hours(
        config_path="config.json",
        table_name="veloclimat.veloclimatmeter_meteo_raw",
        columns=["temperature", "humidite", "vitesse", "temperature_bot", "temperature_top"],
        hours_ranges=[(12,18), (18,24)],
        output_table="veloclimat.veloclimatmeter_meteo_stats_plages"
    )

    if stats_labsticc_sensor and stats_veloclimatmeter:
        print("\n✅ Analyse terminée avec succès !")
        exit(0)
    else:
        print("\n❌ L'analyse a rencontré des erreurs")
        exit(1)