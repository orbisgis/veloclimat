import re
from sqlalchemy import text

from process.utils import create_engine_from_config


# Script pour calculer l'Indice Biométéorologique (IBM)

# Config json file to connect to the database
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


def _is_subquery(input_table):
    """
    Détecte si input_table est un subquery (SELECT) ou une simple table

    Args:
        input_table: chaîne représentant la table ou subquery

    Returns:
        bool: True si c'est un subquery, False si c'est une table simple
    """
    return "select" in input_table.lower() or "(" in input_table


def _validate_input_table(input_table):
    """
    Valide le nom de table ou subquery

    Args:
        input_table: chaîne représentant la table ou subquery

    Returns:
        Tuple (valid: bool, error_message: str)
    """
    # Si c'est un subquery
    if _is_subquery(input_table):
        # Vérifier les mots-clés SQL basiques
        stripped = input_table.strip().lower()
        if not (stripped.startswith("select") or stripped.startswith("(")):
            return False, "Subquery doit commencer par SELECT ou ("
        if "temperature" not in input_table.lower():
            return False, "Subquery doit contenir une colonne 'temperature'"
        if "timestamp" not in input_table.lower():
            return False, "Subquery doit contenir une colonne 'timestamp'"
        return True, ""

    # Si c'est une table simple
    table_parts = input_table.split('.')
    for part in table_parts:
        if not part.isidentifier():
            return False, f"Nom de table invalide: {input_table}"

    return True, ""


def _clean_subquery(input_table):
    """
    supprime espaces inutiles, indentation

    Args:
        input_table: subquery brute

    Returns:
        str: subquery nettoyée
    """
    # Réduire les espaces multiples à un seul
    cleaned = re.sub(r'\s+', ' ', input_table.strip())
    return cleaned


def calculate_ibm(config_path, input_table, output_table=None):
    """
    Calcule l'Indice Biométéorologique (IBM) - moyenne glissante sur 3 jours

    Args:
        config_path: chemin vers le fichier config.json
        input_table: nom de la table source (ex: 'schema.table')
                    OU une subquery SELECT avec colonnes 'temperature' et 'timestamp'
                    ex: "(SELECT temperature, timestamp FROM table1 UNION ALL SELECT temperature, timestamp FROM table2) AS combined"
        output_table: nom de la table de sortie. Si None, utilise {input_table}_ibm ou ibm_result si subquery

    Returns:
        Tuple (success: bool, message: str)
    """


    # Valider input_table
    is_valid, error_msg = _validate_input_table(input_table)
    if not is_valid:
        return False, f"Erreur : {error_msg}"

    # Nettoyer input_table si c'est un subquery
    is_subquery = _is_subquery(input_table)
    if is_subquery:
        input_table = _clean_subquery(input_table)
        print(f"📋 Subquery détectée et nettoyée")
    else:
        print(f"📊 Table source détectée: {input_table}")

    # Déterminer la table de sortie
    if output_table is None:
        if is_subquery:
            output_table = "veloclimat.ibm_result"
        else:
            output_table = f"{input_table}_ibm"

    # Valider output_table
    output_parts = output_table.split('.')
    for part in output_parts:
        if not part.isidentifier():
            return False, f"Erreur : nom de table de sortie invalide: {output_table}"

    # Créer l'engine
    engine = create_engine_from_config(config_path)

    try:
        with engine.connect() as conn:
            # Tester la connexion
            conn.execute(text("SELECT 1"))
            print("✅ Connexion à PostgreSQL réussie !")

            # Drop table if exists
            print(f"🗑️  Suppression de la table {output_table} si elle existe...")
            conn.execute(text(f"DROP TABLE IF EXISTS {output_table}"))
            conn.commit()

            # Construire la source de données
            # Si c'est un subquery
            if is_subquery:
                # Vérifier si déjà entouré de parenthèses avec alias
                if input_table.strip().startswith("(") and " as " in input_table.lower():
                    source = input_table
                else:
                    # Extraire l'alias s'il existe
                    alias_match = re.search(r'\)\s+as\s+(\w+)', input_table, re.IGNORECASE)
                    if alias_match:
                        source = input_table
                    else:
                        # Ajouter les parenthèses et un alias générique
                        source = f"({input_table}) AS source_data"
            else:
                source = input_table

            # Requête SQL pour calculer l'IBM (moyenne glissante sur 3 jours)
            query = f"""
            CREATE TABLE {output_table} AS
            WITH daily_temps AS (
                -- Extraire les Tn (min) et Tx (max) par jour
                SELECT
                    DATE("timestamp") AS day,
                    MIN(temperature) AS tn,
                    MAX(temperature) AS tx
                FROM {source}
                GROUP BY DATE("timestamp")
            ),
            daily_mean AS (
                -- Calculer la moyenne (tn + tx)/2 pour chaque jour
                SELECT
                    day,
                    tn,
                    tx,
                    (tn + tx) / 2 AS daily_avg
                FROM daily_temps
            )
            -- Calculer la moyenne glissante sur 3 jours (IBM)
            SELECT
                day,
                tn,
                tx,
                ROUND(AVG(daily_avg) OVER (
                    ORDER BY day
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                )::numeric, 2) AS ibm
            FROM daily_mean
            ORDER BY day
            """

            print(f"📊 Calcul de l'IBM en cours...")
            conn.execute(text(query))
            conn.commit()
            print(f"✅ Indice Biométéorologique créé avec succès dans {output_table} !")

            # Afficher les stats
            result = conn.execute(text(f"""
                SELECT 
                    COUNT(*) as nb_jours,
                    ROUND(MIN(tn)::numeric, 2) as tn_min,
                    ROUND(MAX(tx)::numeric, 2) as tx_max,
                    ROUND(AVG(ibm)::numeric, 2) as ibm_moyen,
                    ROUND(MIN(ibm)::numeric, 2) as ibm_min,
                    ROUND(MAX(ibm)::numeric, 2) as ibm_max
                FROM {output_table}
            """))

            stats = result.mappings().fetchone()
            if stats:
                print("\n" + "=" * 70)
                print("📈 STATISTIQUES IBM")
                print("=" * 70)
                print(f"Nombre de jours: {stats['nb_jours']}")
                print(f"Température min (Tn): {stats['tn_min']}°C")
                print(f"Température max (Tx): {stats['tx_max']}°C")
                print(f"IBM moyen: {stats['ibm_moyen']}°C")
                print(f"IBM min: {stats['ibm_min']}°C")
                print(f"IBM max: {stats['ibm_max']}°C")
                print("=" * 70)

            return True, "Calcul IBM terminé avec succès"

    except Exception as e:
        return False, f"❌ Erreur SQL : {e}"

    finally:
        engine.dispose()


# Run
if __name__ == "__main__":

    #success, message = calculate_ibm(
    #    config_path="config.json",
    #    input_table="veloclimat.labsticc_sensor_preprocess",
    #    output_table="veloclimat.labsticc_sensor_preprocess_ibm"
    #)

    success, message = calculate_ibm(
        config_path="config.json",
        input_table="""(SELECT temperature, "timestamp" FROM veloclimat.labsticc_sensor_preprocess
                        UNION ALL
                        SELECT temperature, "timestamp" FROM veloclimat.veloclimatmeter_preprocess) AS combined_data""",
        output_table="veloclimat.ibm_combined"
    )

    print(message)
    exit(0 if success else 1)