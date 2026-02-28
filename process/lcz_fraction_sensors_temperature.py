from sqlalchemy import text

from process.utils import create_engine_from_config

def lcz_fraction(
        conn,
        source_table,
        output_table,
        lcz_table,
        columns,
        buffer_size=100,
        delete_source=False
):
    """
    This script is used to compute LCZ fractions around sensor locations based on a buffer.

    Input:
    - source_table: Sensor locations with temperature data
    - lcz_table: LCZ polygons table
    - buffer size to compute the fractions

    Output:
    - output_table: LCZ fractions for each sensor location with geometry preserved

    Args:
        conn: connexion SQLAlchemy
        source_table: Table name containing sensor data (REQUIRED)
        output_table: Output table name (REQUIRED)
        lcz_table: LCZ polygons table name (REQUIRED)
        columns: List of columns to keep from source_table (id and the_geom are always included) (REQUIRED)
                 Example: ["temperature", "t_inter", "timestamp"]
        buffer_size: Buffer size in meters (default: 100)
        delete_source: Delete source table after processing (default: False)
    """
    # Validation des paramètres obligatoires
    if not source_table:
        print("❌ Erreur : source_table est obligatoire")
        return False

    if not output_table:
        print("❌ Erreur : output_table est obligatoire")
        return False

    if not lcz_table:
        print("❌ Erreur : lcz_table est obligatoire")
        return False

    if not columns or len(columns) == 0:
        print("❌ Erreur : columns est obligatoire et ne peut pas être vide")
        return False

    print("\n📊 Préparation des données...")

    # Generate dynamic index names based on source table
    # Extract schema and table name
    source_parts = source_table.split('.')
    source_table_clean = source_parts[-1]  # Get table name without schema

    lcz_parts = lcz_table.split('.')
    lcz_table_clean = lcz_parts[-1]

    idx_source_geom = f"idx_{source_table_clean}_geom_3857"
    idx_lcz_geom = f"idx_{lcz_table_clean}_geom_3857"
    idx_output_point = f"idx_{source_table_clean}_lcz_fractions_point_id"

    # Build the columns list
    select_columns = ", ".join(columns)
    select_columns_b = ", ".join([f"b.{col}" for col in columns])
    output_columns = ", ".join([f"MAX({col}) AS {col}" for col in columns])

    query = f"""
            -- INDEX SPATIAUX (à exécuter une fois)
            CREATE INDEX IF NOT EXISTS {idx_source_geom}
                ON {source_table}
                USING GIST(ST_Transform(the_geom, 3857));

            CREATE INDEX IF NOT EXISTS {idx_lcz_geom}
                ON {lcz_table}
                USING GIST(ST_Transform(the_geom, 3857));

            DROP TABLE IF EXISTS {output_table};

            CREATE TABLE {output_table} AS
            WITH buffers AS (
                SELECT
                    id,
                    the_geom,
                    {select_columns},
                    ST_Buffer(ST_Transform(the_geom, 3857), {buffer_size}) AS buffer_geom
                FROM {source_table}
            ),
                 lcz_3857 AS (
                     SELECT
                         lcz_primary,
                         ST_Transform(the_geom, 3857) AS geom_3857
                     FROM {lcz_table}
                 ),
                 lcz_intersections AS (
                     SELECT
                         b.id AS point_id,
                         b.the_geom,
                         {select_columns_b},
                         r.lcz_primary,
                         ST_Area(ST_Intersection(b.buffer_geom, r.geom_3857)) / ST_Area(b.buffer_geom) AS lcz_fraction
                     FROM buffers b
                              JOIN lcz_3857 r ON ST_Intersects(b.buffer_geom, r.geom_3857)
                 ),
                 lcz_aggregated AS (
                     SELECT
                         point_id,
                         the_geom,
                         {select_columns},
                         lcz_primary,
                         SUM(lcz_fraction) AS lcz_fraction_sum
                     FROM lcz_intersections
                     GROUP BY point_id, the_geom, {select_columns}, lcz_primary
                 ),
                 lcz_with_rank AS (
                     SELECT
                         point_id,
                         the_geom,
                         {select_columns},
                         lcz_primary,
                         lcz_fraction_sum,
                         ROW_NUMBER() OVER (PARTITION BY point_id ORDER BY lcz_fraction_sum DESC) AS rn
                     FROM lcz_aggregated
                 )
            SELECT
                point_id AS id,
                the_geom,
                {output_columns},
                MAX(CASE WHEN rn = 1 THEN lcz_primary END) AS lcz_primary_max,
                MAX(CASE WHEN rn = 2 THEN lcz_primary END) AS lcz_primary_max_2,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 1), 0) AS lcz_1,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 2), 0) AS lcz_2,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 3), 0) AS lcz_3,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 4), 0) AS lcz_4,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 5), 0) AS lcz_5,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 6), 0) AS lcz_6,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 7), 0) AS lcz_7,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 8), 0) AS lcz_8,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 9), 0) AS lcz_9,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 10), 0) AS lcz_10,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 101), 0) AS lcz_101,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 102), 0) AS lcz_102,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 103), 0) AS lcz_103,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 104), 0) AS lcz_104,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 105), 0) AS lcz_105,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 106), 0) AS lcz_106,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 107), 0) AS lcz_107,
                -- Create LCZ group
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 105)), 0) AS lcz_urban,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary IN (101, 102, 103, 104)), 0) AS lcz_vegetation,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 106), 0) AS lcz_bare,
                COALESCE(SUM(lcz_fraction_sum) FILTER (WHERE lcz_primary = 107), 0) AS lcz_water
            FROM lcz_with_rank
            GROUP BY point_id, the_geom;
            
            CREATE INDEX {idx_output_point} ON {output_table}(id);
            
            ANALYZE {output_table};
            """

    try:
        conn.execute(text(query))
        conn.commit()
        print(f"✅ Fractions de LCZ calculées avec succès !")

        # Suppression de la table source si demandé
        if delete_source:
            print(f"\n🗑️ Suppression de la table source: {source_table}...")
            drop_query = f"DROP TABLE IF EXISTS {source_table};"
            conn.execute(text(drop_query))
            conn.commit()
            print(f"✅ Table source supprimée avec succès !")

        return True
    except Exception as e:
        print(f"❌ Erreur SQL : {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    # Créer l'engine
    engine = create_engine_from_config("config.json")

    try:
        with engine.connect() as conn:
            # Tester la connexion
            conn.execute(text("SELECT 1"))
            print("✅ Connexion à PostgreSQL réussie !")

            # Paramètres obligatoires
            source_table = "veloclimat.labsticc_sensors_temperature_interpolate"
            output_table = "veloclimat.labsticc_sensors_temperature_lcz"
            lcz_table = "veloclimat.rsu_lcz"
            columns_to_keep = ["temperature", "t_inter", "timestamp", "diff_temperature"]

            success = lcz_fraction(
                conn,
                source_table=source_table,
                output_table=output_table,
                lcz_table=lcz_table,
                columns=columns_to_keep,
                buffer_size=100,
                delete_source=False
            )

            if success:
                print("\n" + "=" * 70)
                print("✅ Traitement terminé avec succès !")
                print("=" * 70)
                return True
            else:
                return False

    except Exception as e:
        print(f"❌ Erreur : {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        engine.dispose()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)