from sqlalchemy import text

from process.utils import create_engine_from_config

def prepare_MF_data(conn):
    """
    Prepare Météo-France weather station data.

    Input:
    - The name of the station table: veloclimat.weather_stations_mf
    - The name of the weather data stations : weather_data_stations_mf

    Each Météo-France station is connected to two other stations through Delaunay triangulation.
    The triangles are used to perform linear interpolation of values from the Météo-France data
    stored in the veloclimat.weather_data_stations_mf table.

    Output :
    - veloclimat.weather_stations_mf_delaunay that contains the delaunay triangles
    - veloclimat.weather_stations_mf_delaunay_pts delaunay points with the station identifier (numer_insee/numer_stat)

    Args:
        conn: SQLAlchemy connection
    """
    print("\n📊 Start delaunay triangulation...")

    query = """
            -- 1 Triangulate the weather stations in order to interpolate the veloclimaeter location
            drop table if exists veloclimat.weather_stations_mf_delaunay;

            create table veloclimat.weather_stations_mf_delaunay as
                
            SELECT (gdump).geom As the_geom,  (gdump).path[1] as id_triangle
            FROM ( SELECT ST_Dump(ST_DelaunayTriangles(ST_Collect(the_geom))) As gdump
            FROM veloclimat.weather_stations_mf) As foo;
                
            --2 Explode the triangles to extract their vertexes
            drop table if exists veloclimat.weather_stations_mf_delaunay_pts;

            create table veloclimat.weather_stations_mf_delaunay_pts as

            SELECT (gdump).geom As the_geom,  (gdump).path[2] as id_pt,  id_triangle
            FROM ( SELECT ST_DumpPoints(the_geom) As gdump, id_triangle
            FROM veloclimat.weather_stations_mf_delaunay) As foo;


            --3. Set the identifier of the weather stations to each vertexes of the triangulation
            create index on veloclimat.weather_stations_mf_delaunay_pts using GIST(THE_GEOM);

            alter table veloclimat.weather_stations_mf_delaunay_pts add column numer_insee integer;

            update veloclimat.weather_stations_mf_delaunay_pts as a set numer_insee = b.numer_insee
                from veloclimat.weather_stations_mf as b where st_intersects(a.the_geom, b.the_geom);

            create index on veloclimat.weather_stations_mf_delaunay using GIST(THE_GEOM);
            """

    conn.execute(text(query))
    conn.commit()
    print("✅ Tables de triangulation créées avec succès !")

def main():
      # Créer l'engine
    engine = create_engine_from_config("config.json")

    try:
        with engine.connect() as conn:
            # Tester la connexion
            conn.execute(text("SELECT 1"))
            print("✅ Connexion à PostgreSQL réussie !")

            # Prépare les données
            prepare_MF_data(conn)

            print("\n" + "=" * 70)
            print("✅ Préparation de stations météo terminée avec succès !")
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