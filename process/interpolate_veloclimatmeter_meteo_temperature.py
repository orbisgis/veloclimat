from sqlalchemy import text

from process.utils import create_engine_from_config

def prepare_MF_data(conn):
    """
    This script is used to interpolate temperature for each Veloclimatmeter location based on Météo-France stations.

    Input:
    - Cleaned Veloclimatmeter sensor data (veloclimat.veloclimatmeter_meteo_preprocess)
    - Triangulated weather stations (veloclimat.weather_stations_mf_delaunay)
    - Points of the Delaunay triangles with station IDs (veloclimat.weather_stations_mf_delaunay_pts)

    Output:
    - veloclimat.veloclimatmeter_temperature_interpolate: Interpolated temperature for each location based on Météo-France stations

    Args:
        conn: connexion SQLAlchemy
    """
    print("\n📊 Préparation des données...")

    query = """
            -- 1 For each veloclimatmeter location returns its triangle id and the triangle points
            -- So we can have the 3 MF stations for the locations            
            drop table if exists veloclimat.veloclimatmeter_meteo_delaunay_pts ;
            create table veloclimat.veloclimatmeter_meteo_delaunay_pts as
            select pts.id_pt , pts.the_geom as geom_pt_triangle, pts.id_triangle, pts.numer_insee, t.id_veloclimatmeter, t.the_geom as geom_pt_velo,
                   t.timestamp, t.elevation, t.temperature  from veloclimat.weather_stations_mf_delaunay_pts as pts, (
                select  b.id_triangle, a.id as id_veloclimatmeter, a.the_geom, a.elevation, a.temperature, a.timestamp  
                from veloclimat.veloclimatmeter_meteo_preprocess as a,
                veloclimat.weather_stations_mf_delaunay  as b where st_intersects(a.the_geom, b.the_geom)) as t
            where pts.id_triangle = t.id_triangle;

            create index on veloclimat.veloclimatmeter_meteo_delaunay_pts(numer_insee);
            create index on veloclimat.veloclimatmeter_meteo_delaunay_pts("timestamp");

            -- 2 Collect the weather station data for each veloclimatmeter location from the delaunay points
            -- Update the time position
            -- Note : delta_t, t_ground_0 must be computed before on weather_data_stations_mf
            drop table if exists veloclimat.veloclimatmeter_mf_stations_data;
            create table  veloclimat.veloclimatmeter_meteo_mf_stations_data
            as
            select b.t_ground_0,  EXTRACT(EPOCH from (a."timestamp" - b."date"))/360 as time_interp_weight,b.delta_t, a.*
            from veloclimat.veloclimatmeter_meteo_delaunay_pts as a , veloclimat.weather_data_stations_mf as b
            where a.numer_insee= b.numer_sta and b."date" > (a."timestamp"  - INTERVAL '6 Minutes')  and b."date" <= a."timestamp";
            
            create index on veloclimat.veloclimatmeter_meteo_mf_stations_data(id_triangle);
            create index on veloclimat.veloclimatmeter_meteo_mf_stations_data(id_veloclimatmeter);            
            """

    conn.execute(text(query))
    conn.commit()
    print("✅ Relation entre les stations triangulées et les points du veloclimatmeter réalisées avec succès !")

    query_interpolate = """
                        -- Create the final table that contains the mesured temperature 
                        -- and the interpolated temperature based on weather stations
                        drop table if exists veloclimat.veloclimatmeter_temperature_interpolate;
                        
                        create table veloclimat.veloclimatmeter_temperature_interpolate as

                        SELECT DISTINCT ON (id_veloclimatmeter)
                        id_veloclimatmeter as id,
                        "timestamp",
                        temperature,
                        t_inter ,
                        geom_pt_velo as the_geom,
                        id_triangle
                        from (
                        SELECT
                            ((st_z(st_intersection(triangles.polygon_t_ground_0, s.geom_pt_velo))
                                +st_z(st_intersection(triangles.polygon_delta_t, s.geom_pt_velo))*s.time_interp_weight)-0.0065*s.elevation) as t_inter,
                            s.id_triangle, s.geom_pt_velo , s.id_veloclimatmeter, s."timestamp", s.temperature
                        FROM (
                                 SELECT
                                     st_setsrid(ST_MakePolygon(
                                                        ST_MakeLine(
                                                                ARRAY_AGG(
                                                                        ST_MakePoint(ST_X(geom_pt_triangle), ST_Y(geom_pt_triangle), t_ground_0)
                                                                            ORDER BY id_pt DESC
                                                                )
                                                        )
                                                ), 4326) AS polygon_t_ground_0,
                                     st_setsrid(ST_MakePolygon(
                                                        ST_MakeLine(
                                                                ARRAY_AGG(
                                                                        ST_MakePoint(ST_X(geom_pt_triangle), ST_Y(geom_pt_triangle), delta_t)
                                                                            ORDER BY id_pt DESC
                                                                )
                                                        )
                                                ), 4326) AS polygon_delta_t,
                                     id_triangle,
                                     id_veloclimatmeter
                                 FROM veloclimat.veloclimatmeter_meteo_mf_stations_data
                                 GROUP BY id_triangle, id_veloclimatmeter
                             ) AS triangles
                                 JOIN veloclimat.veloclimatmeter_meteo_mf_stations_data s
                                      ON triangles.id_triangle = s.id_triangle
                                          AND triangles.id_veloclimatmeter = s.id_veloclimatmeter
                                          ) as foo ORDER BY id_veloclimatmeter, "timestamp" DESC;
                        
                        ALTER TABLE veloclimat.veloclimatmeter_temperature_interpolate ADD COLUMN DIFF_TEMPERATURE;
                        UPDATE veloclimat.veloclimatmeter_temperature_interpolate SET DIFF_TEMPERATURE = temperature - t_inter;
                        DROP TABLE IF EXISTS veloclimat.veloclimatmeter_meteo_mf_stations_data, veloclimat.veloclimatmeter_meteo_delaunay_pts;
                        """
    conn.execute(text(query_interpolate))
    conn.commit()


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