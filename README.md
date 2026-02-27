# VeloClimat

A set of scripts to analyse the VeloClimat sensor data


# VeloClimat - Database Structure

## Available Tables

| Table Name                              | Description                                                                                     |
|-----------------------------------------|-------------------------------------------------------------------------------------------------|
| `veloclimat.labsticc_sensors_raw`      | Raw temperature and humidity data collected with the "ThermoSensor"                              |
| `veloclimat.veloclimatmeter_meteo_raw` | Raw meteorological data collected with the VeloClimatmeter                                      |
| `veloclimat.veloclimatmeter_gyro_raw`  | Raw gyroscopic and accelerometer data collected with the VeloClimatmeter                        |
| `veloclimat.physio_records_raw`        | Raw physiological data collected with Core sensors and heart rate monitors                      |
| `veloclimat.weather_stations_mf`       | Location of Météo-France weather stations near the route                                        |
| `veloclimat.weather_data_stations_mf`  | Meteorological data from Météo-France weather stations (recorded every 6 minutes)              |

---

## Table: `veloclimat.labsticc_sensors_raw`

| Column         | Type               | Description                               |
|----------------|--------------------|-------------------------------------------|
| `id`           | int4               | Unique identifier                         |
| `timestamp`    | timestamptz        | Timestamp of the record                   |
| `temperature`  | float8             | Temperature                      |
| `humidity`     | float8             | Humidity                         |
| `the_geom`     | geometry(Point, 4326) | Geographic location (WGS84 coordinates)   |
| `accuracy`     | float8             | Measurement accuracy                      |
| `sensor_name`  | varchar            | Name of the sensor                        |
| `id_track`     | int4               | Track/route identifier                    |
| `thermo_name`  | varchar            | Name of the thermo party                  |
| `elevation`    | float8             | Elevation (in meters, extracted from a DEM) |

---

## Table: `veloclimat.veloclimatmeter_meteo_raw`

| Column                  | Type               | Description                             |
|-------------------------|--------------------|-----------------------------------------|
| `id`                    | int4               | Unique identifier                       |
| `timestamp`             | timestamptz        | Timestamp of the record                 |
| `id_track`              | int4               | Track/route identifier                  |
| `the_geom`              | geometry(Point, 4326) | Geographic location (WGS84 coordinates) |
| `altitude`              | float8             | Altitude (in meters)                    |
| `vitesse`               | float8             | Speed                                   |
| `direction`             | float8             | Direction                               |
| `temperature`           | float8             | Temperature                             |
| `humidite`              | float8             | Humidity                                |
| `pression`              | float8             | Pressure                                |
| `temperature_bot`       | float8             | Bottom temperature                      |
| `temperature_top`       | float8             | Top temperature                         |
| `pm_1_ug_m3`            | float8             | PM1 concentration (µg/m³)               |
| `pm_2_5_ug_m3`          | float8             | PM2.5 concentration (µg/m³)             |
| `pm_10_ug_m3`           | float8             | PM10 concentration (µg/m³)              |
| `niveau_sonore_db_a`    | float8             | Noise level (dB A)                      |
| `distancegauche`        | float8             | Distance to the left                    |
| `distancedroite`        | float8             | Distance to the right                   |
| `thermo_name`           | varchar            | Name of the thermo party                 |
| `sensor_name`           | varchar            | Name of the sensor                      |
| `elevation`             | float8             | Elevation (in meters,extracted from a DEM)                  |

---

## Table: `veloclimat.veloclimatmeter_gyro_raw`

| Column              | Type               | Description                                      |
|---------------------|--------------------|--------------------------------------------------|
| `id`                | int4               | Unique identifier                                |
| `timestamp`         | timestamp          | Timestamp of the record                          |
| `id_track`          | int4               | Track/route identifier                           |
| `moy_gyro_x`        | float8             | Average gyroscope X-axis value                   |
| `moy_gyro_y`        | float8             | Average gyroscope Y-axis value                   |
| `moy_gyro_z`        | float8             | Average gyroscope Z-axis value                   |
| `min_gyro_x`        | float8             | Minimum gyroscope X-axis value                   |
| `min_gyro_y`        | float8             | Minimum gyroscope Y-axis value                   |
| `min_gyro_z`        | float8             | Minimum gyroscope Z-axis value                   |
| `max_gyro_x`        | float8             | Maximum gyroscope X-axis value                   |
| `max_gyro_y`        | float8             | Maximum gyroscope Y-axis value                   |
| `max_gyro_z`        | float8             | Maximum gyroscope Z-axis value                   |
| `moy_accel_x`       | float8             | Average accelerometer X-axis value               |
| `moy_accel_y`       | float8             | Average accelerometer Y-axis value               |
| `moy_accel_z`       | float8             | Average accelerometer Z-axis value               |
| `min_accel_x`       | float8             | Minimum accelerometer X-axis value               |
| `min_accel_y`       | float8             | Minimum accelerometer Y-axis value               |
| `min_accel_z`       | float8             | Minimum accelerometer Z-axis value               |
| `max_accel_x`       | float8             | Maximum accelerometer X-axis value               |
| `max_accel_y`       | float8             | Maximum accelerometer Y-axis value               |
| `max_accel_z`       | float8             | Maximum accelerometer Z-axis value               |
| `thermo_name`       | varchar            | Name of the thermo party                         |
| `sensor_name`       | varchar            | Name of the sensor                               |

---

## Table: `veloclimat.physio_records_raw`

| Column                     | Type               | Description                              |
|----------------------------|--------------------|------------------------------------------|
| `id`                       | int4               | Unique identifier            |
| `the_geom`                 | geometry(Point, 4326) | Geographic location (WGS84 coordinates) |
| `rider`                    | varchar            | Rider identifier                         |
| `enhanced_speed`           | float8             | Enhanced speed                            |
| `enhanced_altitude`        | float8             | Enhanced altitude                        |
| `heart_rate`               | int4               | Heart rate                               |
| `core_temperature`         | float8             | Core temperature                         |
| `skin_temperature`         | float8             | Skin temperature                         |
| `core_data_quality`        | int4               | Core data quality                        |
| `core_reserved`            | int4               | Reserved field                           |
| `heat_strain_index`        | float8             | Heat strain index                        |
| `total_hemoglobin_conc`    | int4               | Total hemoglobin concentration            |
| `CIQ_core_temperature`    | float8             | CIQ core temperature                     |
| `temperature`              | int4               | Temperature                              |
| `saturated_hemoglobin_percent` | int4          | Saturated hemoglobin percentage          |
| `CIQ_skin_temperature`    | float8             | CIQ skin temperature                     |
| `time_date`                | varchar(50)        | Time and date                            |

---

## Table: `veloclimat.weather_stations_mf`

| Column         | Type               | Description                                                          |
|----------------|--------------------|----------------------------------------------------------------------|
| `the_geom`     | geometry(geometry, 4326) | Geographic location (WGS84 coordinates)                              |
| `numer_insee`  | int4               | INSEE number                                                         |
| `nom_station`  | varchar(50)        | Name of the weather station                                          |
| `elevation`    | float8             | Elevation (in meters)                                                |
| `id`           | serial4            | Unique identifier (auto-incremented)                                 |
| `zone`         | int4               | Zone identifier used to qualify two areas (before and after Alençon) |

---

## Table: `veloclimat.weather_data_stations_mf`

| Column          | Type               | Description                     |
|-----------------|--------------------|---------------------------------|
| `delta_t`       | float4             | Time delta                      |
| `date`          | timestamptz        | Timestamp of the record         |
| `numer_sta`     | int8               | Station number  (INSEE number ) |
| `dd`            | int4               | Wind direction                  |
| `ff`            | float4             | Wind speed                      |
| `t`             | float4             | Temperature                     |
| `u`             | int4               | Relative humidity               |
| `ray_glo01`     | int4               | Global radiation                |
| `elevation`     | float8             | Elevation (in meters)           |
| `t_ground_0`    | float8             | Ground temperature              |


# Scripts

## preprocess_data_sensors.py

This Python script cleans and preprocesses raw sensor data stored in a PostgreSQL database using SQLAlchemy. 
It prepares the data for further analysis by ensuring it is clean, consistent, and well-structured.

3 tables are save :

- veloclimatmeter_meteo_preprocess
- labsticc_sensor_preprocess
- labsticc_sensor_reference_preprocess

### Main Features

- **Data Cleaning:** Removes duplicates, filters by time and accuracy, and excludes specific entries.
- **Aggregation:** Aggregates data by second for consistent time intervals.
- **Speed Calculation:** Computes speeds between consecutive points and applies a sliding window for smoothing.
- **Unique Identifiers:** Generates unique identifiers for tracking.
- **Indexing:** Adds indexes for efficient querying.


## prepare_weather_stations_delaunay.py

This script prepares Météo-France weather station data.

Inputs:
- The name of the station table: veloclimat.weather_stations_mf
- The name of the weather data stations : weather_data_stations_mf

Each Météo-France station is connected to two other stations through Delaunay triangulation. 
The triangles are used to perform linear interpolation of values from the Météo-France data 
stored in the veloclimat.weather_data_stations_mf table.

Output :
- veloclimat.weather_stations_mf_delaunay that contains the delaunay triangles
- veloclimat.weather_stations_mf_delaunay_pts delaunay points with the station identifier (numer_insee/numer_stat)

## interpolate_veloclimatmeter_meteo_temperature.py

This script is used to interpolate temperature for each Veloclimatmeter location based on Météo-France stations.

Inputs

- **Cleaned Veloclimatmeter sensor data**: `veloclimat.veloclimatmeter_meteo_preprocess`
- **Triangulated weather stations**: `veloclimat.weather_stations_mf_delaunay`
- **Points of the Delaunay triangles with station IDs**: `veloclimat.weather_stations_mf_delaunay_pts`

Output

- **Interpolated temperature data**: `veloclimat.veloclimatmeter_temperature_interpolate`
  Interpolated temperature for each location based on Météo-France stations


