import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

from process.utils import create_engine_from_config

# Configuration à la base de données
CONFIG_PATH = "config.json"
TABLE_NAME = "veloclimat.labsticc_sensors_temperature_interpolate"
THERMO_NAME = "Alençon"
DOSSIER_SORTIE = "/tmp/"


try:
    engine = create_engine_from_config(CONFIG_PATH)

    query = f"""
        SELECT "timestamp" ,
            temperature as sensor_t,
            t_inter as meteofrance_t
        FROM {TABLE_NAME} where thermo_name= '{THERMO_NAME}'
        ORDER BY "timestamp"
        """

    df = pd.read_sql(text(query), con=engine)
    engine.dispose()

    # Convertir timestamp en datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Grouper par minute et calculer la moyenne
    df_hourly = df.set_index("timestamp").resample("T").mean()

    # Interpoler les valeurs manquantes pour avoir des courbes continues
    df_hourly = df_hourly.interpolate(method="linear")

    # Créer le graphique
    fig, ax = plt.subplots(figsize=(14, 6))

    # Tracer les deux courbes
    ax.plot(df_hourly.index, df_hourly["sensor_t"],
            label="Capteur local", marker="o", linewidth=2, markersize=4, alpha=0.8)
    ax.plot(df_hourly.index, df_hourly["meteofrance_t"],
            label="Météo-France", marker="s", linewidth=2, markersize=4, alpha=0.8)

    # Formatage de l'axe des abscisses (dates)
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.xticks(rotation=45, ha="right")

    # Labels et titre
    ax.set_xlabel("Date et heure", fontsize=12, fontweight="bold")
    ax.set_ylabel("Température (°C)", fontsize=12, fontweight="bold")
    ax.set_title(f"Températures moyennes par heure - {THERMO_NAME}", fontsize=14, fontweight="bold")

    # Légende
    ax.legend(loc="best", fontsize=10)

    # Grille
    ax.grid(True, alpha=0.3)

    # Ajuster la mise en page
    plt.tight_layout()

    # Sauvegarder le fichier
    file_path = f"{DOSSIER_SORTIE}temperatures_moyennes_horaires_{THERMO_NAME}.png"
    plt.savefig(file_path, dpi=300, bbox_inches="tight")
    print(f"✅ Graphique sauvegardé : {file_path}")

    # Afficher le graphique
    plt.show()

except Exception as e:
    print(f"❌ Erreur lors de la récupération des données : {e}")
    exit(1)