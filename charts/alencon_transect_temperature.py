import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib.colors import ListedColormap, BoundaryNorm
from process.utils import create_engine_from_config

# Script to create the chart for Alençon transect

# Configuration de la base de données
CONFIG_PATH = "config.json"
TABLE_NAME = "veloclimat.labsticc_sensors_temperature_lcz"
THERMO_NAME = "Alençon - Matthieu"
DOSSIER_SORTIE = "/tmp/"

# Définition des seuils et couleurs pour le transect
seuils = [-10, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4]
couleurs = [
    "#30123b",  # < -5
    "#455bcd",  # -5
    "#3e9cfe",  # -4
    "#18d7cb",  # -3
    "#48f882",  # -2
    "#a4fc3c",  # -1
    "#fea331",  # +1
    "#ef5911",  # +2
    "#c22403",  # +3
    "#7a0403",  # > 3
]

# Création de la colormap discrète
cmap = ListedColormap(couleurs)
norm = BoundaryNorm(seuils, cmap.N)

# Timestamp de séparation (en Europe/Paris)
separation_timestamp = pd.to_datetime("2025-06-30 23:09:08.000").tz_localize('Europe/Paris')

try:
    # Connexion à la base de données
    engine = create_engine_from_config(CONFIG_PATH)

    # Requête SQL
    query = f"""
        SELECT "timestamp",
               temperature,
               diff_temperature,
               elevation,
               id
        FROM {TABLE_NAME}
        WHERE thermo_name = '{THERMO_NAME}'
        ORDER BY "timestamp"
    """

    # Chargement des données
    df = pd.read_sql(text(query), con=engine)
    engine.dispose()

    # Vérification des données
    if df.empty:
        print("❌ Aucune donnée trouvée.")
        exit(1)

    # Suppression des valeurs manquantes
    df = df.dropna(subset=['timestamp', 'temperature', 'diff_temperature', 'elevation'])

    # Conversion des timestamps en Europe/Paris si nécessaire
    if df['timestamp'].dtype == 'datetime64[ns, UTC]':
        df['timestamp'] = df['timestamp'].dt.tz_convert('Europe/Paris')
    elif df['timestamp'].dtype == 'datetime64[ns]':
        df['timestamp'] = df['timestamp'].dt.tz_localize('Europe/Paris')

    # Création de la figure
    fig = plt.figure(figsize=(16, 9))

    # Ajout d'un axe principal pour le graphique
    ax1 = fig.add_subplot(1, 1, 1)

    # Tracé de la courbe d'élévation (sans marqueurs)
    ax1.plot(
        df['timestamp'],
        df['elevation'],
        color='gray',
        alpha=0.7,
        linestyle='-',
        linewidth=1.5,
        zorder=2,
        label='Altitude'
    )

    # Tracé des marqueurs pour la température, colorés par diff_temperature (style QGIS)
    scatter = ax1.scatter(
        df['timestamp'],
        df['temperature'],
        c=df['diff_temperature'],
        cmap=cmap,
        norm=norm,
        s=50,
        edgecolor='none',
        alpha=0.8,
        label='Température',
        zorder=4
    )

    # Remplissage de l'espace sous la courbe des températures en deux parties (sans bordures et sans espace blanc)
    # Partie 1 : gris jusqu'au timestamp de séparation
    mask_before = df['timestamp'] <= separation_timestamp
    if mask_before.any():
        ax1.fill_between(
            df['timestamp'][mask_before],
            df['temperature'][mask_before],
            alpha=0.2,
            color='gray',
            zorder=1,
            edgecolor='none',
            interpolate=True,
            label='_nolegend_'
        )
        # Ajout du texte "Ville" sous la courbe dans la zone grise
        ax1.text(
            df['timestamp'][mask_before].iloc[len(df['timestamp'][mask_before]) // 2],
            df['temperature'][mask_before].min() - 0.8,
            'Ville',
            fontsize=12,
            color='black',
            ha='center',
            va='top',
            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none')
        )

    # Partie 2 : vert après le timestamp de séparation
    mask_after = df['timestamp'] >= separation_timestamp
    if mask_after.any():
        ax1.fill_between(
            df['timestamp'][mask_after],
            df['temperature'][mask_after],
            alpha=0.2,
            color='green',
            zorder=1,
            edgecolor='none',
            interpolate=True,
            label='_nolegend_'
        )
        # Ajout du texte "Campagne" sous la courbe dans la zone verte
        ax1.text(
            df['timestamp'][mask_after].iloc[len(df['timestamp'][mask_after]) // 2],
            df['temperature'][mask_after].min() - 0.8,
            'Campagne',
            fontsize=12,
            color='black',
            ha='center',
            va='top',
            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none')
        )

    # Configuration des échelles des axes
    ax1.set_ylim(df['temperature'].min() - 2, df['temperature'].max() + 2)
    ax1.set_xlim(df['timestamp'].min(), df['timestamp'].max())

    # Configuration du premier axe Y (température)
    ax1.set_ylabel("Température (°C)", fontsize=12, color='black')
    ax1.tick_params(axis='y', labelcolor='black')

    # Formatage de l'axe X pour afficher les heures +2 et en horizontal
    def format_utc2_plus_2(x, pos=None):
        timestamp = mdates.num2date(x)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=pd.Timestamp("2020-01-01").tz_localize('Europe/Paris').tzinfo)
        time_plus_2 = (timestamp + pd.Timedelta(hours=2)).strftime('%H:%M')
        return time_plus_2

    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(format_utc2_plus_2))
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.setp(ax1.get_xticklabels(), rotation=0, ha='center', fontsize=9)

    # Création du second axe Y (pour l'élévation)
    ax2 = ax1.twinx()
    ax2.plot(
        df['timestamp'],
        df['elevation'],
        color='gray',
        linestyle='-',
        linewidth=1.5,
        zorder=3
    )
    ax2.set_ylabel("Altitude (m)", fontsize=12, color='gray')
    ax2.tick_params(axis='y', labelcolor='gray')

    # Grille
    ax1.grid(True, linestyle=':', alpha=0.3, zorder=0)

    # Ajustement de la position du graphique principal pour laisser de la place en bas
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + 0.05, box.width, box.height * 0.85])

    ax1.text(df['timestamp'].min(), ax1.get_ylim()[0], 'Départ',
             fontsize=11, ha='left', va='bottom', color='black')

    ax1.text(df['timestamp'].max(), ax1.get_ylim()[0], 'Arrivée',
         fontsize=11, ha='right', va='bottom', color='black')

    # Ajout d'un titre pour la barre de couleur à gauche
    fig.text(box.x0, box.y0, 'Ecart de température (°C)', ha='left', fontsize=10)

    # Ajout de l'axe pour la barre de couleur sous l'axe X
    cax = fig.add_axes([box.x0, box.y0 - 0.05, box.width, 0.03])
    cbar = fig.colorbar(scatter, cax=cax, orientation='horizontal', ticks=seuils[:-1])
    cbar.ax.set_xticklabels([
        '< -5', '-5', '-4', '-3', '-2', '-1',
        '+1', '+2', '+3', '> +3'
    ], rotation=0, ha='center')

    # Titre ajusté pour éviter le chevauchement, marche pas sniff
    #plt.suptitle(f"Évolution de la température et de l'élévation pour {THERMO_NAME}\n(Coloration des marqueurs : différence de température, style QGIS)",
    #             fontsize=14, y=0.95)

    # Sauvegarde du graphique
    fichier_sortie_png = f"{DOSSIER_SORTIE}transect_temperature_elevation_diff_{THERMO_NAME}.png"
    plt.savefig(fichier_sortie_png, dpi=300, bbox_inches='tight', transparent=False)

    print(f"✅ Graphique sauvegardé : {fichier_sortie_png}")

    # Affichage (optionnel)
    plt.show()

except Exception as e:
    print(f"❌ Erreur : {e}")
    exit(1)
