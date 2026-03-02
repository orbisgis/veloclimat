import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.transforms import blended_transform_factory
import requests

# ============================================================
# PARAMÈTRES
# ============================================================
VILLE           = "Paimpol"
DATE_DEBUT      = "1978-01-01"
DATE_FIN        = "2025-09-30"   # ERA5 : délai de publication ~5 mois
MOIS_FILTRES    = [6, 7, 8, 9]   # juin, juillet, août, septembre
HEURE_DEBUT     = 0              # inclus
HEURE_FIN       = 24              # inclus
SEUIL_CHAUD     = 29.0            # seuil en °C
MIN_CONSECUTIFS = 1               # nombre minimum de jours consécutifs chauds
FENETRE_MOBILE  = 3               # taille de la moyenne mobile en années
AFFICHER_MEDIANE = False            # afficher la médiane mobile
DOSSIER_SORTIE  = "/tmp/"         # ← à modifier

# Option pour dupliquer automatiquement les années de début et de fin
# Lissage des moyennes
DUPLIQUER_DATES = True

MOIS_FR = {
    1: 'Jan', 2: 'Fév', 3: 'Mar', 4: 'Avr',
    5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Aoû',
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Déc'
}

def fetch_coordinates(ville):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": ville, "format": "json", "limit": 1}
    headers = {"User-Agent": "temperature-bubbles-app/1.0"}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200 and response.json():
        result = response.json()[0]
        lat = float(result['lat'])
        lon = float(result['lon'])
        print(f"Ville trouvée : {result['display_name']}")
        print(f"Coordonnées  : {lat:.4f}, {lon:.4f}")
        return lat, lon
    else:
        print(f"Erreur Nominatim : ville '{ville}' introuvable.")
        exit()

def fetch_hourly_chunk(date_debut, date_fin, latitude, longitude):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   latitude,
        "longitude":  longitude,
        "start_date": date_debut,
        "end_date":   date_fin,
        "hourly":     "temperature_2m",
        "timezone":   "auto"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['hourly']
        return pd.DataFrame({
            'datetime': pd.to_datetime(data['time']),
            'temp':     data['temperature_2m']
        })
    else:
        print(f"  Erreur API : {response.status_code} - {response.text}")
        return pd.DataFrame()

def fetch_all_hourly(date_debut, date_fin, latitude, longitude):
    all_chunks = []
    start = pd.to_datetime(date_debut)
    end   = pd.to_datetime(date_fin) + pd.Timedelta(hours=23)

    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + pd.DateOffset(years=2) - pd.Timedelta(hours=1), end)
        print(f"  Récupération : {chunk_start.date()} → {chunk_end.date()}")
        chunk_df = fetch_hourly_chunk(
            chunk_start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
            latitude, longitude
        )
        if not chunk_df.empty:
            all_chunks.append(chunk_df)
        chunk_start = chunk_end + pd.Timedelta(hours=1)

    df = pd.concat(all_chunks, ignore_index=True)
    print(f"\nDonnées disponibles : {df['datetime'].min().date()} → {df['datetime'].max().date()}")
    return df

def count_consecutive_hot_days(filtered, seuil, min_consecutifs):
    """Compte les jours appartenant à une séquence d'au moins min_consecutifs jours chauds."""
    jours_par_annee = (
        filtered[filtered['temp'] > seuil]
        .groupby('year')['date']
        .apply(lambda dates: sorted(set(dates)))
        .reset_index()
        .rename(columns={'date': 'dates_chaudes'})
    )

    result = []
    for _, row in jours_par_annee.iterrows():
        dates = [pd.Timestamp(d) for d in row['dates_chaudes']]
        count = 0
        i = 0
        while i < len(dates):
            j = i
            while j + 1 < len(dates) and (dates[j + 1] - dates[j]).days == 1:
                j += 1
            longueur = j - i + 1
            if longueur >= min_consecutifs:
                count += longueur
            i = j + 1
        result.append({'year': row['year'], 'jours_chauds': count})

    return pd.DataFrame(result)

def filter_and_aggregate_yearly(df, mois_filtres, heure_debut, heure_fin, seuil, min_consecutifs):
    """Filtre sur les mois et la plage horaire, agrège par année + compte les jours chauds consécutifs."""
    mask = (
            df['datetime'].dt.month.isin(mois_filtres) &
            (df['datetime'].dt.hour >= heure_debut) &
            (df['datetime'].dt.hour <= heure_fin)
    )
    filtered = df[mask].copy()
    filtered['year'] = filtered['datetime'].dt.year
    filtered['date'] = filtered['datetime'].dt.date

    yearly = filtered.groupby('year').agg(
        temp_mean=('temp', 'mean'),
        temp_max=('temp',  'max'),
        temp_min=('temp',  'min'),
    ).reset_index()

    jours_chauds = count_consecutive_hot_days(filtered, seuil, min_consecutifs)
    yearly = yearly.merge(jours_chauds, on='year', how='left')
    yearly['jours_chauds'] = yearly['jours_chauds'].fillna(0).astype(int)
    return yearly

def dupliquer_annees_debut_fin(df, date_debut, date_fin, activer_duplication):
    """Duplique les données des années de début et de fin si activé."""
    if not activer_duplication:
        return df

    annee_debut = pd.to_datetime(date_debut).year
    annee_fin   = pd.to_datetime(date_fin).year

    # Dupliquer l'année de début (si elle existe)
    if annee_debut in df['year'].values:
        ligne_debut = df[df['year'] == annee_debut].copy()
        ligne_debut_moins_un = ligne_debut.replace({annee_debut: annee_debut - 1})
        df = pd.concat([df, ligne_debut_moins_un], ignore_index=True)

    # Dupliquer l'année de fin (si elle existe)
    if annee_fin in df['year'].values:
        ligne_fin = df[df['year'] == annee_fin].copy()
        ligne_fin_plus_un = ligne_fin.replace({annee_fin: annee_fin + 1})
        df = pd.concat([df, ligne_fin_plus_un], ignore_index=True)

    # Trier et réinitialiser l'index
    df = df.sort_values(by='year').reset_index(drop=True)

    if annee_debut in df['year'].values:
        print(f"\nDonnées de {annee_debut} dupliquées pour {annee_debut - 1}.")
    if annee_fin in df['year'].values:
        print(f"Données de {annee_fin} dupliquées pour {annee_fin + 1}.")

    return df

# --- Géocodage ---
LATITUDE, LONGITUDE = fetch_coordinates(VILLE)

# --- Récupération ---
print(f"\nRécupération des données horaires ({DATE_DEBUT} → {DATE_FIN})...")
df_hourly = fetch_all_hourly(DATE_DEBUT, DATE_FIN, LATITUDE, LONGITUDE)

# --- Filtrage et agrégation ---
mois_label = ', '.join(MOIS_FR[m] for m in MOIS_FILTRES)
print(f"\nFiltrage : mois [{mois_label}]  ·  {HEURE_DEBUT:02d}h00 – {HEURE_FIN:02d}h00  ·  seuil {SEUIL_CHAUD}°C  ·  {MIN_CONSECUTIFS} jours consécutifs min.")
df_yearly = filter_and_aggregate_yearly(df_hourly, MOIS_FILTRES, HEURE_DEBUT, HEURE_FIN, SEUIL_CHAUD, MIN_CONSECUTIFS)

# --- Dupliquer les années de début et de fin si nécessaire ---
df_yearly = dupliquer_annees_debut_fin(df_yearly, DATE_DEBUT, DATE_FIN, DUPLIQUER_DATES)

# Ajout des colonnes de moyennes mobiles
label_mobile = f'moy. mobile {FENETRE_MOBILE} ans'
df_yearly['roll_mean'] = df_yearly['temp_mean'].rolling(FENETRE_MOBILE, center=True).mean()
df_yearly['roll_max'] = df_yearly['temp_max'].rolling(FENETRE_MOBILE, center=True).mean()
df_yearly['roll_min'] = df_yearly['temp_min'].rolling(FENETRE_MOBILE, center=True).mean()
if AFFICHER_MEDIANE:
    df_yearly['roll_median'] = df_yearly['temp_mean'].rolling(FENETRE_MOBILE, center=True).median()

# --- Filtrer pour afficher uniquement les données dans l'intervalle DATE_DEBUT–DATE_FIN ---
annee_debut = pd.to_datetime(DATE_DEBUT).year
annee_fin   = pd.to_datetime(DATE_FIN).year
df_yearly_filtre = df_yearly[(df_yearly['year'] >= annee_debut) & (df_yearly['year'] <= annee_fin)].copy()

# Utiliser df_yearly_filtre pour l'affichage
annee_fin_reelle = df_yearly_filtre['year'].max()

print(f"\n{len(df_yearly_filtre)} années affichées ({df_yearly_filtre['year'].min()} → {annee_fin_reelle}) :")
print(df_yearly_filtre.to_string(index=False))

# --- Graphique ---
fig, ax = plt.subplots(figsize=(20, 9), facecolor='#0a0e1a')
ax.set_facecolor('#0a0e1a')

# Utiliser les colonnes de moyennes mobiles de df_yearly_filtre
years_graph = df_yearly_filtre['year']
roll_mean_graph = df_yearly_filtre['roll_mean']
roll_max_graph = df_yearly_filtre['roll_max']
roll_min_graph = df_yearly_filtre['roll_min']

ax.plot(years_graph, roll_mean_graph,
        label=f'Moyenne ({label_mobile})', color='#f0c040',
        linewidth=2, zorder=3, marker='o', markersize=4)
if AFFICHER_MEDIANE:
    roll_median_graph = df_yearly_filtre['roll_median']
    ax.plot(years_graph, roll_median_graph,
            label=f'Médiane ({label_mobile})', color='#00ff99',
            linewidth=2, zorder=3, marker='s', markersize=4, linestyle='-', alpha=0.75)
ax.plot(years_graph, roll_max_graph,
        label=f'Maximum ({label_mobile})', color='#ff5566',
        linewidth=1.2, linestyle='--', alpha=0.85, zorder=2, marker='^', markersize=4)
ax.plot(years_graph, roll_min_graph,
        label=f'Minimum ({label_mobile})', color='#00c8ff',
        linewidth=1.2, linestyle='--', alpha=0.85, zorder=2, marker='v', markersize=4)

ax.fill_between(years_graph, roll_min_graph, roll_max_graph,
                color='#334466', alpha=0.2, zorder=1, label='Amplitude min–max')

# --- Axe X : années (sans les années dupliquées) + jours chauds consécutifs en dessous ---
ax.set_xticks(years_graph)
ax.set_xticklabels(years_graph, rotation=45, fontsize=8, color='#8899bb',
                   fontfamily='monospace')
ax.tick_params(axis='x', pad=2)
ax.tick_params(axis='y', labelsize=9, colors='#8899bb')

trans = blended_transform_factory(ax.transData, ax.transAxes)

# Annotations uniquement pour les années non dupliquées
for idx, row in df_yearly_filtre.iterrows():
    jc = row['jours_chauds']
    if jc > 0:
        ax.text(row['year'], -0.12, f'{int(jc)}',
                transform=trans, ha='center', va='top',
                fontsize=8, color='#ff3333', fontfamily='monospace',
                fontweight='bold', clip_on=False, zorder=10)

# Label explicatif à gauche
ax.text(years_graph.iloc[0] - 0.5, -0.12,
        f'jours > {int(SEUIL_CHAUD)}°C:',
        transform=trans, ha='right', va='top',
        fontsize=8, color='#ff3333', fontfamily='monospace',
        fontweight='bold', clip_on=False, zorder=10)

for spine in ax.spines.values():
    spine.set_edgecolor('#223355')
ax.set_ylabel('Température (°C)', fontsize=12, color='#8899bb', fontfamily='monospace')

ax.grid(True, axis='y', linestyle='--', alpha=0.25, color='#445577')
ax.grid(True, axis='x', linestyle=':', alpha=0.15, color='#445577')

# Titre
titre = (
    f'Températures annuelles — {VILLE.upper()}  ·  {DATE_DEBUT[:4]}–{annee_fin_reelle}\n'
    f'Mois : {mois_label}  ·  {HEURE_DEBUT:02d}h00 – {HEURE_FIN:02d}h00'
)
ax.set_title(titre, fontsize=16, fontweight='bold', color='white',
             fontfamily='monospace', pad=14)

# Légende
legend = ax.legend(fontsize=10, loc='upper left',
                   facecolor='#0d1525', edgecolor='#334466',
                   labelcolor='white', framealpha=0.85)
for text in legend.get_texts():
    text.set_fontfamily('monospace')

# Footer
fig.text(0.01, 0.01, 'Source : ERA5 / Copernicus Climate Change Service',
         fontsize=8, color='#445566', fontfamily='monospace', style='italic')

# --- Sauvegarde ---
mois_str       = ''.join(str(m) for m in MOIS_FILTRES)
nom_fichier    = f"yearly_temp_{VILLE.lower()}_{DATE_DEBUT[:4]}_{annee_fin_reelle}_m{mois_str}_{HEURE_DEBUT}h{HEURE_FIN}h_roll{FENETRE_MOBILE}_cons{MIN_CONSECUTIFS}.png"
chemin_complet = f"{DOSSIER_SORTIE}/{nom_fichier}"

plt.subplots_adjust(left=0.05, right=0.96, top=0.90, bottom=0.16)
plt.savefig(chemin_complet, dpi=150, facecolor='#0a0e1a')
plt.show()
print(f"\nGraphique sauvegardé : {chemin_complet}")