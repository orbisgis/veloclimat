"""
Utilitaires  pour les scripts de traitement des données veloclimat

Ce module fournit des fonctions communes pour:
- Charger la configuration depuis un fichier JSON
- Se connecter à la base de données
"""

import json
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text


def load_config(config_filename="config.json"):
    """
    Charge la configuration depuis un fichier JSON

    Le fichier est cherché dans le même répertoire que le script appelant.

    Args:
        config_filename (str): nom du fichier de configuration (défaut: "config.json")

    Returns:
        dict: configuration de la base de données avec clés 'host', 'port', 'user', 'password', 'database'

    Raises:
        FileNotFoundError: si le fichier n'existe pas
        json.JSONDecodeError: si le JSON est invalide
        KeyError: si la clé 'database' manque dans le fichier

    Example:
        >>> config = load_config("config.json")
        >>> print(config['host'])
        'localhost'
    """
    # Remonte 2 niveaux pour trouver le répertoire du script appelant
    # car ce fichier (utils.py) se trouve dans le même répertoire
    caller_frame = None
    try:
        import inspect
        frame = inspect.currentframe()
        if frame is not None and frame.f_back is not None:
            caller_file = frame.f_back.f_globals.get('__file__')
            if caller_file:
                caller_frame = Path(caller_file).parent
    except:
        pass

    # Fallback: utilise le répertoire du script courant
    if caller_frame is None:
        caller_frame = Path(__file__).parent

    config_path = caller_frame / config_filename

    try:
        with open(config_path) as f:
            config = json.load(f)['database']
            return config
    except FileNotFoundError:
        raise FileNotFoundError(
            f"❌ Fichier de configuration non trouvé: {config_path}\n"
            f"   Assurez-vous que '{config_filename}' existe dans: {caller_frame}"
        )
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"❌ Configuration JSON invalide dans {config_path}: {e}",
            e.doc,
            e.pos
        )
    except KeyError:
        raise KeyError(
            f"❌ Clé 'database' manquante dans {config_path}\n"
            f"   La structure doit être: {{'database': {{'host': '...', 'port': 5432, ...}}}}"
        )


def create_engine_from_config(config_path="config.json"):
    """
    Crée un engine SQLAlchemy depuis la configuration

    Args:
        config_path (str): chemin vers le fichier config.json

    Returns:
        sqlalchemy.engine.Engine: engine PostgreSQL

    Raises:
        FileNotFoundError: si le fichier de configuration n'existe pas
        KeyError: si des clés manquent dans la configuration

    Example:
        >>> engine = create_engine_from_config("config.json")
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT 1"))
    """
    try:
        config = load_config(config_path)
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        raise e

    try:
        url = (
            f"postgresql://{quote_plus(config['user'])}:"
            f"{quote_plus(config['password'])}@"
            f"{config['host']}:{config['port']}/"
            f"{config['database']}"
        )
        return create_engine(url)
    except KeyError as e:
        raise KeyError(f"❌ Clé manquante dans la configuration: {e}")




