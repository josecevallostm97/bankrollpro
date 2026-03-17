# ╔══════════════════════════════════════════════════════════════════╗
# ║   🏆  BANKROLL PRO — Versión Final                      ║
# ║   Odds API + Understat (5 ligas) + football-data.org (5 ligas) ║
# ║   Ecuador · Colombia · Chile → aparecen sin filtro xG          ║
# ║   Lista A : Doble Oportunidad  ≥ 80%  + cuota mínima 1.20     ║
# ║   Lista B : Value Picks cuota  1.70 – 2.20                     ║
# ║   Lista C : Over/Under 2.5     ≥ 70%                           ║
# ║   Lista 👑: Smart Parlays      A×A solo                        ║
# ╚══════════════════════════════════════════════════════════════════╝
#
# INSTALACIÓN (una sola vez en PowerShell):
#   pip install requests pandas tabulate aiohttp understat nest_asyncio --quiet
#   pip install understat --no-deps --quiet
#
# EJECUCIÓN en PowerShell:
#   python bankrollpro_v6.py
#
# EJECUCIÓN en Google Colab:
#   Igual — el script detecta el entorno automáticamente.

import sys
import time
import asyncio
import requests
import pandas as pd
import nest_asyncio
from datetime import datetime, timezone
from tabulate import tabulate
from itertools import combinations
from collections import defaultdict
from bs4 import BeautifulSoup
import unicodedata
import json

nest_asyncio.apply()

# ═══════════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════

ODDS_API_KEY          = "174f413b331bba862acbd9198406c09c"
FOOTBALLDATA_API_KEY  = "3260d0ac54e848e5a627d71495aa9667"
TELEGRAM_TOKEN        = "8505149080:AAHtyvW7uL4OZ4x66tLhUeLQPwPaIjaAVZo"
TELEGRAM_CHAT_ID      = "5933853317"

CUOTA_MIN_LISTA_A     = 1.25   # Rango Doble Oportunidad — mínimo
CUOTA_MAX_LISTA_A     = 1.45   # Rango Doble Oportunidad — máximo
PROB_MINIMA_LISTA_A   = 70.0   # Probabilidad mínima implícita requerida
CUOTA_MIN_LISTA_B     = 1.70
CUOTA_MAX_LISTA_B     = 2.20
MAX_LISTA_A_POR_DIA   = 12
PROB_MINIMA_LISTA_C   = 70.0
LINEA_GOLES           = 2.5
PARLAY_CUOTA_MIN      = 1.50
PARLAY_CUOTA_MAX      = 2.20
PARLAY_MAX_RESULTADOS = 5
BOOKMAKER_PREFERIDO   = "betano"  # Solo cuotas de Betano

# ═══════════════════════════════════════════════════════════════════
#  📅  FECHAS
# ═══════════════════════════════════════════════════════════════════

def obtener_fechas_analisis():
    hoy = datetime.now(timezone.utc).date()
    return [hoy], {hoy: "HOY"}

FECHAS_ANALISIS, ETIQUETAS_FECHAS = obtener_fechas_analisis()

# ═══════════════════════════════════════════════════════════════════
#  ⚽  LIGAS Y FUENTES DE DATOS
# ═══════════════════════════════════════════════════════════════════

# 5 grandes ligas → Understat (xG + forma)
LIGAS_CON_UNDERSTAT = {
    "soccer_epl",
    "soccer_spain_la_liga",
    "soccer_germany_bundesliga",
    "soccer_italy_serie_a",
    "soccer_france_ligue_one",
}

# Ligas adicionales → football-data.org (solo forma, sin xG)
LIGAS_CON_FOOTBALLDATA = {
    "soccer_netherlands_eredivisie" : "DED",
    "soccer_efl_championship"       : "ELC",
    "soccer_portugal_primeira_liga" : "PPL",
    "soccer_brazil_campeonato"      : "BSA",
    "soccer_uefa_champs_league"     : "CL",
}

# Todas las ligas del sistema
LIGAS = [
    {"nombre": "Premier League (Inglaterra)",  "sport_key": "soccer_epl"},
    {"nombre": "Bundesliga (Alemania)",        "sport_key": "soccer_germany_bundesliga"},
    {"nombre": "La Liga (España)",             "sport_key": "soccer_spain_la_liga"},
    {"nombre": "Serie A (Italia)",             "sport_key": "soccer_italy_serie_a"},
    {"nombre": "Ligue 1 (Francia)",            "sport_key": "soccer_france_ligue_one"},
    {"nombre": "Eredivisie (Países Bajos)",    "sport_key": "soccer_netherlands_eredivisie"},
    {"nombre": "Championship (Inglaterra)",    "sport_key": "soccer_efl_championship"},
    {"nombre": "Primeira Liga (Portugal)",     "sport_key": "soccer_portugal_primeira_liga"},
    {"nombre": "Brasileirao Série A",          "sport_key": "soccer_brazil_campeonato"},
    {"nombre": "LaLiga Hypermotion (España)",   "sport_key": "soccer_spain_segunda_division"},
    {"nombre": "Primera División (Argentina)", "sport_key": "soccer_argentina_primera_division"},
    {"nombre": "Liga MX (México)",             "sport_key": "soccer_mexico_ligamx"},
    {"nombre": "UEFA Champions League",        "sport_key": "soccer_uefa_champs_league"},
    {"nombre": "UEFA Europa League",           "sport_key": "soccer_uefa_europa_league"},
    {"nombre": "UEFA Conference League",       "sport_key": "soccer_uefa_europa_conference_league"},
    {"nombre": "Copa Libertadores",            "sport_key": "soccer_conmebol_copa_libertadores"},
    {"nombre": "Copa Sudamericana",            "sport_key": "soccer_conmebol_copa_sudamericana"},
    {"nombre": "Liga Pro (Ecuador)",           "sport_key": "soccer_ecuador_liga_pro"},
    {"nombre": "Primera A (Colombia)",         "sport_key": "soccer_colombia_primera_a"},
    {"nombre": "Primera División (Chile)",     "sport_key": "soccer_chile_campeonato"},
]

# ═══════════════════════════════════════════════════════════════════
#  📅  TEMPORADA DINÁMICA
# ═══════════════════════════════════════════════════════════════════

def obtener_temporada_understat():
    hoy = datetime.now()
    return str(hoy.year - 1) if hoy.month <= 7 else str(hoy.year)

# ═══════════════════════════════════════════════════════════════════
#  🗺️  MAPEO UNDERSTAT (Odds API → Understat)
# ═══════════════════════════════════════════════════════════════════

mapeo_nombres_understat = {
    # ── PREMIER LEAGUE ────────────────────────────────────────────
    "Arsenal"                  : "Arsenal",
    "Aston Villa"              : "Aston_Villa",
    "Bournemouth"              : "Bournemouth",
    "Brentford"                : "Brentford",
    "Brighton"                 : "Brighton",
    "Brighton & Hove Albion"   : "Brighton",
    "Chelsea"                  : "Chelsea",
    "Crystal Palace"           : "Crystal_Palace",
    "Everton"                  : "Everton",
    "Fulham"                   : "Fulham",
    "Ipswich"                  : "Ipswich",
    "Ipswich Town"             : "Ipswich",
    "Leicester"                : "Leicester",
    "Leicester City"           : "Leicester",
    "Liverpool"                : "Liverpool",
    "Man City"                 : "Manchester_City",
    "Manchester City"          : "Manchester_City",
    "Man United"               : "Manchester_United",
    "Manchester United"        : "Manchester_United",
    "Newcastle"                : "Newcastle_United",
    "Newcastle United"         : "Newcastle_United",
    "Nottingham Forest"        : "Nottingham_Forest",
    "Nott'm Forest"            : "Nottingham_Forest",
    "Southampton"              : "Southampton",
    "Spurs"                    : "Tottenham",
    "Tottenham"                : "Tottenham",
    "Tottenham Hotspur"        : "Tottenham",
    "West Ham"                 : "West_Ham",
    "West Ham United"          : "West_Ham",
    "Wolves"                   : "Wolverhampton_Wanderers",
    "Wolverhampton Wanderers"  : "Wolverhampton_Wanderers",
    # ── LA LIGA ───────────────────────────────────────────────────
    "Alaves"                   : "Alaves",
    "Deportivo Alaves"         : "Alaves",
    "Athletic Bilbao"          : "Athletic_Club",
    "Athletic Club"            : "Athletic_Club",
    "Atletico Madrid"          : "Atletico_Madrid",
    "Atlético Madrid"          : "Atletico_Madrid",
    "Barcelona"                : "Barcelona",
    "FC Barcelona"             : "Barcelona",
    "Betis"                    : "Real_Betis",
    "Real Betis"               : "Real_Betis",
    "Celta Vigo"               : "Celta_Vigo",
    "Espanyol"                 : "Espanyol",
    "Getafe"                   : "Getafe",
    "Girona"                   : "Girona",
    "Las Palmas"               : "Las_Palmas",
    "Leganes"                  : "Leganes",
    "Mallorca"                 : "Mallorca",
    "Osasuna"                  : "Osasuna",
    "Rayo Vallecano"           : "Rayo_Vallecano",
    "Real Madrid"              : "Real Madrid",
    "Real Sociedad"            : "Real_Sociedad",
    "Sevilla"                  : "Sevilla",
    "Valencia"                 : "Valencia",
    "Valladolid"               : "Valladolid",
    "Villarreal"               : "Villarreal",
    # ── BUNDESLIGA ────────────────────────────────────────────────
    "Bayer Leverkusen"         : "Bayer_Leverkusen",
    "Bayern Munich"            : "Bayern_Munich",
    "FC Bayern Munich"         : "Bayern_Munich",
    "Borussia Dortmund"        : "Borussia_Dortmund",
    "RB Leipzig"               : "RB_Leipzig",
    "Eintracht Frankfurt"      : "Eintracht_Frankfurt",
    "VfB Stuttgart"            : "Stuttgart",
    "Stuttgart"                : "Stuttgart",
    "SC Freiburg"              : "Freiburg",
    "Freiburg"                 : "Freiburg",
    "Hoffenheim"               : "Hoffenheim",
    "TSG Hoffenheim"           : "Hoffenheim",
    "Werder Bremen"            : "Werder_Bremen",
    "Borussia Monchengladbach" : "Borussia_MGB",
    "FC Augsburg"              : "Augsburg",
    "Augsburg"                 : "Augsburg",
    "Union Berlin"             : "Union_Berlin",
    "Mainz 05"                 : "Mainz",
    "Mainz"                    : "Mainz",
    "Heidenheim"               : "Heidenheim",
    "Holstein Kiel"            : "Holstein_Kiel",
    "St. Pauli"                : "St._Pauli",
    "FC St. Pauli"             : "St._Pauli",
    "VfL Wolfsburg"            : "Wolfsburg",
    "VfL Bochum"               : "Bochum",
    # ── SERIE A ───────────────────────────────────────────────────
    "AC Milan"                 : "AC_Milan",
    "Milan"                    : "AC_Milan",
    "Inter Milan"              : "Internazionale",
    "Inter"                    : "Internazionale",
    "Juventus"                 : "Juventus",
    "Napoli"                   : "Napoli",
    "AS Roma"                  : "Roma",
    "Roma"                     : "Roma",
    "Lazio"                    : "Lazio",
    "Atalanta"                 : "Atalanta",
    "Fiorentina"               : "Fiorentina",
    "Torino"                   : "Torino",
    "Bologna"                  : "Bologna",
    "Genoa"                    : "Genoa",
    "Cagliari"                 : "Cagliari",
    "Hellas Verona"            : "Hellas_Verona",
    "Verona"                   : "Hellas_Verona",
    "Udinese"                  : "Udinese",
    "Empoli"                   : "Empoli",
    "Venezia"                  : "Venezia",
    "Como"                     : "Como",
    "Parma"                    : "Parma",
    "Lecce"                    : "Lecce",
    "Monza"                    : "Monza",
    # ── LIGUE 1 ───────────────────────────────────────────────────
    "Paris Saint-Germain"      : "Paris_Saint_Germain",
    "PSG"                      : "Paris_Saint_Germain",
    "Marseille"                : "Marseille",
    "Lyon"                     : "Lyon",
    "Monaco"                   : "Monaco",
    "AS Monaco"                : "Monaco",
    "Lille"                    : "Lille",
    "Nice"                     : "Nice",
    "Lens"                     : "Lens",
    "Rennes"                   : "Rennes",
    "Strasbourg"               : "Strasbourg",
    "Nantes"                   : "Nantes",
    "Montpellier"              : "Montpellier",
    "Reims"                    : "Reims",
    "Brest"                    : "Brest",
    "Toulouse"                 : "Toulouse",
    "Auxerre"                  : "Auxerre",
    "Saint-Etienne"            : "Saint_Etienne",
    "Le Havre"                 : "Le_Havre",
    "Angers"                   : "Angers",
}

def normalizar_nombre_understat(nombre):
    if nombre in mapeo_nombres_understat:
        return mapeo_nombres_understat[nombre]
    return nombre.replace(" ", "_")

# ═══════════════════════════════════════════════════════════════════
#  🔤  NORMALIZACIÓN PARA FOOTBALL-DATA.ORG (fuzzy match)
# ═══════════════════════════════════════════════════════════════════

def _normalizar_str(s):
    """
    Normaliza un nombre de equipo para comparación:
    - Minúsculas
    - Sin tildes
    - Sin prefijos comunes (FC, AFC, SC, etc.)
    """
    s = s.lower().strip()
    # Quitar tildes
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Quitar prefijos/sufijos comunes
    for term in ["fc ", "afc ", "sc ", "ac ", "as ", "cd ", "ud ", "ca ",
                 "rc ", "rcd ", "sl ", "cf ", "if ", "bk ", "sk "]:
        if s.startswith(term):
            s = s[len(term):]
    for term in [" fc", " sc", " cf", " ac", " as"]:
        if s.endswith(term):
            s = s[:-len(term)]
    return s.strip()

def _encontrar_equipo_en_tabla(nombre_odds, tabla):
    """
    Busca el equipo más cercano en la tabla de football-data.org.
    Usa matching por palabras con umbral de confianza.
    """
    if not tabla:
        return None

    nombre_norm = _normalizar_str(nombre_odds)

    # 1. Match exacto
    for eq in tabla:
        if _normalizar_str(eq["equipo"]) == nombre_norm:
            return eq

    # 2. Match parcial (uno contiene al otro)
    for eq in tabla:
        eq_norm = _normalizar_str(eq["equipo"])
        if nombre_norm in eq_norm or eq_norm in nombre_norm:
            return eq

    # 3. Match por solapamiento de palabras
    palabras_odds = set(nombre_norm.split())
    mejor_score   = 0
    mejor_eq      = None
    for eq in tabla:
        eq_norm      = _normalizar_str(eq["equipo"])
        palabras_eq  = set(eq_norm.split())
        if not palabras_odds or not palabras_eq:
            continue
        solapamiento = len(palabras_odds & palabras_eq)
        score = solapamiento / max(len(palabras_odds), len(palabras_eq))
        if score > mejor_score:
            mejor_score = score
            mejor_eq    = eq

    if mejor_score >= 0.4:
        return mejor_eq

    return None

# ═══════════════════════════════════════════════════════════════════
#  📊  FOOTBALL-DATA.ORG — Carga de tablas con forma
# ═══════════════════════════════════════════════════════════════════

FD_HEADERS  = {"X-Auth-Token": FOOTBALLDATA_API_KEY}
FD_BASE_URL = "https://api.football-data.org/v4"

def _fd_calcular_forma(codigo_liga):
    """
    Descarga los últimos partidos jugados de la liga y calcula
    los últimos 5 resultados de cada equipo.
    Retorna: { team_id: ['W','D','L',...] }
    """
    url    = f"{FD_BASE_URL}/competitions/{codigo_liga}/matches"
    params = {"status": "FINISHED", "limit": 80}
    time.sleep(7)   # respetar límite 10 req/min del plan gratuito
    try:
        r = requests.get(url, headers=FD_HEADERS, params=params, timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
    except Exception:
        return {}

    resultados = defaultdict(list)
    for partido in reversed(data.get("matches", [])):
        h_id = partido["homeTeam"]["id"]
        a_id = partido["awayTeam"]["id"]
        sh   = partido["score"]["fullTime"]["home"]
        sa   = partido["score"]["fullTime"]["away"]
        if sh is None or sa is None:
            continue
        if sh > sa:   rh, ra = "W", "L"
        elif sh < sa: rh, ra = "L", "W"
        else:         rh, ra = "D", "D"
        if len(resultados[h_id]) < 5: resultados[h_id].append(rh)
        if len(resultados[a_id]) < 5: resultados[a_id].append(ra)

    return dict(resultados)


def _fd_obtener_tabla(sport_key):
    """
    Descarga standings + forma de una liga en football-data.org.
    Retorna lista de dicts o None.
    """
    codigo = LIGAS_CON_FOOTBALLDATA.get(sport_key)
    if not codigo:
        return None

    url = f"{FD_BASE_URL}/competitions/{codigo}/standings"
    time.sleep(7)
    try:
        r = requests.get(url, headers=FD_HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"      ⚠️  football-data.org error {r.status_code} para {codigo}")
            return None
        data = r.json()
    except Exception as e:
        print(f"      ⚠️  Error football-data.org: {e}")
        return None

    tabla_total = None
    for grupo in data.get("standings", []):
        if grupo.get("type") == "TOTAL":
            tabla_total = grupo.get("table", [])
            break

    if not tabla_total:
        return None

    # Calcular forma desde partidos
    forma_map = _fd_calcular_forma(codigo)

    resultado = []
    for fila in tabla_total:
        eq_data = fila.get("team", {})
        resultado.append({
            "equipo"           : eq_data.get("name", "?"),
            "team_id"          : eq_data.get("id"),
            "partidos_jugados" : fila.get("playedGames", 0),
            "puntos"           : fila.get("points", 0),
            "diferencia_goles" : fila.get("goalDifference", 0),
            "forma"            : forma_map.get(eq_data.get("id"), []),
        })

    return resultado


def pre_cargar_footballdata(partidos):
    """
    Pre-carga las tablas de football-data.org para las ligas
    que tienen partidos hoy y están en LIGAS_CON_FOOTBALLDATA.
    Retorna: { sport_key: [lista de equipos con forma] }
    """
    ligas_activas = set(
        p["sport_key"] for p in partidos
        if p.get("sport_key") in LIGAS_CON_FOOTBALLDATA
    )

    if not ligas_activas:
        return {}

    print(f"\n  📊  Cargando forma desde football-data.org ({len(ligas_activas)} liga(s))...")
    cache = {}
    for sport_key in ligas_activas:
        codigo = LIGAS_CON_FOOTBALLDATA[sport_key]
        print(f"      ⚽  {sport_key} ({codigo})...")
        tabla = _fd_obtener_tabla(sport_key)
        if tabla:
            cache[sport_key] = tabla
            print(f"          └─ ✅ {len(tabla)} equipos cargados")
        else:
            print(f"          └─ ⚠️  Sin datos")

    print(f"  ✅  football-data.org listo.\n")
    return cache

# ═══════════════════════════════════════════════════════════════════
#  🔬  UNDERSTAT — Enriquecimiento asíncrono
# ═══════════════════════════════════════════════════════════════════

def _understat_metricas_sync(nombre, temporada):
    """
    Llama directamente a la API de Understat usando requests (sin aiohttp).
    Extrae xG, xGA y forma de los últimos 3 partidos del equipo.
    """
    try:
        url = f"https://understat.com/team/{nombre}/{temporada}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.content, "html.parser")
        scripts = soup.find_all("script")

        datos_partidos = None
        for script in scripts:
            if script.string and "datesData" in script.string:
                raw = script.string
                start = raw.index("JSON.parse('") + len("JSON.parse('")
                end   = raw.index("')", start)
                json_str = raw[start:end].encode().decode("unicode_escape")
                datos_partidos = json.loads(json_str)
                break

        if not datos_partidos:
            return None

        jugados = [p for p in datos_partidos if p.get("isResult") is True]
        u3      = jugados[-3:]
        if len(u3) < 3:
            return None

        xgs, xgas, racha = [], [], []
        for p in u3:
            local = p["h"]["title"] == nombre
            xgs.append(float(p["xG"]["h"] if local else p["xG"]["a"]))
            xgas.append(float(p["xG"]["a"] if local else p["xG"]["h"]))
            gf = int(p["goals"]["h"] if local else p["goals"]["a"])
            gc = int(p["goals"]["a"] if local else p["goals"]["h"])
            racha.append("W" if gf > gc else "D" if gf == gc else "L")

        return {
            "xG_promedio" : round(sum(xgs)  / 3, 2),
            "xGA_promedio": round(sum(xgas) / 3, 2),
            "forma"       : racha,
            "fuente"      : "understat",
        }
    except Exception:
        return None


def enriquecer_con_understat(partidos):
    """
    Consulta Understat para las 5 grandes ligas usando requests puro.
    Sin aiohttp ni librería understat — 100% compatible con GitHub Actions.
    """
    partidos_us = [p for p in partidos if p.get("sport_key") in LIGAS_CON_UNDERSTAT]
    if not partidos_us:
        return {}

    mapa = {}
    for p in partidos_us:
        mapa[p["home"]] = normalizar_nombre_understat(p["home"])
        mapa[p["away"]] = normalizar_nombre_understat(p["away"])

    nombres    = list(set(mapa.values()))
    temporada  = obtener_temporada_understat()
    print(f"\n  🔬  Consultando Understat para {len(nombres)} equipos...")

    met_map = {}
    for nombre in nombres:
        time.sleep(2)   # respetar rate limit de Understat
        met_map[nombre] = _understat_metricas_sync(nombre, temporada)

    cache = {}
    for p in partidos_us:
        pid = f"{p['home']}|{p['away']}"
        cache[pid] = {
            "home": met_map.get(mapa[p["home"]]),
            "away": met_map.get(mapa[p["away"]]),
        }

    ok = sum(1 for v in cache.values() if v["home"] or v["away"])
    print(f"  ✅  Understat: datos para {ok}/{len(cache)} partido(s).\n")
    return cache

# ═══════════════════════════════════════════════════════════════════
#  🔗  ENRIQUECIMIENTO UNIFICADO
# ═══════════════════════════════════════════════════════════════════

def obtener_metricas_equipo_fd(nombre_odds, sport_key, fd_cache):
    """
    Busca las métricas de un equipo en el cache de football-data.org.
    Retorna dict con forma y fuente, o None.
    """
    tabla = fd_cache.get(sport_key)
    if not tabla:
        return None
    equipo = _encontrar_equipo_en_tabla(nombre_odds, tabla)
    if not equipo or not equipo["forma"]:
        return None
    return {
        "xG_promedio" : None,   # football-data no tiene xG
        "xGA_promedio": None,
        "forma"       : equipo["forma"],
        "fuente"      : "footballdata",
    }

# ═══════════════════════════════════════════════════════════════════
#  🚫  EXCLUSIÓN Y ETIQUETAS
# ═══════════════════════════════════════════════════════════════════

def _debe_excluir(metricas):
    """
    Descarta si el favorito está en mal momento.
    - Understat: excluye si 2+ L O xG < xGA
    - Football-data: excluye si 2+ L (no hay xG)
    - Sin datos: NO excluye
    """
    if metricas is None:
        return False
    forma = metricas.get("forma", [])
    if forma.count("L") >= 2:
        return True
    xg  = metricas.get("xG_promedio")
    xga = metricas.get("xGA_promedio")
    if xg is not None and xga is not None and xg < xga:
        return True
    return False


def _etiqueta(metricas):
    """Genera la columna de datos estadísticos para la tabla."""
    if metricas is None:
        return "[Sin datos]"
    forma_str = "-".join(metricas.get("forma", []))
    fuente    = metricas.get("fuente", "")
    if fuente == "understat":
        xg  = metricas.get("xG_promedio", "?")
        xga = metricas.get("xGA_promedio", "?")
        return f"[xG:{xg}/{xga} | {forma_str}]"
    elif fuente == "footballdata":
        return f"[Forma: {forma_str}]"
    return "[Sin datos]"


def _metricas_favorito(item, us_cache, fd_cache):
    """
    Devuelve las métricas del equipo favorito de un partido.
    Intenta primero Understat, luego football-data.org.
    """
    pid       = item.get("partido_id", "")
    sport_key = item.get("sport_key", "")

    # Determinar qué equipo es el favorito
    tipo = item.get("tipo_do") or item.get("favorito", "")

    if sport_key in LIGAS_CON_UNDERSTAT and pid in us_cache:
        datos = us_cache[pid]
        if tipo in ("1X", "Local"):   return datos.get("home")
        elif tipo in ("X2", "Visitante"): return datos.get("away")
        elif tipo == "12":
            c1 = item.get("cuota_1_raw", 99)
            c2 = item.get("cuota_2_raw", 99)
            return datos.get("home") if c1 <= c2 else datos.get("away")

    elif sport_key in LIGAS_CON_FOOTBALLDATA:
        home = item.get("home_team", "")
        away = item.get("away_team", "")
        if tipo in ("1X", "Local"):
            return obtener_metricas_equipo_fd(home, sport_key, fd_cache)
        elif tipo in ("X2", "Visitante"):
            return obtener_metricas_equipo_fd(away, sport_key, fd_cache)
        elif tipo == "12":
            c1 = item.get("cuota_1_raw", 99)
            c2 = item.get("cuota_2_raw", 99)
            nombre = home if c1 <= c2 else away
            return obtener_metricas_equipo_fd(nombre, sport_key, fd_cache)

    return None   # Liga sin datos estadísticos

# ═══════════════════════════════════════════════════════════════════
#  🌐  THE ODDS API
# ═══════════════════════════════════════════════════════════════════

ODDS_BASE_URL = "https://api.the-odds-api.com/v4"
uso_api = {"usadas": 0, "restantes": "?"}

def _actualizar_uso(r):
    uso_api["usadas"]    = r.headers.get("x-requests-used",      uso_api["usadas"])
    uso_api["restantes"] = r.headers.get("x-requests-remaining", "?")

def obtener_partidos_liga(sport_key, fechas_objetivo):
    url    = f"{ODDS_BASE_URL}/sports/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY, "regions": "eu",
        "markets": "h2h,totals", "oddsFormat": "decimal", "dateFormat": "iso",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        _actualizar_uso(r)
        if r.status_code == 401: print("      ❌  API Key inválida"); sys.exit(1)
        if r.status_code in (404, 422): return []
        if r.status_code == 429:
            print("      ⏳  Rate limit — esperando 5s...")
            time.sleep(5)
            return obtener_partidos_liga(sport_key, fechas_objetivo)
        if r.status_code != 200: print(f"      ⚠️  Error {r.status_code}"); return []

        resultado = []
        for raw in r.json():
            commence = datetime.fromisoformat(raw["commence_time"].replace("Z", "+00:00"))
            if commence.date() not in fechas_objetivo: continue
            p = _normalizar_partido(raw, commence)
            if p:
                p["sport_key"] = sport_key
                resultado.append(p)
        return resultado
    except requests.exceptions.Timeout:
        print("      ⏱️  Timeout"); return []
    except Exception as e:
        print(f"      ❌  Error: {e}"); return []


def _normalizar_partido(raw, commence):
    bms = raw.get("bookmakers", [])
    if not bms: return None
    if BOOKMAKER_PREFERIDO:
        f = [b for b in bms if b.get("key") == BOOKMAKER_PREFERIDO]
        if f: bms = f

    acc_1, acc_x, acc_2, acc_over, acc_under = [], [], [], [], []
    for bm in bms:
        for mkt in bm.get("markets", []):
            k = mkt.get("key")
            if k == "h2h":
                for o in mkt.get("outcomes", []):
                    p = o.get("price")
                    if p is None: continue
                    if o["name"] == raw["home_team"]:   acc_1.append(p)
                    elif o["name"] == raw["away_team"]: acc_2.append(p)
                    else:                               acc_x.append(p)
            elif k == "totals":
                for o in mkt.get("outcomes", []):
                    desc  = str(o.get("description", o.get("point", "")))
                    price = o.get("price")
                    name  = o.get("name", "")
                    if price is None: continue
                    try:    linea = float(desc)
                    except: continue
                    if abs(linea - LINEA_GOLES) < 0.01:
                        if "over"  in name.lower(): acc_over.append(price)
                        elif "under" in name.lower(): acc_under.append(price)

    def prom(lst): return round(sum(lst)/len(lst), 3) if lst else None
    c1=prom(acc_1); cx=prom(acc_x); c2=prom(acc_2)
    if not all([c1, cx, c2]): return None
    try:
        c1x = round(1/(1/c1+1/cx), 3)
        cx2 = round(1/(1/cx+1/c2), 3)
        c12 = round(1/(1/c1+1/c2), 3)
    except ZeroDivisionError:
        c1x=cx2=c12=None

    return {
        "fecha": commence.date(), "hora_utc": commence.strftime("%H:%M UTC"),
        "home": raw["home_team"], "away": raw["away_team"], "liga": "",
        "cuota_1": c1, "cuota_x": cx, "cuota_2": c2,
        "cuota_1x": c1x, "cuota_x2": cx2, "cuota_12": c12,
        "over_25": prom(acc_over), "under_25": prom(acc_under),
    }

# ═══════════════════════════════════════════════════════════════════
#  📊  ANÁLISIS
# ═══════════════════════════════════════════════════════════════════

def prob(cuota):
    return round(100/cuota, 1) if cuota and cuota > 1 else 0.0

def analizar_lista_a(p):
    opciones = [("1X", p["cuota_1x"]), ("X2", p["cuota_x2"]), ("12", p["cuota_12"])]
    mejor = None
    for tipo, cuota in opciones:
        if cuota is None: continue
        # Cuota DO debe estar entre 1.25 y 1.45 Y probabilidad >= 75%
        if not (CUOTA_MIN_LISTA_A <= cuota <= CUOTA_MAX_LISTA_A): continue
        pr = prob(cuota)
        if pr < PROB_MINIMA_LISTA_A: continue
        if mejor is None or pr > mejor["prob"]:
            mejor = {"tipo": tipo, "cuota": cuota, "prob": pr}
    if mejor:
        return {
            "fecha": p["fecha"], "hora": p["hora_utc"], "liga": p["liga"],
            "partido":    f"{p['home']} vs {p['away']}",
            "partido_id": f"{p['home']}|{p['away']}",
            "home_team":  p["home"], "away_team": p["away"],
            "sport_key":  p.get("sport_key", ""),
            "tipo_do":    mejor["tipo"],
            "cuota_1_raw": p["cuota_1"], "cuota_2_raw": p["cuota_2"],
            "cuota_do":   f"[{mejor['tipo']}] {mejor['cuota']}",
            "cuota_raw":  mejor["cuota"],
            "prob_do":    f"{mejor['prob']}%",
            "prob_raw":   mejor["prob"],
            "criterio":   f"DO {mejor['tipo']} @ {mejor['cuota']} ({mejor['prob']}%)",
            "origen":     "A",
            "pick_label": f"DO {mejor['tipo']} @ {mejor['cuota']}",
        }
    return None

def analizar_lista_b(p):
    candidatos = [(p["cuota_1"],"Local"), (p["cuota_2"],"Visitante")]
    candidatos.sort(key=lambda x: x[0])
    cuota_fav, tipo_fav = candidatos[0]
    if CUOTA_MIN_LISTA_B <= cuota_fav <= CUOTA_MAX_LISTA_B:
        return {
            "fecha": p["fecha"], "hora": p["hora_utc"], "liga": p["liga"],
            "partido":    f"{p['home']} vs {p['away']}",
            "partido_id": f"{p['home']}|{p['away']}",
            "home_team":  p["home"], "away_team": p["away"],
            "sport_key":  p.get("sport_key", ""),
            "cuotas_1x2": f"{p['cuota_1']} / {p['cuota_x']} / {p['cuota_2']}",
            "favorito":   tipo_fav,
            "cuota_fav":  cuota_fav,
            "prob_fav":   f"{prob(cuota_fav)}%",
        }
    return None

def analizar_lista_c(p):
    over_c  = p.get("over_25")
    under_c = p.get("under_25")
    cands = []
    if over_c:
        pr = prob(over_c)
        if pr >= PROB_MINIMA_LISTA_C:
            cands.append({"mercado": f"Over {LINEA_GOLES}", "cuota": over_c, "prob": pr})
    if under_c:
        pr = prob(under_c)
        if pr >= PROB_MINIMA_LISTA_C:
            cands.append({"mercado": f"Under {LINEA_GOLES}", "cuota": under_c, "prob": pr})
    if not cands: return None
    mejor = max(cands, key=lambda x: x["prob"])
    return {
        "fecha": p["fecha"], "hora": p["hora_utc"], "liga": p["liga"],
        "partido":    f"{p['home']} vs {p['away']}",
        "partido_id": f"{p['home']}|{p['away']}",
        "sport_key":  p.get("sport_key", ""),
        "mercado":    mejor["mercado"], "cuota": mejor["cuota"],
        "cuota_raw":  mejor["cuota"], "prob": f"{mejor['prob']}%",
        "prob_raw":   mejor["prob"],
        "over_25":    f"{over_c}  ({prob(over_c)}%)"  if over_c  else "—",
        "under_25":   f"{under_c} ({prob(under_c)}%)" if under_c else "—",
        "origen":     "C",
        "pick_label": f"{mejor['mercado']} @ {mejor['cuota']}",
    }

# ═══════════════════════════════════════════════════════════════════
#  👑  SMART PARLAYS
# ═══════════════════════════════════════════════════════════════════

def generar_smart_parlays(lista_a, lista_c):
    cands = []
    pool  = [(i, "A") for i in lista_a]
    for (p1,_),(p2,_) in combinations(pool, 2):
        _evaluar_par(p1, "A", p2, "A", cands)
    cands.sort(key=lambda x: x["prob_conjunta"], reverse=True)
    return cands[:PARLAY_MAX_RESULTADOS]

def _evaluar_par(p1, o1, p2, o2, cands):
    if p1.get("partido_id") == p2.get("partido_id"): return
    c1=p1.get("cuota_raw"); c2=p2.get("cuota_raw")
    if not c1 or not c2: return
    ct = round(c1*c2, 3)
    if not (PARLAY_CUOTA_MIN <= ct <= PARLAY_CUOTA_MAX): return
    pr1=p1.get("prob_raw",0); pr2=p2.get("prob_raw",0)
    pc = round((pr1/100)*(pr2/100)*100, 1)
    cands.append({
        "fecha": p1["fecha"], "origen": f"{o1}×{o2}",
        "partido1": p1["partido"], "liga1": p1["liga"],
        "pick1": p1.get("pick_label","—"), "cuota1": c1, "prob1": f"{pr1}%",
        "partido2": p2["partido"], "liga2": p2["liga"],
        "pick2": p2.get("pick_label","—"), "cuota2": c2, "prob2": f"{pr2}%",
        "cuota_total": ct, "prob_conjunta": pc, "prob_conj_str": f"{pc}%",
    })

# ═══════════════════════════════════════════════════════════════════
#  🖨️  PRESENTACIÓN
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  📱  TELEGRAM — Envío automático
# ═══════════════════════════════════════════════════════════════════

def enviar_telegram(mensaje):
    """
    Envía un mensaje a Telegram vía Bot API.
    Divide automáticamente si supera 4096 caracteres.
    """
    url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    limite = 4096
    partes = [mensaje[i:i+limite] for i in range(0, len(mensaje), limite)]

    for parte in partes:
        payload = {
            "chat_id"   : TELEGRAM_CHAT_ID,
            "text"      : parte,
            "parse_mode": "HTML",
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                print(f"  ✅  Telegram: mensaje enviado.")
            else:
                print(f"  ⚠️  Telegram error {r.status_code}: {r.text}")
        except Exception as e:
            print(f"  ⚠️  Telegram no disponible: {e}")
        time.sleep(0.5)


def compilar_mensaje_telegram(fecha, todos_los_partidos, lista_a, lista_b, lista_c, lista_elite):
    """
    Construye el resumen optimizado para celular en formato HTML de Telegram.
    """
    fecha_str  = datetime.now().strftime("%d/%m/%Y %H:%M UTC")
    n_partidos = len(todos_los_partidos)
    n_a        = len(lista_a)
    n_b        = len(lista_b)
    n_c        = len(lista_c)
    n_elite    = len(lista_elite)
    lineas     = []

    # ── Encabezado ────────────────────────────────────────────────
    lineas.append("🏆 <b>BANKROLL PRO — Versión Final</b>")
    lineas.append(f"📅 {fecha_str}")
    lineas.append(f"📦 {n_partidos} partidos analizados")
    lineas.append(f"📊 A: {n_a}  |  B: {n_b}  |  C: {n_c}  |  👑 {n_elite}")
    lineas.append("─" * 28)

    # ── Lista A ───────────────────────────────────────────────────
    if lista_a:
        lineas.append(f"\n🏦 <b>LISTA A — BANKROLL BUILDERS ({n_a})</b>")
        lineas.append("<i>Doble Oportunidad ≥ 80% | Cuota ≥ 1.20</i>\n")
        for r in lista_a:
            stats = r.get("stats_tag", "[Sin datos]")
            lineas.append(
                f"⚽ <b>{r['partido']}</b>\n"
                f"   🏟 {r['liga']}\n"
                f"   ⏰ {r['hora']}\n"
                f"   🎯 {r['cuota_do']}  →  {r['prob_do']}\n"
                f"   🔬 {stats}"
            )
            lineas.append("")
    else:
        lineas.append("\n🏦 <b>LISTA A</b> — Sin picks hoy")

    lineas.append("─" * 28)

    # ── Lista B ───────────────────────────────────────────────────
    if lista_b:
        lineas.append(f"\n💎 <b>LISTA B — VALUE PICKS ({n_b})</b>")
        lineas.append("<i>Favorito entre 1.70 y 2.20</i>\n")
        for r in lista_b:
            stats = r.get("stats_tag", "[Sin datos]")
            lineas.append(
                f"⚽ <b>{r['partido']}</b>\n"
                f"   🏟 {r['liga']}\n"
                f"   ⏰ {r['hora']}\n"
                f"   🎯 {r['favorito']} @ {r['cuota_fav']}  →  {r['prob_fav']}\n"
                f"   🔬 {stats}"
            )
            lineas.append("")
    else:
        lineas.append("\n💎 <b>LISTA B</b> — Sin picks hoy")

    lineas.append("─" * 28)

    # ── Lista C ───────────────────────────────────────────────────
    if lista_c:
        lineas.append(f"\n⚽ <b>LISTA C — GOLES ({n_c})</b>")
        lineas.append(f"<i>Over/Under 2.5 ≥ 70%</i>\n")
        for r in lista_c:
            stats = r.get("stats_tag", "[Sin datos]")
            lineas.append(
                f"⚽ <b>{r['partido']}</b>\n"
                f"   🏟 {r['liga']}\n"
                f"   ⏰ {r['hora']}\n"
                f"   🎯 {r['mercado']} @ {r['cuota']}  →  {r['prob']}\n"
                f"   🔬 {stats}"
            )
            lineas.append("")
    else:
        lineas.append("\n⚽ <b>LISTA C</b> — Sin picks hoy")

    lineas.append("─" * 28)

    # ── Smart Parlays ─────────────────────────────────────────────
    if lista_elite:
        lineas.append(f"\n👑 <b>SMART PARLAYS — ELITE ({n_elite})</b>\n")
        for i, p in enumerate(lista_elite, 1):
            lineas.append(
                f"<b>#{i}</b> 🎰 Cuota: <b>{p['cuota_total']}</b>  |  Prob: {p['prob_conj_str']}\n"
                f"   🔵 {p['partido1']}\n"
                f"      {p['pick1']}\n"
                f"   🟢 {p['partido2']}\n"
                f"      {p['pick2']}"
            )
            lineas.append("")
    else:
        lineas.append("\n👑 <b>SMART PARLAYS</b> — Sin combinadas hoy")

    lineas.append("─" * 28)

    # ── Pie ───────────────────────────────────────────────────────
    lineas.append(
        "\n⚠️ <i>Análisis estadístico. NO garantiza resultados.</i>\n"
        "<i>Apuesta solo lo que puedas permitirte perder.</i>"
    )

    return "\n".join(lineas)

SEP  = "═" * 65
SEP2 = "─" * 65

def imprimir_encabezado():
    print(f"\n{SEP}")
    print(f"  🏆  BANKROLL PRO — Versión Final")
    print(f"{SEP}")
    print(f"  📅  Fecha: {' | '.join(str(f) for f in FECHAS_ANALISIS)}")
    print(f"  🔬  Understat (xG+forma): PL · La Liga · Bundesliga · Serie A · Ligue 1")
    print(f"  📊  football-data (forma): Eredivisie · Championship · Primeira Liga · Brasileirao · CL")
    print(f"  📋  Sin filtro estadístico: MX · ARG · Europa · Conference · Libertadores · ECU · COL · CHI")
    print(f"  💡  Lista A: cuota DO {CUOTA_MIN_LISTA_A}–{CUOTA_MAX_LISTA_A} + prob ≥ {PROB_MINIMA_LISTA_A}% (Betano)")
    print(f"  📡  Conectando con The Odds API...\n")

def imprimir_tabla(titulo, emoji, filas, columnas):
    borde = "═" * 63
    print(f"\n╔{borde}╗")
    print(f"║  {emoji}  {titulo:<59}║")
    print(f"╚{borde}╝")
    if filas:
        print(f"\n  📋  {len(filas)} partido(s):\n")
        print(tabulate(filas, headers=columnas, tablefmt="rounded_outline"))
    else:
        print(f"\n  ⚪  No hay picks con los criterios actuales.")
    print()

def imprimir_smart_parlays(parlays):
    borde = "═" * 63
    print(f"\n╔{borde}╗")
    print(f"║  👑  LISTA ELITE — SMART PARLAYS  |  Top {PARLAY_MAX_RESULTADOS} Dupletas{' '*14}║")
    print(f"╚{borde}╝")
    if not parlays:
        print(f"\n  ⚪  No se generaron combinadas hoy.\n")
        return
    print(f"\n  📋  {len(parlays)} combinada(s):\n")
    for i, p in enumerate(parlays, 1):
        print(f"  ┌─ #{i}  [{p['origen']}]  Prob: {p['prob_conj_str']}  │  Cuota: {p['cuota_total']}")
        print(f"  │  🔵  {p['partido1']}  ({p['liga1']})")
        print(f"  │       {p['pick1']}  │  {p['prob1']}")
        print(f"  │  🟢  {p['partido2']}  ({p['liga2']})")
        print(f"  │       {p['pick2']}  │  {p['prob2']}")
        print(f"  └{'─'*62}\n")

# ═══════════════════════════════════════════════════════════════════
#  🚀  FUNCIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════════

def main():
    imprimir_encabezado()

    set_fechas         = set(FECHAS_ANALISIS)
    todos_los_partidos = []
    ligas_activas      = 0

    print(f"  🔍  Verificando ligas...")
    for liga in LIGAS:
        nombre    = liga["nombre"]
        sport_key = liga["sport_key"]
        print(f"  ⚽  {nombre}")
        time.sleep(1.2)
        partidos = obtener_partidos_liga(sport_key, set_fechas)
        if partidos:
            ligas_activas += 1
            for p in partidos:
                p["liga"] = nombre
            todos_los_partidos.extend(partidos)
            print(f"      └─ {len(partidos)} partido(s)")
        else:
            print(f"      ⏸️   Sin partidos hoy")

    print(f"\n  🌍  Ligas activas: {ligas_activas}")
    print(f"  📦  Total partidos: {len(todos_los_partidos)}")

    if not todos_los_partidos:
        print("\n  ⛔  No hay partidos hoy.")
        return

    # ── Cargar datos estadísticos ─────────────────────────────────
    fd_cache = pre_cargar_footballdata(todos_los_partidos)
    us_cache = enriquecer_con_understat(todos_los_partidos)

    # ── LISTA A ───────────────────────────────────────────────────
    lista_a_raw = [analizar_lista_a(p) for p in todos_los_partidos]
    lista_a_raw = [x for x in lista_a_raw if x]

    lista_a_filt = []
    excl_a = 0
    for item in lista_a_raw:
        met = _metricas_favorito(item, us_cache, fd_cache)
        if _debe_excluir(met):
            excl_a += 1
            continue
        item["stats_tag"]      = _etiqueta(met)
        item["xg_favorito"]    = met.get("xG_promedio")    if met else None
        item["xga_favorito"]   = met.get("xGA_promedio")   if met else None
        item["racha_favorito"] = "-".join(met["forma"])    if met and met.get("forma") else None
        lista_a_filt.append(item)

    lista_a_filt.sort(key=lambda x: x["prob_raw"], reverse=True)
    lista_a = []
    cnt_a   = {}
    for item in lista_a_filt:
        k = str(item["fecha"])
        cnt_a.setdefault(k, 0)
        if cnt_a[k] < MAX_LISTA_A_POR_DIA:
            lista_a.append(item)
            cnt_a[k] += 1

    if excl_a:
        print(f"  🚫  Lista A: {excl_a} descartado(s) por forma/xG.")

    # ── LISTA B ───────────────────────────────────────────────────
    lista_b_raw = [analizar_lista_b(p) for p in todos_los_partidos]
    lista_b_raw = [x for x in lista_b_raw if x]
    lista_b     = []
    excl_b      = 0
    for item in lista_b_raw:
        met = _metricas_favorito(item, us_cache, fd_cache)
        if _debe_excluir(met):
            excl_b += 1
            continue
        item["stats_tag"]      = _etiqueta(met)
        item["xg_favorito"]    = met.get("xG_promedio")    if met else None
        item["xga_favorito"]   = met.get("xGA_promedio")   if met else None
        item["racha_favorito"] = "-".join(met["forma"])    if met and met.get("forma") else None
        lista_b.append(item)
    lista_b.sort(key=lambda x: float(x["cuota_fav"]))
    if excl_b:
        print(f"  🚫  Lista B: {excl_b} descartado(s) por forma/xG.")

    # ── LISTA C ───────────────────────────────────────────────────
    lista_c_raw = [analizar_lista_c(p) for p in todos_los_partidos]
    lista_c     = [x for x in lista_c_raw if x]
    for item in lista_c:
        pid  = item.get("partido_id","")
        sk   = item.get("sport_key","")
        home = item.get("partido","").split(" vs ")[0] if " vs " in item.get("partido","") else ""
        if sk in LIGAS_CON_UNDERSTAT and pid in us_cache:
            item["stats_tag"] = _etiqueta(us_cache[pid].get("home"))
        elif sk in LIGAS_CON_FOOTBALLDATA:
            met = obtener_metricas_equipo_fd(home, sk, fd_cache)
            item["stats_tag"] = _etiqueta(met)
        else:
            item["stats_tag"] = "[Sin datos]"
    lista_c.sort(key=lambda x: x["prob_raw"], reverse=True)

    lista_elite = generar_smart_parlays(lista_a, lista_c)

    # ── RESUMEN ───────────────────────────────────────────────────
    borde = "═" * 63
    print(f"\n╔{borde}╗")
    print(f"║  📊  RESUMEN EJECUTIVO{' '*41}║")
    print(f"╚{borde}╝\n")
    res_cols = ["Fecha","Tipo","Partidos","Lista A","Lista B","Lista C","👑 Elite"]
    res_data = []
    for fecha in FECHAS_ANALISIS:
        np  = len([p for p in todos_los_partidos if p["fecha"]==fecha])
        et  = ETIQUETAS_FECHAS.get(fecha, str(fecha))
        na  = len([x for x in lista_a     if x["fecha"]==fecha])
        nb  = len([x for x in lista_b     if x["fecha"]==fecha])
        nc  = len([x for x in lista_c     if x["fecha"]==fecha])
        nel = len([x for x in lista_elite if x["fecha"]==fecha])
        res_data.append([
            str(fecha), et, np,
            f"✅ {na}"  if na  else "⚪ 0",
            f"💎 {nb}"  if nb  else "⚪ 0",
            f"⚽ {nc}"  if nc  else "⚪ 0",
            f"👑 {nel}" if nel else "⚪ 0",
        ])
    print(tabulate(res_data, headers=res_cols, tablefmt="rounded_outline"))

    # ── TABLAS ────────────────────────────────────────────────────
    cols_a  = ["Fecha","Hora","Liga","Partido","Cuota DO","Prob. DO","Criterio","Stats 🔬"]
    filas_a = [[str(r["fecha"]),r["hora"],r["liga"],r["partido"],
                r["cuota_do"],r["prob_do"],r["criterio"],r.get("stats_tag","[Sin datos]")] for r in lista_a]
    imprimir_tabla(f"LISTA A — BANKROLL BUILDERS  |  DO: {CUOTA_MIN_LISTA_A}–{CUOTA_MAX_LISTA_A}  |  Prob ≥ {PROB_MINIMA_LISTA_A}%", "🏦", filas_a, cols_a)

    cols_b  = ["Fecha","Hora","Liga","Partido","1/X/2","Favorito","Cuota","Prob.","Stats 🔬"]
    filas_b = [[str(r["fecha"]),r["hora"],r["liga"],r["partido"],
                r["cuotas_1x2"],r["favorito"],r["cuota_fav"],r["prob_fav"],r.get("stats_tag","[Sin datos]")] for r in lista_b]
    imprimir_tabla(f"LISTA B — VALUE PICKS  |  Favorito {CUOTA_MIN_LISTA_B}–{CUOTA_MAX_LISTA_B}", "💎", filas_b, cols_b)

    cols_c  = ["Fecha","Hora","Liga","Partido","Mercado","Cuota","Prob.","Over 2.5","Under 2.5","Stats 🔬"]
    filas_c = [[str(r["fecha"]),r["hora"],r["liga"],r["partido"],
                r["mercado"],r["cuota"],r["prob"],r["over_25"],r["under_25"],r.get("stats_tag","[Sin datos]")] for r in lista_c]
    imprimir_tabla(f"LISTA C — GOLES  |  Over/Under {LINEA_GOLES} ≥ {PROB_MINIMA_LISTA_C}%", "⚽", filas_c, cols_c)

    sin_ou = len([p for p in todos_los_partidos if not p.get("over_25") and not p.get("under_25")])
    if sin_ou:
        print(f"  ℹ️   {sin_ou} partido(s) sin cuotas Over/Under. Es normal.\n")

    imprimir_smart_parlays(lista_elite)

    # ── CSV ───────────────────────────────────────────────────────
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M")
    nombre_csv = f"bankrollpro_final_{timestamp}.csv"
    drop_a = ["partido_id","home_team","away_team","tipo_do","sport_key",
              "cuota_1_raw","cuota_2_raw","cuota_raw","prob_raw","origen","pick_label","stats_tag"]
    drop_b = ["partido_id","home_team","away_team","sport_key","stats_tag"]
    drop_c = ["partido_id","sport_key","cuota_raw","prob_raw","origen","pick_label","stats_tag"]
    dfs = []
    if lista_a:
        df=pd.DataFrame(lista_a).drop(columns=drop_a,errors="ignore")
        df.insert(0,"lista","A - Bankroll Builder"); dfs.append(df)
    if lista_b:
        df=pd.DataFrame(lista_b).drop(columns=drop_b,errors="ignore")
        df.insert(0,"lista","B - Value Pick"); dfs.append(df)
    if lista_c:
        df=pd.DataFrame(lista_c).drop(columns=drop_c,errors="ignore")
        df.insert(0,"lista","C - Goles"); dfs.append(df)
    if lista_elite:
        df=pd.DataFrame(lista_elite).drop(columns=["prob_conjunta"],errors="ignore")
        df.insert(0,"lista","ELITE - Smart Parlay"); dfs.append(df)
    if dfs:
        pd.concat(dfs,ignore_index=True).to_csv(nombre_csv,index=False,encoding="utf-8-sig")
        print(f"  💾  CSV exportado: {nombre_csv}\n")

    print(f"{SEP2}")
    print(f"  ⚠️   Análisis estadístico. NO garantiza resultados.")
    print(f"  Apuesta solo lo que puedas permitirte perder.")
    print(f"{SEP2}\n")
# ── ENVÍO A TELEGRAM ─────────────────────────────────────────
    print("\n  🚀 Compilando y enviando reporte a Telegram...")
    mensaje_tg = compilar_mensaje_telegram(
        FECHAS_ANALISIS[0], todos_los_partidos, lista_a, lista_b, lista_c, lista_elite
    )
    enviar_telegram(mensaje_tg)
    print("  ✅ Proceso finalizado con éxito.")

