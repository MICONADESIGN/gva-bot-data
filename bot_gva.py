#!/usr/bin/env python3
"""
bot_gva.py — OpoMaster GVA Bot · v6.4 (fix rendiment: skip re-upload + diff incremental)

Canvis respecte v6.3:
  - Pas 5: NO carrega tots els JSONs existents per reconstruir docs_lis/docs_pue.
    En lloc d'això, descarrega interins.json existent i l'actualitza INCREMENTALMENT
    només amb els nous PDFs processats. Estalvia centenars de descàrregues HTTP.
  - Pas 6: SKIP upload si el fitxer posicions/puestos del dia ja existia i no ha
    canviat (no s'ha afegit cap document nou a aquell dia). Estalvia 120+ uploads.
  - construir_interins_json ara rep el dict d'interins previ (incremental).
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from github import Auth, Github, GithubException
from pypdf import PdfReader

import firebase_admin
from firebase_admin import credentials, firestore, messaging

import parsers
from parsers import (
    CATALEG,
    DocumentBolsaInicial,
    DocumentGVA,
    EntradaBolsa,
    EntradaBolsaInicial,
    nom_oficial,
    parse_bolsa_inicial,
    parse_document,
)


# ════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════

GVA_BASE = "https://ceice.gva.es"

PAGINES = {
    "puestos_continua": (
        f"{GVA_BASE}/es/web/rrhh-educacion/convocatoria-y-peticion-telematica",
        ["puestos ofertados", "llocs ofertats", "puestos provisionales", "llocs provisionals"],
    ),
    "adjudicaciones_continuas": (
        f"{GVA_BASE}/es/web/rrhh-educacion/resolucion",
        [
            "adjudicación maestros", "adjudicació mestres",
            "adjudicación secundaria", "adjudicació secundària",
            "lista única", "llista única", "lista_mae", "lis_mae", "lis_sec",
        ],
    ),
    "adjudicaciones_dc": (
        f"{GVA_BASE}/es/web/rrhh-educacion/resolucion1",
        ["puesto asignado provisional", "lloc assignat provisional", "difícil cobertura",
         "_par.pdf", "_pa.pdf"],
    ),
    "puestos_dc": (
        f"{GVA_BASE}/es/web/rrhh-educacion/convocatoria-y-peticion-telematica6",
        ["puestos provisionales", "llocs provisionals", "difícil cobertura", "_pue_par.pdf"],
    ),
    "bolsas_inici": (
        f"{GVA_BASE}/es/web/rrhh-educacion/participantes2",
        ["anexo i", "anexo ii", "lista única de maestros", "listas de especialidades",
         "_par_def_int_lis_mae", "_par_def_int_lis_sec", "personal funcionario interino"],
    ),
    "vacantes": (
        f"{GVA_BASE}/es/web/rrhh-educacion/plazas3",
        ["listado de vacantes", "llistat de vacants", "_lis_vac"],
    ),
}

URLS_HISTORIQUES = [
    # Setembre 2025
    "https://ceice.gva.es/documents/162909733/395952743/250903_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250903_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250903_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250909_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250909_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250909_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250911_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250911_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250911_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250912_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250912_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250916_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250916_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250916_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250918_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250918_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250918_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396452758/250919_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396452758/250919_par.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250923_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250923_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250923_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250925_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250925_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250925_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396452758/250926_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396452758/250926_pue_par.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250930_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250930_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/395952743/250930_lis_sec.pdf",
    # Octubre 2025
    "https://ceice.gva.es/documents/162909733/396867178/251002_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251002_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251002_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251003_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251003_par.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251014_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251014_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251014_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251016_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251016_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251016_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251017_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251017_par.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251021_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251021_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251021_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251023_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251023_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251023_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251024_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251024_par.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251028_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251028_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251028_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251030_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251030_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/396867178/251030_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251031_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/396894852/251031_par.pdf",
    # Novembre 2025
    "https://ceice.gva.es/documents/162909733/398127328/251104_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251104_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251104_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251106_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251106_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251106_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251107_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251107_pue_par.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251111_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251111_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251111_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251113_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251113_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251113_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251114_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251114_par.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251118_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251118_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251118_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251120_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251120_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251120_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/174471913/251121_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251121_par.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251125_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251125_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251125_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251127_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251127_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/398127328/251127_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251128_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/398262677/251128_par.pdf",
    # Desembre 2025
    "https://ceice.gva.es/documents/162909733/399228761/251202_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251202_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251202_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251204_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251204_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251204_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251218_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251218_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/399228761/251218_lis_sec.pdf",
    # Gener 2026
    "https://ceice.gva.es/documents/162909733/400509504/260108_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260108_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260108_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260113_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260113_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260113_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260115_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260115_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260115_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/400817622/260116_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400817622/260116_par.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260120_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260120_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260120_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260127_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260127_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260127_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260129_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260129_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/400509504/260129_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/400817622/260130_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/400817622/260130_par.pdf",
    # Febrer 2026
    "https://ceice.gva.es/documents/162909733/401513401/260203_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260203_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260203_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260205_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260205_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260205_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260206_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260206_par.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260210_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260210_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260210_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260212_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260212_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260212_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260213_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260213_par.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260217_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260217_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260217_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260219_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260219_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260219_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260220_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260220_par.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260224_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260224_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260224_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260226_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260226_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/401513401/260226_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260227_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/401633082/260227_par.pdf",
    # Març 2026
    "https://ceice.gva.es/documents/162909733/402808877/260303_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260303_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260303_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260305_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260305_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260305_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/402972856/260306_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402972856/260306_par.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260310_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260310_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260310_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260312_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260312_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260312_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260317_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260317_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260317_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260324_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260324_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/402808877/260324_lis_sec.pdf",
    # Abril 2026
    "https://ceice.gva.es/documents/162909733/406030899/260416_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260416_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260416_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/406178897/260417_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/406178897/260417_par.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260421_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260421_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260421_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260423_pue_prov.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260423_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/406030899/260423_lis_sec.pdf",
    "https://ceice.gva.es/documents/162909733/406178897/260424_pue_prov.pdf",
    # Bolses inicials
    "https://ceice.gva.es/documents/162909733/385102957/ini_2025_par_pro_int_lis_mae.pdf",
    "https://ceice.gva.es/documents/162909733/385102957/ini_2025_par_pro_int_lis_sec.pdf",
]

TIPO_REGEX = [
    ("bolsa_inici_mae", re.compile(r"(par_def_int_lis_mae|ini_\d{4}_par_pro_int_lis_mae)", re.I)),
    ("bolsa_inici_sec", re.compile(r"(par_def_int_lis_sec|ini_\d{4}_par_pro_int_lis_sec)", re.I)),
    ("pue_prov",    re.compile(r"_pue_prov\.pdf$",  re.I)),
    ("pue_par",     re.compile(r"_pue_par\.pdf$",   re.I)),
    ("lis_mae",     re.compile(r"_lis_mae\.pdf$",   re.I)),
    ("lis_sec",     re.compile(r"_lis_sec\.pdf$",   re.I)),
    ("par",         re.compile(r"_par\.pdf$",       re.I)),
    ("pa",          re.compile(r"_pa\.pdf$",        re.I)),
]
RE_FECHA_PREFIX = re.compile(r"^(\d{6})_")
RE_LIFERAY_TAIL = re.compile(r"\.pdf(/[a-f0-9-]+)?(?:\?[^#]*)?$", re.I)
RE_INI_BOLSA = re.compile(r"^ini_(\d{4})_", re.I)

MAX_CURSOS = 3
SCHEMA_VERSION = 1

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# ════════════════════════════════════════════════════════════════════════
# FIREBASE / FCM
# ════════════════════════════════════════════════════════════════════════

def init_firebase() -> bool:
    try:
        info = json.loads(os.environ["FIREBASECORE"])
        firebase_admin.initialize_app(credentials.Certificate(info))
        print("✅ Firebase inicialitzat")
        return True
    except Exception as e:
        print(f"⚠️ Firebase no disponible: {e}", file=sys.stderr)
        return False


def fcm_topic_general(filename: str, tipus: str, curs: str) -> None:
    titol_per_tipus = {
        "pue_prov": "Nous puestos publicats",
        "pue_par":  "Puestos difícil cobertura",
        "lis_mae":  "Adjudicació Mestres",
        "lis_sec":  "Adjudicació Secundària",
        "par":      "Adjudicació Difícil Cobertura",
        "bolsa_inici_mae": "Bolsa inicial Mestres",
        "bolsa_inici_sec": "Bolsa inicial Secundària",
    }
    try:
        msg = messaging.Message(
            notification=messaging.Notification(
                title=titol_per_tipus.get(tipus, "Nou document GVA"),
                body=f"Curs {curs} · Obre OpoMaster",
            ),
            data={"filename": filename, "tipus": tipus, "curs": curs, "kind": "general"},
            topic="gva_all",
        )
        resp = messaging.send(msg)
        print(f"📲 FCM general → {resp}")
    except Exception as e:
        print(f"⚠️ FCM general error: {e}")


def fcm_topic_user(nom_norm: str, missatge: str, payload: dict) -> None:
    safe = re.sub(r"[^A-Z0-9_]", "_", nom_norm)[:120]
    topic = f"gva_user_{safe}"
    try:
        msg = messaging.Message(
            notification=messaging.Notification(
                title="Actualització de seguiment",
                body=missatge,
            ),
            data={**payload, "kind": "user_followed"},
            topic=topic,
        )
        resp = messaging.send(msg)
        print(f"📲 FCM {topic} → {resp}")
    except Exception as e:
        print(f"⚠️ FCM {topic} error: {e}")


def update_firestore(curs: str, manifest_url: str, doc_count: int) -> None:
    try:
        db = firestore.client()
        db.collection("cursos").document(curs).set({
            "curs": curs,
            "manifestURL": manifest_url,
            "documentCount": doc_count,
            "lastUpdated": firestore.SERVER_TIMESTAMP,
            "schemaVersion": SCHEMA_VERSION,
        }, merge=True)
        print(f"✅ Firestore actualitzat: cursos/{curs}")
    except Exception as e:
        print(f"⚠️ Firestore error: {e}")


# ════════════════════════════════════════════════════════════════════════
# UTILS
# ════════════════════════════════════════════════════════════════════════

def any_temporada_actual() -> int:
    now = datetime.now(timezone.utc)
    return now.year if now.month >= 7 else now.year - 1


def tag_curs(any_inici: int) -> str:
    return f"{any_inici}-{any_inici + 1}"


def any_curs_de_filename(filename: str) -> Optional[int]:
    m_ini = RE_INI_BOLSA.match(filename)
    if m_ini:
        return int(m_ini.group(1))
    m = RE_FECHA_PREFIX.match(filename)
    if not m:
        return None
    yy = int(m.group(1)[:2])
    mm = int(m.group(1)[2:4])
    year = 2000 + yy
    return year if mm >= 7 else year - 1


def published_date_de_filename(filename: str) -> str:
    m_ini = RE_INI_BOLSA.match(filename)
    if m_ini:
        return f"{m_ini.group(1)}-07-01"
    m = RE_FECHA_PREFIX.match(filename)
    if not m:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yymmdd = m.group(1)
    return f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"


def data_visualitzacio_setmana(filename: str, tipus: str) -> Optional[str]:
    if RE_INI_BOLSA.match(filename):
        return None
    m = RE_FECHA_PREFIX.match(filename)
    if not m:
        return None
    yymmdd = m.group(1)
    try:
        from datetime import date, timedelta
        d = date(2000 + int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6]))
    except ValueError:
        return None
    if tipus in ("pue_prov", "pue_par"):
        d_anterior = d - timedelta(days=1)
        while d_anterior.weekday() >= 5:
            d_anterior -= timedelta(days=1)
        return d_anterior.isoformat()
    if tipus in ("lis_mae", "lis_sec", "par", "pa"):
        return d.isoformat()
    return None


def normalitzar_nom(nom: str) -> str:
    return parsers._norm_text(nom).replace(",", "").strip()


def clasificar_filename(filename: str) -> Optional[str]:
    fn = filename.lower()
    for tipus, rx in TIPO_REGEX:
        if rx.search(fn):
            return tipus
    return None


# ════════════════════════════════════════════════════════════════════════
# SCRAPING
# ════════════════════════════════════════════════════════════════════════

def extreure_pdfs(url_pagina: str, textos_match: list[str]) -> list[tuple[str, str]]:
    try:
        r = session.get(url_pagina, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ Scraping {url_pagina}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    res: list[tuple[str, str]] = []
    vist: set[str] = set()
    textos_lower = [t.lower() for t in textos_match]

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if ".pdf" not in href.lower():
            continue
        m = re.search(r"(.+?\.pdf)", href, re.I)
        if not m:
            continue
        url_pdf = urljoin(url_pagina, m.group(1))
        if url_pdf in vist:
            continue
        anchor = a.get_text(" ", strip=True)
        parent_text = ""
        if a.parent:
            parent_text = a.parent.get_text(" ", strip=True)[:300]
        fn = url_pdf.rstrip("/").split("/")[-1]
        es_format_estandard = bool(RE_FECHA_PREFIX.match(fn) or RE_INI_BOLSA.match(fn))
        if es_format_estandard or any(
            t in (anchor.lower() + " " + parent_text.lower() + " " + fn.lower())
            for t in textos_lower
        ):
            vist.add(url_pdf)
            res.append((url_pdf, anchor or parent_text[:80]))

    return res


def descarregar_pdf(url: str, dest: Path) -> bool:
    try:
        r = session.get(url, timeout=60, stream=True, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ Baixar {url}: {e}")
        return False
    total = 0
    with open(dest, "wb") as f:
        for chunk in r.iter_content(65536):
            if chunk:
                f.write(chunk)
                total += len(chunk)
    if total < 1024:
        dest.unlink(missing_ok=True)
        return False
    return True


def extreure_text(path: Path) -> str:
    try:
        return "".join(p.extract_text() or "" for p in PdfReader(str(path)).pages)
    except Exception as e:
        print(f"⚠️ Llegir PDF {path.name}: {e}")
        return ""


# ════════════════════════════════════════════════════════════════════════
# RELEASES
# ════════════════════════════════════════════════════════════════════════

def get_or_create_release(repo, tag: str):
    try:
        return repo.get_release(tag)
    except GithubException:
        print(f"ℹ️ Creant release '{tag}'")
        return repo.create_git_release(
            tag=tag, name=f"Curs {tag}",
            message=f"Documents OpoMaster — curs {tag}",
            draft=False, prerelease=False,
        )


def upload_asset(release, filepath: Path, content_type: str, max_retries: int = 5) -> str:
    """
    Puja un asset al release amb retry exponencial.

    GitHub Releases API té un rate limit no documentat però estricte: si
    pugem 200+ assets seguits en pocs segons, retorna 403 Forbidden i el
    PyGithub torna a la cua amb backoff. Sovint però el backoff intern
    no és suficient.

    Aquest wrapper:
      1. Esborra l'asset si ja existia (amb retry).
      2. Puja amb retry exponencial (1s, 2s, 4s, 8s, 16s).
      3. Espera 100ms entre pujades successives per no saturar.
      4. Si tot falla, deixa l'excepció propagar.
    """
    # Petit delay anti-rate-limit (100ms entre pujades). Amb 200 assets
    # això afegeix només 20 segons al total però evita el 403 generalment.
    time.sleep(0.1)

    # Esborrar asset existent (amb retry)
    for intent in range(max_retries):
        try:
            for asset in release.get_assets():
                if asset.name == filepath.name:
                    asset.delete_asset()
                    break
            break  # delete OK (o no calia)
        except GithubException as e:
            if e.status == 403 and intent < max_retries - 1:
                espera = 2 ** intent
                print(f"⏳ delete_asset rate-limit ({intent+1}/{max_retries}), retry en {espera}s")
                time.sleep(espera)
                continue
            raise
        except Exception as e:
            if intent < max_retries - 1:
                time.sleep(2 ** intent)
                continue
            raise

    # Pujar asset (amb retry)
    last_exc: Optional[Exception] = None
    for intent in range(max_retries):
        try:
            a = release.upload_asset(str(filepath), filepath.name, content_type=content_type)
            return a.browser_download_url
        except GithubException as e:
            last_exc = e
            if e.status == 403 and intent < max_retries - 1:
                espera = 2 ** intent + 1  # 1s, 2s, 4s, 8s, 16s
                print(f"⏳ upload_asset {filepath.name} rate-limit ({intent+1}/{max_retries}), retry en {espera}s")
                time.sleep(espera)
                continue
            raise
        except Exception as e:
            last_exc = e
            if intent < max_retries - 1:
                espera = 2 ** intent + 1
                print(f"⚠️ upload_asset {filepath.name} error ({intent+1}/{max_retries}): {e}, retry en {espera}s")
                time.sleep(espera)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError(f"upload_asset {filepath.name}: max retries exhausted")


def get_asset_url(release, name: str) -> Optional[str]:
    for asset in release.get_assets():
        if asset.name == name:
            return asset.browser_download_url
    return None


def descarregar_asset_si_existeix(release, name: str) -> Optional[dict]:
    url = get_asset_url(release, name)
    if not url:
        return None
    try:
        r = session.get(url, timeout=60)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None


# ════════════════════════════════════════════════════════════════════════
# PURGA
# ════════════════════════════════════════════════════════════════════════

def purgar_cursos_obsolets(repo, any_actual: int, firebase_ok: bool) -> None:
    any_obsolet = any_actual - MAX_CURSOS
    tag = tag_curs(any_obsolet)
    try:
        rel = repo.get_release(tag)
        for a in rel.get_assets():
            a.delete_asset()
        rel.delete_release()
        try:
            repo.get_git_ref(f"tags/{tag}").delete()
        except GithubException:
            pass
        print(f"🗑️ Release '{tag}' purgada")
    except GithubException:
        pass
    if firebase_ok:
        try:
            firestore.client().collection("cursos").document(tag).delete()
            print(f"🗑️ Firestore cursos/{tag} purgat")
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════
# AGREGACIÓ INCREMENTAL
# ════════════════════════════════════════════════════════════════════════

def _bucket_cuerpo(cuerpo: str, codi_esp: str = "") -> str:
    """
    Categoritza el cuerpo en 2 buckets per separar interins:

      - "Mestres"      → bolsa de mestres (codis 12X, 153, MESTRES)
      - "AltresCossos" → secundària + FP + EOI + música + arts (la resta)

    Motiu: la GVA gestiona DUES bolses paral·leles (mestres + secundària).
    Una persona pot estar inscrita a totes dues amb les mateixes especialitats
    o amb especialitats diferents. Si fusionem-les en un sol registre, la
    UI mostra esp barrejades (p.ex. "Mestre" amb especialitat de Física).
    """
    codi = (codi_esp or "").strip().upper()
    if codi.startswith("1") and len(codi) == 3:
        return "Mestres"
    if codi == "MESTRES":
        return "Mestres"
    if (cuerpo or "").lower() in ("mestres", "maestros", "mae"):
        return "Mestres"
    return "AltresCossos"


def _clau_interi(nom_norm: str, bucket: str) -> str:
    """Clau composta única per a cada combinació (interí, bucket)."""
    return f"{nom_norm}__{bucket}"


def aplicar_docs_nous_a_interins(
    interins: dict,
    bolses_inicials_noves: list[DocumentBolsaInicial],
    docs_lis_nous: list[DocumentGVA],
    fitxers_nous: set[str],
    es_primera_execucio: bool,
) -> list[dict]:
    """
    Actualitza el dict `interins` IN-PLACE amb les novetats dels docs nous.
    Retorna la llista d'esdeveniments a notificar (buit si primera execució).

    LÒGICA DE LA GVA (v6.5):

    BOLSA DE MESTRES (lis_mae):
      És UNA SOLA llista. Cada mestre apareix UN sol cop amb una posició
      global. NO hi ha capçaleres d'especialitat dins el lis_mae. La
      posició s'aplica a TOTES les habilitacions del mestre (Inf, Pri, Eng,
      Mus, EF, etc.).
      → Quan parsegem un lis_mae i trobem una entrada amb codi="MESTRES",
        actualitzem la pos i estat de TOTES les habilitacions ja conegudes
        del mestre, no sols la fictícia "MESTRES".

    BOLSA DE SECUNDÀRIA / ALTRES (lis_sec, par):
      Té capçaleres d'especialitat. Cada esp té la seva pròpia bolsa
      independent. Una persona pot tenir esp diferents amb posicions
      diferents.
      → Actualitzem només la pos i estat de l'esp concreta.

    CLAU DEL DICT:
      Format: "<NOM_NORMALITZAT>__<bucket>" on bucket és "Mestres" o
      "AltresCossos". Una persona inscrita a ambdues bolses → 2 entrades.

    SERVEIS PRESTATS:
      - Bolsa inicial: ve directament del PDF (AMB SERVEIS / SENSE SERVEIS).
      - lis_*: si una entrada apareix com "adjudicat", marca teServeisPrestats=True.
        El "ha_participat" o "no_adjudicat" NO marca serveis (només indica
        que va participar al procés sense ser adjudicat).
    """
    esdeveniments: list[dict] = []

    # 1. Bolses inicials noves (normalment 0 en execucions normals)
    for bolsa in bolses_inicials_noves:
        for ent in bolsa.entrades:
            nom_norm = normalitzar_nom(ent.nom)
            bucket = _bucket_cuerpo(ent.cuerpo, ent.codiEspecialitat)
            clau = _clau_interi(nom_norm, bucket)
            if clau not in interins:
                interins[clau] = {
                    "nom": ent.nom,
                    "cuerpo": ent.cuerpo,
                    "bucket": bucket,
                    "especialitats": {},
                }
            esp_key = ent.codiEspecialitat
            interins[clau]["especialitats"][esp_key] = {
                "nomEsp": ent.nomEspecialitat,
                "veDeBolsa": True,
                "teServeisPrestats": ent.teServeisPrestats,
                "posicioIniciCurs": ent.posicio,
                "posicioActual": ent.posicio,
                "dataAlta": bolsa.publishedDate,
                "estatActual": "pendent",
                "ultimaAdjudicacio": None,
            }

    # 2. Docs lis_* nous en ordre cronològic
    docs_ordenats = sorted(docs_lis_nous, key=lambda d: d.publishedDate)

    for doc in docs_ordenats:
        for ent in doc.entrades:
            nom_norm = normalitzar_nom(ent.nom)
            bucket = _bucket_cuerpo(ent.cuerpo, ent.codiEspecialitat)
            clau = _clau_interi(nom_norm, bucket)
            if clau not in interins:
                interins[clau] = {
                    "nom": ent.nom,
                    "cuerpo": ent.cuerpo,
                    "bucket": bucket,
                    "especialitats": {},
                }
            esps = interins[clau]["especialitats"]
            esp_key = ent.codiEspecialitat

            # ─── CAS ESPECIAL: lis_mae amb codi "MESTRES" ───────────────
            # El lis_mae no té capçaleres d'especialitat — totes les entrades
            # tenen codiEspecialitat="MESTRES". Cal propagar la posició
            # i estat a TOTES les habilitacions ja conegudes del mestre.
            if bucket == "Mestres" and esp_key == "MESTRES":
                # Si encara no en tenim cap esp registrada, creem el placeholder "MESTRES"
                # (cas raro: mestre que apareix al lis_mae sense bolsa inicial prèvia).
                if not esps:
                    esps["MESTRES"] = {
                        "nomEsp": "Mestres (general)",
                        "veDeBolsa": False,
                        "teServeisPrestats": False,
                        "posicioIniciCurs": None,
                        "posicioActual": ent.posicio,
                        "dataAlta": doc.publishedDate,
                        "estatActual": ent.estat,
                        "ultimaAdjudicacio": None,
                    }

                # Propagar a totes les esp del mestre (esp reals i placeholder)
                for k, esp in esps.items():
                    esp["posicioActual"] = ent.posicio
                    esp["estatActual"] = ent.estat

                    if ent.estat == "adjudicat":
                        ja_tenia = esp["teServeisPrestats"]
                        esp["teServeisPrestats"] = True
                        esp["ultimaAdjudicacio"] = {
                            "data": doc.publishedDate,
                            "filename": doc.filename,
                            "tipusSubst": ent.tipusSubstitucio,
                            "centre": ent.nomCentre,
                            "localitat": ent.localitat,
                            "jornada": ent.jornada,
                            "nomEspPuesto": ent.nomEspPuesto,
                        }
                        if not es_primera_execucio and doc.filename in fitxers_nous:
                            esdeveniments.append({
                                "tipus": "adjudicat",
                                "nom_norm": nom_norm,
                                "bucket": bucket,
                                "nom_visible": ent.nom,
                                "codiEsp": k,
                                "nomEsp": esp.get("nomEsp", k),
                                "centre": ent.nomCentre,
                                "localitat": ent.localitat,
                                "data": doc.publishedDate,
                                "filename": doc.filename,
                                "promogut_serveis": not ja_tenia,
                            })
                continue
            # ─── FI cas mestres ─────────────────────────────────────────

            # ─── Cas estàndard: lis_sec/par amb codi específic ─────────
            if esp_key not in esps:
                # Especialitat nova durant el curs
                esps[esp_key] = {
                    "nomEsp": ent.nomEspecialitat,
                    "veDeBolsa": False,
                    "teServeisPrestats": False,
                    "posicioIniciCurs": None,
                    "posicioActual": ent.posicio,
                    "dataAlta": doc.publishedDate,
                    "estatActual": "pendent",
                    "ultimaAdjudicacio": None,
                }
                if not es_primera_execucio and doc.filename in fitxers_nous:
                    esdeveniments.append({
                        "tipus": "nova_especialitat",
                        "nom_norm": nom_norm,
                        "bucket": bucket,
                        "nom_visible": ent.nom,
                        "codiEsp": esp_key,
                        "nomEsp": ent.nomEspecialitat,
                        "data": doc.publishedDate,
                        "filename": doc.filename,
                    })

            esp = esps[esp_key]
            esp["posicioActual"] = ent.posicio
            esp["estatActual"] = ent.estat

            if ent.estat == "adjudicat":
                ja_tenia = esp["teServeisPrestats"]
                esp["teServeisPrestats"] = True
                esp["ultimaAdjudicacio"] = {
                    "data": doc.publishedDate,
                    "filename": doc.filename,
                    "tipusSubst": ent.tipusSubstitucio,
                    "centre": ent.nomCentre,
                    "localitat": ent.localitat,
                    "jornada": ent.jornada,
                    "nomEspPuesto": ent.nomEspPuesto,
                }
                if not es_primera_execucio and doc.filename in fitxers_nous:
                    esdeveniments.append({
                        "tipus": "adjudicat",
                        "nom_norm": nom_norm,
                        "bucket": bucket,
                        "nom_visible": ent.nom,
                        "codiEsp": esp_key,
                        "nomEsp": ent.nomEspecialitat,
                        "centre": ent.nomCentre,
                        "localitat": ent.localitat,
                        "data": doc.publishedDate,
                        "filename": doc.filename,
                        "promogut_serveis": not ja_tenia,
                    })

    if es_primera_execucio and esdeveniments:
        print(f"ℹ️ Primera execució: {len(esdeveniments)} esdeveniments descartats (no s'envien FCM)")
        esdeveniments = []

    return esdeveniments


def construir_catalog_json() -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "especialitats": CATALEG,
    }


# ════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════

def main() -> None:
    token = os.environ.get("GITHUB_TOKEN")
    repo_name = "sergimendozaibiza/gva-bot-data"
    if not token or not repo_name:
        print("❌ Falten GITHUB_TOKEN / GITHUB_REPOSITORY", file=sys.stderr)
        sys.exit(1)

    firebase_ok = init_firebase()

    any_actual = any_temporada_actual()
    curs_actual = tag_curs(any_actual)
    cursos_acceptats = {any_actual - i for i in range(MAX_CURSOS)}
    print(f"📅 Curs actual: {curs_actual}  Acceptats: {sorted(cursos_acceptats)}")

    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(repo_name)

    purgar_cursos_obsolets(repo, any_actual, firebase_ok)

    release = get_or_create_release(repo, curs_actual)

    # ── Recollir candidats ──────────────────────────────────────────────
    candidats: dict[str, str] = {}
    for clau, (url, textos) in PAGINES.items():
        print(f"🔎 {clau}: {url}")
        trobats = extreure_pdfs(url, textos)
        for u, txt in trobats:
            candidats.setdefault(u, txt)
        print(f"   → {len(trobats)} PDFs")
    for u in URLS_HISTORIQUES:
        candidats.setdefault(u, "historic")
    print(f"📋 Candidats únics: {len(candidats)}")

    # ── Llegir manifest existent ────────────────────────────────────────
    manifest_existent = descarregar_asset_si_existeix(release, "manifest.json") or {
        "schemaVersion": SCHEMA_VERSION,
        "curs": curs_actual,
        "files": {"posicions": {}, "puestos": {}, "bolses_inicials": {}},
    }
    posicions_existents: dict = manifest_existent.get("files", {}).get("posicions", {})
    puestos_existents: dict   = manifest_existent.get("files", {}).get("puestos", {})
    bolses_existents: dict    = manifest_existent.get("files", {}).get("bolses_inicials", {})

    # Primer run si no hi havia cap fitxer previ
    es_primera_execucio = (
        len(posicions_existents) == 0
        and len(puestos_existents) == 0
        and len(bolses_existents) == 0
    )
    if es_primera_execucio:
        print("🆕 Primera execució — sense notificacions FCM individuals")
    else:
        print(f"📋 Manifest previ: {len(posicions_existents)} pos · "
              f"{len(puestos_existents)} pue · {len(bolses_existents)} bolses")

    # ── Determinar quins PDFs cal processar ────────────────────────────
    ja_processats = set(posicions_existents) | set(puestos_existents) | set(bolses_existents)

    # FALLBACK: si el workflow anterior va fallar abans de pujar manifest.json
    # (p.ex. per rate-limit de GitHub a meitat camí), els PDFs poden haver-se
    # pujat al release però NO estar registrats al manifest. En aquest cas el
    # bot creuria que cal reprocessar-los — desaprofitant temps i provocant
    # més rate-limits. Llegim els assets directament del release per detectar
    # quins PDFs ja són allà i tractar-ho com a "execució en recovery".
    pdfs_al_release: set[str] = set()
    try:
        for asset in release.get_assets():
            nom = asset.name
            if nom.endswith(".pdf"):
                pdfs_al_release.add(nom)
    except Exception as e:
        print(f"⚠️ No s'han pogut llegir assets del release: {e}")
    en_recovery = bool(pdfs_al_release) and es_primera_execucio
    if en_recovery:
        print(f"🔄 Recovery: {len(pdfs_al_release)} PDFs ja al release (manifest desactualitzat)")
        print("   → Reprocessarem els PDFs sense re-pujar-los, per reconstruir interins.json")
        # Mantenim es_primera_execucio=True perquè:
        #   1. No enviar notificacions FCM (no són notícies reals)
        #   2. Forçar reconstrucció completa de interins.json
        # NOTA: si volem el recovery però amb FCM (p.ex. sortida prematura
        # del workflow normal sense PDFs nous reals), caldria un flag separat.

    # ── Descarregar interins.json existent (base incremental) ───────────
    # En lloc de recarregar TOTS els JSONs de posicions/* per reconstruir
    # l'estat, descarreguem directament interins.json que ja conté el
    # resultat acumulat de tots els PDFs anteriors.
    interins_existent: dict = {}
    if not es_primera_execucio:
        interins_data = descarregar_asset_si_existeix(release, "interins.json")
        if interins_data:
            interins_existent = interins_data.get("interins", {})
            print(f"📋 interins.json existent: {len(interins_existent)} interins")

            # ─── DETECCIÓ DE CONTAMINACIÓ ─────────────────────────────────
            # Si l'interins.json existent fou generat per una versió antiga
            # del bot (sense bucket o amb bug de barreja d'esp), conté
            # registres __Mestres amb codis d'esp de secundària. La nova
            # versió del bot, que parteix d'aquest dict podrit, no el
            # neteja — només afegeix/actualitza. Resultat: la podridura
            # persisteix execució rere execució.
            #
            # Solució: detectar el patró de contaminació (>5% de __Mestres
            # amb codis NO de mestres) i, si hi és, NETEJAR l'interins
            # existent abans de processar. Es perdran les dades, però es
            # reconstruiran des dels PDFs ja al release (que es llegeixen
            # de nou, no es tornen a baixar de la GVA).
            mestres_total = 0
            mestres_contaminats = 0
            for k, v in interins_existent.items():
                if v.get("bucket") != "Mestres":
                    continue
                mestres_total += 1
                codis = v.get("especialitats", {}).keys()
                # Codi de mestres = comença per "1" i té len 3, o és "MESTRES"
                no_mestres = [
                    c for c in codis
                    if not (c.startswith("1") and len(c) == 3) and c != "MESTRES"
                ]
                if no_mestres:
                    mestres_contaminats += 1

            if mestres_total > 0:
                ratio = mestres_contaminats / mestres_total
                print(f"📊 Mestres contaminats: {mestres_contaminats}/{mestres_total} "
                      f"({ratio*100:.1f}%)")
                if ratio > 0.05:
                    print(f"⚠️ CONTAMINACIÓ DETECTADA — purgant interins.json existent")
                    print(f"   → Reconstruint des dels PDFs (lectura del release, no GVA)")
                    interins_existent = {}
                    en_recovery = True
                    es_primera_execucio = True  # Forcem reconstrucció
                    # Buidem ja_processats per als tipus que afecten interins
                    # (lis_*, bolses_inicials) perquè es reprocessin. Els pue_*
                    # NO afecten interins, així que els deixem com a "ja fets".
                    ja_processats = {
                        fn for fn in ja_processats
                        if not (
                            fn.endswith("_lis_mae.pdf") or
                            fn.endswith("_lis_sec.pdf") or
                            fn.endswith("_par.pdf") or
                            "par_def_int_lis" in fn or
                            "par_pro_int_lis" in fn
                        )
                    }
                    print(f"   → ja_processats reduït a {len(ja_processats)} fitxers")
            # ───────────────────────────────────────────────────────────────
        else:
            if en_recovery:
                print("⚠️ interins.json no existeix encara — execució anterior incompleta")
                print("   → Reprocessarem tots els PDFs lis_*/bolses per reconstruir-lo")
            else:
                print("⚠️ No s'ha pogut carregar interins.json — reconstruint des de zero")
                # No marquem primera execució aquí — només reprocessem.

    # ── Processar PDFs nous ─────────────────────────────────────────────
    docs_lis_nous: list[DocumentGVA] = []
    docs_pue_nous: list[DocumentGVA] = []
    bolses_inicials_noves: list[DocumentBolsaInicial] = []
    fitxers_nous: set[str] = set()
    docs_nous_meta: list[dict] = []

    # Per a primera execució SENSE interins.json previ, necessitem processar
    # tots els PDFs de tipus lis_* per reconstruir interins.json des de zero.
    # Però en compte de CARREGAR els JSONs existents (lent), directament
    # REPROCESSEM els PDFs (que ja tenim al release com a backup).
    # Nota: aixó significa que la primera execució SEMPRE reprocessa tots els PDFs.
    # Les execucions normals (increment) SOLO processen PDFs nous.
    if es_primera_execucio:
        # Afegim tots els candidats al conjunt a processar (ja_processats = buit)
        pass  # ja_processats és buit, tots seran processats

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        for url, ctx in sorted(candidats.items()):
            fn = url.rstrip("/").split("/")[-1]
            tipus = clasificar_filename(fn)
            if not tipus:
                continue
            any_doc = any_curs_de_filename(fn)
            if any_doc is None or any_doc not in cursos_acceptats:
                continue

            if fn in ja_processats:
                continue  # ← SKIP: ja processat en execució anterior

            print(f"📥 {fn} ({tipus}, curs {tag_curs(any_doc)})")
            dest = tmpdir / fn
            # Si el PDF està al release (recovery scenario), descarreguem
            # des del release en lloc de la GVA — és més fiable.
            descarregat = False
            if fn in pdfs_al_release:
                url_release = get_asset_url(release, fn)
                if url_release and descarregar_pdf(url_release, dest):
                    descarregat = True
                    print(f"   ↻ baixat des del release (recovery)")
            if not descarregat:
                if not descarregar_pdf(url, dest):
                    continue
            text = extreure_text(dest)
            if not text:
                continue
            published = published_date_de_filename(fn)
            fitxers_nous.add(fn)

            if tipus.startswith("bolsa_inici"):
                bolsa_doc = parse_bolsa_inicial(text, fn, published)
                print(f"   ✅ Bolsa inicial: {len(bolsa_doc.entrades)} entrades")
                bolses_inicials_noves.append(bolsa_doc)
                # Skip pujada PDF si ja és al release (recovery scenario)
                if fn not in pdfs_al_release:
                    upload_asset(release, dest, "application/pdf")
                p2 = tmpdir / f"bolses_inicials__{fn.replace('.pdf','')}.json"
                with open(p2, "w", encoding="utf-8") as f:
                    json.dump(bolsa_doc.to_dict(), f, ensure_ascii=False, separators=(",", ":"))
                bu = upload_asset(release, p2, "application/json")
                bolses_existents[fn] = {
                    "url": bu,
                    "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            else:
                doc = parse_document(text, fn, tipus, published)
                if tipus in ("lis_mae", "lis_sec", "par"):
                    docs_lis_nous.append(doc)
                else:
                    docs_pue_nous.append(doc)
                print(f"   ✅ {len(doc.entrades)} interins, {len(doc.puestos)} llocs")
                # Skip pujada PDF si ja és al release (recovery scenario)
                if fn not in pdfs_al_release:
                    upload_asset(release, dest, "application/pdf")

            docs_nous_meta.append({"filename": fn, "tipus": tipus, "curs": tag_curs(any_doc)})

    print(f"📋 Nous processats: {len(docs_lis_nous)} lis_*, "
          f"{len(docs_pue_nous)} pue_*, {len(bolses_inicials_noves)} bolses")

    # Si no hi ha res nou i no és primera execució, podem sortir ràpid
    if not fitxers_nous and not es_primera_execucio:
        print("✅ Cap document nou. Res a actualitzar.")
        return

    # ── Construir/actualitzar posicions/*.json i puestos/*.json ─────────
    # IMPORTANT: només pugem els fitxers dels DIES AFECTATS per PDFs nous.
    # Si un displayDate no té cap document nou, no re-pugem el seu JSON.
    from collections import defaultdict

    # Agrupar docs nous per displayDate
    lis_per_data: dict[str, list[DocumentGVA]] = defaultdict(list)
    for d in docs_lis_nous:
        disp = data_visualitzacio_setmana(d.filename, d.tipus) or d.publishedDate
        lis_per_data[disp].append(d)

    pue_per_data: dict[str, list[DocumentGVA]] = defaultdict(list)
    for d in docs_pue_nous:
        disp = data_visualitzacio_setmana(d.filename, d.tipus) or d.publishedDate
        pue_per_data[disp].append(d)

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)

        for display_date, ds in lis_per_data.items():
            # Fusionar amb documents ja existents per aquest dia (si n'hi ha)
            # Recuperem el JSON existent del release per no perdre dades del dia
            docs_existents_dia: list[dict] = []
            # Busquem si hi havia un JSON per aquest dia
            any_existent_url = None
            for fn_ex, info_ex in posicions_existents.items():
                if info_ex.get("displayDate") == display_date:
                    any_existent_url = info_ex.get("url")
                    break
            if any_existent_url:
                try:
                    resp = session.get(any_existent_url, timeout=30)
                    if resp.ok:
                        docs_existents_dia = resp.json().get("documents", [])
                except Exception:
                    pass

            # Afegir nous (evitant duplicats per filename)
            fns_existents = {d["filename"] for d in docs_existents_dia}
            docs_finals = docs_existents_dia + [
                {
                    **d.to_dict(),
                    "displayDate": data_visualitzacio_setmana(d.filename, d.tipus) or d.publishedDate,
                }
                for d in ds if d.filename not in fns_existents
            ]

            payload = {
                "schemaVersion": SCHEMA_VERSION,
                "displayDate": display_date,
                "documents": docs_finals,
            }
            fname = f"posicions__{display_date}.json"
            p = tmpdir / fname
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            url = upload_asset(release, p, "application/json")
            for d in ds:
                posicions_existents[d.filename] = {
                    "url": url,
                    "displayDate": display_date,
                    "publishedDate": d.publishedDate,
                    "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }

        for display_date, ds in pue_per_data.items():
            docs_existents_dia = []
            any_existent_url = None
            for fn_ex, info_ex in puestos_existents.items():
                if info_ex.get("displayDate") == display_date:
                    any_existent_url = info_ex.get("url")
                    break
            if any_existent_url:
                try:
                    resp = session.get(any_existent_url, timeout=30)
                    if resp.ok:
                        docs_existents_dia = resp.json().get("documents", [])
                except Exception:
                    pass

            fns_existents = {d["filename"] for d in docs_existents_dia}
            docs_finals = docs_existents_dia + [
                {
                    **d.to_dict(),
                    "displayDate": data_visualitzacio_setmana(d.filename, d.tipus) or d.publishedDate,
                }
                for d in ds if d.filename not in fns_existents
            ]
            payload = {
                "schemaVersion": SCHEMA_VERSION,
                "displayDate": display_date,
                "documents": docs_finals,
            }
            fname = f"puestos__{display_date}.json"
            p = tmpdir / fname
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            url = upload_asset(release, p, "application/json")
            for d in ds:
                puestos_existents[d.filename] = {
                    "url": url,
                    "displayDate": display_date,
                    "publishedDate": d.publishedDate,
                    "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }

        # ── Actualitzar interins.json INCREMENTALMENT ───────────────────
        esdeveniments = aplicar_docs_nous_a_interins(
            interins_existent,
            bolses_inicials_noves,
            docs_lis_nous,
            fitxers_nous,
            es_primera_execucio,
        )

        interins_payload = {
            "schemaVersion": SCHEMA_VERSION,
            "curs": curs_actual,
            "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interins": interins_existent,
            "totalInterins": len(interins_existent),
        }
        p_interins = tmpdir / "interins.json"
        with open(p_interins, "w", encoding="utf-8") as f:
            json.dump(interins_payload, f, ensure_ascii=False, separators=(",", ":"))
        url_interins = upload_asset(release, p_interins, "application/json")
        print(f"📤 interins.json ({p_interins.stat().st_size:,} B) · "
              f"{len(interins_existent)} interins → {url_interins[:60]}...")

        # ── catalog.json ────────────────────────────────────────────────
        p_catalog = tmpdir / "catalog.json"
        with open(p_catalog, "w", encoding="utf-8") as f:
            json.dump(construir_catalog_json(), f, ensure_ascii=False, separators=(",", ":"))
        url_catalog = upload_asset(release, p_catalog, "application/json")

        # ── manifest.json ───────────────────────────────────────────────
        manifest = {
            "schemaVersion": SCHEMA_VERSION,
            "curs": curs_actual,
            "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "files": {
                "interins": {
                    "url": url_interins,
                    "lastUpdated": interins_payload["lastUpdated"],
                },
                "catalog": {
                    "url": url_catalog,
                    "lastUpdated": interins_payload["lastUpdated"],
                },
                "posicions": posicions_existents,
                "puestos":   puestos_existents,
                "bolses_inicials": bolses_existents,
            },
            "totalDocuments": (
                len(posicions_existents) + len(puestos_existents) + len(bolses_existents)
            ),
        }
        p_manifest = tmpdir / "manifest.json"
        with open(p_manifest, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, separators=(",", ":"))
        url_manifest = upload_asset(release, p_manifest, "application/json")
        print(f"📤 manifest.json → {url_manifest[:60]}...")

    # ── Firestore ───────────────────────────────────────────────────────
    if firebase_ok:
        update_firestore(curs_actual, url_manifest, manifest["totalDocuments"])

    # ── Notificacions FCM ───────────────────────────────────────────────
    if firebase_ok:
        for d in docs_nous_meta:
            if d["curs"] == curs_actual:
                fcm_topic_general(d["filename"], d["tipus"], curs_actual)

        for ev in esdeveniments:
            if ev["tipus"] == "adjudicat":
                msg = (f"Adjudicat a {ev.get('nomEsp','?')} "
                       f"({ev.get('centre') or '?'} – {ev.get('localitat') or '?'})")
                fcm_topic_user(ev["nom_norm"], msg, {
                    "kind_ev": "adjudicat",
                    "codiEsp": ev["codiEsp"],
                    "data": ev["data"],
                    "filename": ev["filename"],
                })
            elif ev["tipus"] == "nova_especialitat":
                msg = f"Nova especialitat: {ev['nomEsp']} ({ev['codiEsp']})"
                fcm_topic_user(ev["nom_norm"], msg, {
                    "kind_ev": "nova_especialitat",
                    "codiEsp": ev["codiEsp"],
                    "data": ev["data"],
                    "filename": ev["filename"],
                })

    print(f"\n📊 Resum: {len(docs_nous_meta)} docs nous · {len(esdeveniments)} esdeveniments FCM")


if __name__ == "__main__":
    main()
