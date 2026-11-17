#!/usr/bin/env python3
"""
construir.py — matriz precalculada de distancias por carretera entre los 542 municipios
de la Comunitat Valenciana (orígenes) y todos los centros docentes del registro oficial
(destinos). Se ejecuta en GitHub Actions con un servidor OSRM local (perfil `car`).

Entradas (todas abiertas y gratuitas):
  - Centros: WFS oficial del ICV (terramapas.icv.gva.es, CC BY), incluye aularios.
  - Municipios: Wikidata (CC0), código INE (P772) + coordenadas (P625).
  - Red viaria: extractos de OpenStreetMap de Geofabrik (ODbL), preparados por el workflow.

Salida (carpeta --out):
  matriz_v1.bin, origenes.json, centros.json, manifest.json, qa.json,
  muestra_validacion.csv, LEEME.md

Formato matriz_v1.bin (little-endian):
  4 bytes  magic  b"IAD1"
  uint16   n_origenes
  uint16   n_centros
  uint16[n_origenes*n_centros]  distancia por carretera, unidades de 100 m
  uint16[n_origenes*n_centros]  tiempo de conducción, unidades de 10 s
  0xFFFF = sin ruta. Fila = origen (orden de origenes.json), columna = centro (orden de centros.json).
"""
import argparse, array, csv, hashlib, json, math, os, random, re, statistics, sys, time
import unicodedata, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

WFS = ("https://terramapas.icv.gva.es/12_Centros_wfs?service=WFS&version=2.0.0"
       "&request=GetFeature&typename=CentrosDocentesRegimen")
WFS_HOST = "terramapas.icv.gva.es"
WDQS = "https://query.wikidata.org/sparql"
UA = "InteriAlDia-matriz/1.0 (https://github.com/MICONADESIGN/gva-bot-data)"
BBOX = (37.5, 41.0, -2.2, 1.2)             # lat_min, lat_max, lon_min, lon_max
ESPERADOS = {"03": 141, "12": 135, "46": 266}   # municipios por provincia (INE)
SIN_RUTA = 0xFFFF
U_DIST, U_TIEMPO = 100.0, 10.0


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- utilidades
def limpia(v):
    return re.sub(r"\s+", " ", (v or "").strip())


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def http_get(url, tries=4, timeout=180, accept=None):
    err = None
    for k in range(tries):
        try:
            h = {"User-Agent": UA}
            if accept:
                h["Accept"] = accept
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
                return r.read()
        except Exception as e:                       # reintento con espera creciente
            err = e
            log(f"  intento {k + 1}/{tries} falló: {e}")
            time.sleep(5 * (k + 1))
    raise SystemExit(f"No se pudo descargar {url[:120]}: {err}")


# ---------------------------------------------------------------- centros
def local(tag):
    return tag.rsplit("}", 1)[-1]


def parsea_gml(texto):
    raiz = ET.fromstring(texto)
    if local(raiz.tag) == "ExceptionReport":
        raise SystemExit("WFS devolvió un error: " + "".join(raiz.itertext()).strip()[:300])
    filas = [{local(c.tag): (c.text or "") for c in m}
             for m in raiz.iter() if local(m.tag) == "CentrosDocentesRegimen"]
    return filas, int(raiz.attrib.get("numberReturned", len(filas)) or 0)


def num(v):
    try:
        return float(str(v).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def descarga_centros(pagina=500, max_paginas=40):
    filas, inicio = [], 0
    for _ in range(max_paginas):
        url = f"{WFS}&count={pagina}&startindex={inicio}"
        assert urllib.parse.urlparse(url).hostname == WFS_HOST
        lote, n = parsea_gml(http_get(url).decode("utf-8"))
        filas += lote
        log(f"  centros: página desde {inicio}: {len(lote)}")
        if n < pagina:
            break
        inicio += pagina
    out, descartados = {}, []
    for f in filas:
        cod = limpia(f.get("codcen"))
        if not re.fullmatch(r"\d{6,8}", cod):
            continue
        lat, lon = num(f.get("latitud")), num(f.get("longitud"))
        if lat is None or lon is None or not (BBOX[0] <= lat <= BBOX[1] and BBOX[2] <= lon <= BBOX[3]):
            descartados.append(cod)
            continue
        out[cod] = {"cod": cod, "nombre": limpia(f.get("dlibre")),
                    "municipio": limpia(f.get("noms_mun")) or limpia(f.get("localidad_oficial")),
                    "provincia": limpia(f.get("provincia")), "estado": limpia(f.get("cod_estado")),
                    "lat": round(lat, 6), "lon": round(lon, 6)}
    centros = [out[k] for k in sorted(out)]
    if len(centros) < 3000:
        raise SystemExit(f"Solo {len(centros)} centros: descarga incompleta, se aborta.")
    return centros, descartados


# ---------------------------------------------------------------- municipios
SPARQL = """SELECT ?m ?ine ?lat ?lon ?es ?ca WHERE {
  ?m wdt:P772 ?ine . FILTER(REGEX(?ine, "^(03|12|46)[0-9]{3}$"))
  ?m p:P625/psv:P625 [ wikibase:geoLatitude ?lat ; wikibase:geoLongitude ?lon ] .
  OPTIONAL { ?m rdfs:label ?es FILTER(LANG(?es)="es") }
  OPTIONAL { ?m rdfs:label ?ca FILTER(LANG(?ca)="ca") }
}"""


def descarga_municipios():
    url = WDQS + "?format=json&query=" + urllib.parse.quote(SPARQL)
    datos = json.loads(http_get(url, accept="application/sparql-results+json").decode("utf-8"))
    por_ine = {}
    for b in datos["results"]["bindings"]:
        ine = b["ine"]["value"]
        d = por_ine.setdefault(ine, {"lats": [], "lons": [], "es": None, "ca": None,
                                      "qid": b["m"]["value"].rsplit("/", 1)[-1]})
        d["lats"].append(float(b["lat"]["value"]))
        d["lons"].append(float(b["lon"]["value"]))
        d["es"] = d["es"] or b.get("es", {}).get("value")
        d["ca"] = d["ca"] or b.get("ca", {}).get("value")
    cuenta = {}
    for ine in por_ine:
        cuenta[ine[:2]] = cuenta.get(ine[:2], 0) + 1
    if cuenta != ESPERADOS:
        raise SystemExit(f"Municipios por provincia {cuenta} ≠ esperado {ESPERADOS}: se aborta.")
    origenes = []
    for ine in sorted(por_ine):
        d = por_ine[ine]
        lat, lon = sum(d["lats"]) / len(d["lats"]), sum(d["lons"]) / len(d["lons"])
        if not (BBOX[0] <= lat <= BBOX[1] and BBOX[2] <= lon <= BBOX[3]):
            raise SystemExit(f"Coordenada fuera de la C. Valenciana: {ine}")
        nom_es = d["es"] or d["ca"] or ine
        origenes.append({"ine": ine, "es": nom_es, "ca": d["ca"] or nom_es,
                         "lat": round(lat, 6), "lon": round(lon, 6), "qid": d["qid"],
                         "dup_coord": len(d["lats"]) > 1})
    return origenes


# ---------------------------------------------------------------- OSRM
def tabla_osrm(base, orig, dest):
    """orig, dest: listas de (lat, lon). Devuelve (dur, dist, snap_o, snap_d)."""
    coords = ";".join(f"{lo:.6f},{la:.6f}" for la, lo in orig + dest)
    s = ";".join(str(i) for i in range(len(orig)))
    d = ";".join(str(len(orig) + i) for i in range(len(dest)))
    url = f"{base}/table/v1/driving/{coords}?sources={s}&destinations={d}&annotations=duration,distance"
    r = json.loads(http_get(url, tries=3, timeout=300).decode("utf-8"))
    if r.get("code") != "Ok":
        raise SystemExit(f"OSRM table error: {r.get('code')} {r.get('message')}")
    return (r["durations"], r["distances"],
            [w.get("distance") for w in r["sources"]], [w.get("distance") for w in r["destinations"]])


def calcula(base, origenes, centros, bloque_o=20, bloque_d=300):
    # ubicaciones únicas (5 decimales ≈ 1 m): se calculan una vez y se copian a los alias
    llave = lambda c: (round(c["lat"], 5), round(c["lon"], 5))
    rep, col_rep = {}, []
    for j, c in enumerate(centros):
        col_rep.append(rep.setdefault(llave(c), j))
    reps = sorted(set(col_rep))
    log(f"  columnas: {len(centros)} centros → {len(reps)} ubicaciones únicas")
    no, nc = len(origenes), len(centros)
    dist = array.array("H", [SIN_RUTA]) * (no * nc)
    tiem = array.array("H", [SIN_RUTA]) * (no * nc)
    snap_o = [0.0] * no
    snap_d = {}
    pts_o = [(o["lat"], o["lon"]) for o in origenes]
    t0, hechos, total = time.time(), 0, math.ceil(no / bloque_o) * math.ceil(len(reps) / bloque_d)
    for i0 in range(0, no, bloque_o):
        io = range(i0, min(i0 + bloque_o, no))
        for k0 in range(0, len(reps), bloque_d):
            jr = reps[k0:k0 + bloque_d]
            dur, dis, so, sd = tabla_osrm(base, [pts_o[i] for i in io],
                                          [(centros[j]["lat"], centros[j]["lon"]) for j in jr])
            for a, i in enumerate(io):
                snap_o[i] = so[a] or 0.0
                for b, j in enumerate(jr):
                    snap_d[j] = sd[b] or 0.0
                    if dur[a][b] is not None and dis[a][b] is not None:
                        dist[i * nc + j] = min(SIN_RUTA - 1, int(round(dis[a][b] / U_DIST)))
                        tiem[i * nc + j] = min(SIN_RUTA - 1, int(round(dur[a][b] / U_TIEMPO)))
            hechos += 1
            if hechos % 25 == 0 or hechos == total:
                log(f"  tablas {hechos}/{total}  ({time.time() - t0:.0f} s)")
    # copiar columnas de ubicaciones repetidas
    for j, r in enumerate(col_rep):
        if r != j:
            for i in range(no):
                dist[i * nc + j] = dist[i * nc + r]
                tiem[i * nc + j] = tiem[i * nc + r]
            snap_d[j] = snap_d.get(r, 0.0)
    return dist, tiem, snap_o, [snap_d.get(j, 0.0) for j in range(nc)]


# ---------------------------------------------------------------- QA
def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))] if v else None


def calidad(origenes, centros, dist, tiem, snap_o, snap_d, rnd):
    no, nc = len(origenes), len(centros)
    total = no * nc
    nulos = sum(1 for x in dist if x == SIN_RUTA)
    ratios, anomalos = [], []
    for _ in range(40000):
        i, j = rnd.randrange(no), rnd.randrange(nc)
        d = dist[i * nc + j]
        if d == SIN_RUTA:
            continue
        aire = haversine_m(origenes[i]["lat"], origenes[i]["lon"], centros[j]["lat"], centros[j]["lon"])
        carretera = d * U_DIST
        if aire > 3000:                                   # evita el ruido del redondeo a 100 m
            r = carretera / aire
            ratios.append(r)
            if r < 0.9 or r > 4:
                anomalos.append((origenes[i]["es"], centros[j]["cod"], centros[j]["municipio"],
                                 round(aire / 1000, 1), round(carretera / 1000, 1)))
    vel = []
    for _ in range(40000):
        i, j = rnd.randrange(no), rnd.randrange(nc)
        d, t = dist[i * nc + j], tiem[i * nc + j]
        if d != SIN_RUTA and t != SIN_RUTA and t > 30:
            vel.append((d * U_DIST / 1000) / (t * U_TIEMPO / 3600))
    # coordenada del municipio (Wikidata) frente a la media de sus centros (registro oficial)
    por_nom = {}
    for c in centros:
        for parte in c["municipio"].split("/"):
            por_nom.setdefault(norm(parte), []).append(c)
    desv, sin_par = [], 0
    for o in origenes:
        grupo = por_nom.get(norm(o["es"])) or por_nom.get(norm(o["ca"]))
        if not grupo:
            sin_par += 1
            continue
        la = sum(c["lat"] for c in grupo) / len(grupo)
        lo = sum(c["lon"] for c in grupo) / len(grupo)
        desv.append((haversine_m(o["lat"], o["lon"], la, lo) / 1000, o["es"], o["ine"], len(grupo)))
    dk = [x[0] for x in desv]
    return {
        "pares_totales": total, "pares_sin_ruta": nulos, "pct_sin_ruta": round(100 * nulos / total, 4),
        "ratio_carretera_aire": {"n": len(ratios), "p5": pct(ratios, .05), "mediana": pct(ratios, .5),
                                  "p95": pct(ratios, .95), "min": min(ratios) if ratios else None,
                                  "max": max(ratios) if ratios else None},
        "pares_anomalos_ratio_fuera_0.9_4": anomalos[:15], "n_anomalos": len(anomalos),
        "velocidad_media_kmh": {"p5": pct(vel, .05), "mediana": pct(vel, .5), "p95": pct(vel, .95)},
        "ajuste_a_carretera_m": {
            "origenes_p95": pct(snap_o, .95), "origenes_max": max(snap_o),
            "centros_p95": pct(snap_d, .95), "centros_max": max(snap_d),
            "origenes_a_mas_de_2km": [(o["es"], round(s)) for o, s in zip(origenes, snap_o) if s > 2000],
            "centros_a_mas_de_2km": sum(1 for s in snap_d if s > 2000)},
        "municipio_wikidata_vs_media_centros_km": {
            "emparejados": len(desv), "sin_emparejar": sin_par,
            "mediana": pct(dk, .5), "p95": pct(dk, .95), "max": max(dk) if dk else None,
            "peores": [(round(a, 2), b, c, n) for a, b, c, n in sorted(desv, reverse=True)[:12]]},
    }


# ---------------------------------------------------------------- salida
def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def escribe(out, origenes, centros, dist, tiem, meta, qa, rnd):
    os.makedirs(out, exist_ok=True)
    no, nc = len(origenes), len(centros)
    with open(os.path.join(out, "matriz_v1.bin"), "wb") as f:
        f.write(b"IAD1")
        f.write(no.to_bytes(2, "little") + nc.to_bytes(2, "little"))
        for arr in (dist, tiem):
            a = array.array("H", arr)
            if sys.byteorder == "big":
                a.byteswap()
            f.write(a.tobytes())
    with open(os.path.join(out, "origenes.json"), "w", encoding="utf-8") as f:
        json.dump([{"ine": o["ine"], "es": o["es"], "ca": o["ca"], "lat": o["lat"], "lon": o["lon"]}
                   for o in origenes], f, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(out, "centros.json"), "w", encoding="utf-8") as f:
        json.dump({"cod": [c["cod"] for c in centros], "lat": [c["lat"] for c in centros],
                   "lon": [c["lon"] for c in centros]}, f, separators=(",", ":"))
    with open(os.path.join(out, "qa.json"), "w", encoding="utf-8") as f:
        json.dump(qa, f, ensure_ascii=False, indent=1)
    with open(os.path.join(out, "muestra_validacion.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ine", "origen", "lat_o", "lon_o", "codcen", "centro", "municipio_centro",
                    "lat_c", "lon_c", "distancia_m", "tiempo_s"])
        vistos = set()
        while len(vistos) < 100:
            i, j = rnd.randrange(no), rnd.randrange(nc)
            if (i, j) in vistos or dist[i * nc + j] == SIN_RUTA:
                continue
            vistos.add((i, j))
            o, c = origenes[i], centros[j]
            w.writerow([o["ine"], o["es"], o["lat"], o["lon"], c["cod"], c["nombre"], c["municipio"],
                        c["lat"], c["lon"], int(dist[i * nc + j] * U_DIST), int(tiem[i * nc + j] * U_TIEMPO)])
    leeme = f"""# Matriz de distancias por carretera — Interí al Dia

Generada automáticamente ({meta['generado_utc']}).

- Filas: {no} municipios de la Comunitat Valenciana (orígenes, `origenes.json`).
- Columnas: {nc} centros docentes del registro oficial, incluidos aularios (`centros.json`).
- `matriz_v1.bin`: cabecera `IAD1` + uint16 n_origenes + uint16 n_centros, después
  `n_origenes*n_centros` uint16 de distancia (unidades de 100 m) y otros tantos de tiempo
  (unidades de 10 s), little-endian, fila a fila. `0xFFFF` = sin ruta.
- Ruta por carretera para automóvil (perfil `car` de OSRM), sin tráfico en tiempo real.
  Los tiempos son estimaciones de OSRM, no de Apple Maps: contrastar con `muestra_validacion.csv`.

## Fuentes y licencias
- Centros docentes: Institut Cartogràfic Valencià / Generalitat Valenciana (CC BY).
- Municipios (código INE y coordenadas): Wikidata (CC0).
- Red viaria: © colaboradores de OpenStreetMap (ODbL), extractos de Geofabrik.
"""
    with open(os.path.join(out, "LEEME.md"), "w", encoding="utf-8") as f:
        f.write(leeme)
    meta["ficheros"] = {}
    for nombre in ("matriz_v1.bin", "origenes.json", "centros.json", "qa.json", "muestra_validacion.csv", "LEEME.md"):
        p = os.path.join(out, nombre)
        meta["ficheros"][nombre] = {"bytes": os.path.getsize(p), "sha256": sha256(p)}
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--osrm", default="http://localhost:5000")
    ap.add_argument("--out", default="matriz/salida")
    ap.add_argument("--work", default="work")
    ap.add_argument("--fixture-centros")       # solo pruebas: JSON con centros
    ap.add_argument("--fixture-origenes")      # solo pruebas: JSON con orígenes
    a = ap.parse_args()
    rnd = random.Random(20261117)

    if a.fixture_centros:
        centros = json.load(open(a.fixture_centros, encoding="utf-8")); descartados = []
        origenes = json.load(open(a.fixture_origenes, encoding="utf-8"))
    else:
        log("Descargando centros (WFS ICV)…")
        centros, descartados = descarga_centros()
        log("Descargando municipios (Wikidata)…")
        origenes = descarga_municipios()
    est = {}
    for c in centros:
        est[c["estado"]] = est.get(c["estado"], 0) + 1
    log(f"centros={len(centros)} (descartados sin coordenada válida: {len(descartados)}), estados={est}; municipios={len(origenes)}")

    log("Calculando matriz con OSRM…")
    dist, tiem, snap_o, snap_d = calcula(a.osrm, origenes, centros)
    log("Controles de calidad…")
    qa = calidad(origenes, centros, dist, tiem, snap_o, snap_d, rnd)
    qa["centros_descartados_sin_coordenada"] = descartados
    qa["estados"] = est
    log(json.dumps({k: qa[k] for k in ("pct_sin_ruta", "ratio_carretera_aire", "velocidad_media_kmh", "ajuste_a_carretera_m",
                                       "municipio_wikidata_vs_media_centros_km")}, ensure_ascii=False)[:3500])

    meta = {"formato": "IAD1", "version": 1,
            "generado_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "n_origenes": len(origenes), "n_centros": len(centros),
            "unidades": {"distancia_m": int(U_DIST), "tiempo_s": int(U_TIEMPO), "sin_ruta": SIN_RUTA},
            "fuentes": {"centros": "ICV/GVA WFS 12_Centros_wfs (CC BY)", "municipios": "Wikidata P772+P625 (CC0)",
                        "red_viaria": "© OpenStreetMap contributors (ODbL) via Geofabrik"}}
    p_meta = os.path.join(a.work, "meta.json")
    if os.path.exists(p_meta):
        meta["entorno"] = json.load(open(p_meta, encoding="utf-8"))
    escribe(a.out, origenes, centros, dist, tiem, meta, qa, rnd)
    log("Listo:", json.dumps({k: v["bytes"] for k, v in meta["ficheros"].items()}))


if __name__ == "__main__":
    main()
