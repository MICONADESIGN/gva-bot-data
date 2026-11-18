# Matriz de distancias por carretera — Interí al Dia

Generada automáticamente (2026-11-18T10:10:31Z).

- Filas: 542 municipios de la Comunitat Valenciana (orígenes, `origenes.json`).
- Columnas: 3837 centros docentes del registro oficial, incluidos aularios (`centros.json`).
- `matriz_v1.bin`: cabecera `IAD1` + uint16 n_origenes + uint16 n_centros, después
  `n_origenes*n_centros` uint16 de distancia (unidades de 100 m) y otros tantos de tiempo
  (unidades de 10 s), little-endian, fila a fila. `0xFFFF` = sin ruta.
- Ruta por carretera para automóvil (perfil `car` de OSRM), sin tráfico en tiempo real.
  Los tiempos son estimaciones de OSRM, no de Apple Maps: contrastar con `muestra_validacion.csv`.

## Fuentes y licencias
- Centros docentes: Institut Cartogràfic Valencià / Generalitat Valenciana (CC BY).
- Municipios (código INE y coordenadas): Wikidata (CC0).
- Red viaria: © colaboradores de OpenStreetMap (ODbL); extractos regionales de Geofabrik u OpenStreetMap France (ver manifest.json → entorno.extractos_osm).
