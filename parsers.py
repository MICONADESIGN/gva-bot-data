"""
parsers.py — OpoMaster GVA · v6

Parsers Python que repliquen exactament els parsers Swift
d'AdjudicacionesManager.swift validats. Funcionen contra els PDFs reals
de Conselleria descarregats per `pypdf`.

PARSERS:
  parse_lis_doc(text, filename, tipus)   → DocumentGVA
      Per als fitxers diaris `lis_mae` i `lis_sec`. Detecta capçaleres
      d'especialitat, entrades amb format `N` o `N/M` + nom, blocs
      d'adjudicats amb centre/localitat/jornada.

  parse_pue_prov(text, filename, tipus)  → DocumentGVA
      Per als fitxers `pue_prov`, `pue_par`. Llista de places
      ofertades.

  parse_bolsa_inicial(text, filename)    → DocumentBolsaInicial
      Per als fitxers `par_def_int_lis_mae.pdf` / `par_def_int_lis_sec.pdf`
      (publicats al juliol). Extreu el flag "AMB SERVEIS" / "SENSE SERVEIS"
      per cada (nom, especialitat).

CATÀLEG:
  CATALEG: dict {codi_esp → {nom_val, nom_cast, cuerpo}}
  nom_oficial(codi)             → nom valencià oficial
  codi_des_de_nom(nom)          → codi (busca per nom val o cast)
  es_especialitat_valida(codi)  → bool

REGLA DE NEGOCI ("teServeisPrestats" derivat):
  El bot fusiona dades així:
    1. Inicialitza des de la bolsa inicial (origen 1).
    2. A cada lis_*.pdf processat, si una entrada està adjudicada,
       marca/promou el flag a True a aquesta especialitat (origen 2).
       També aplica si l'especialitat és nova (adquirida durant el curs).

NOTES:
  - Aquesta versió NO modifica la lògica de detecció d'entrades del parser
    `lis_doc` (validat 12.310 entrades, 173 adjudicats, Sergio a 7B1+209
    al 260421_lis_sec.pdf).
  - `parse_bolsa_inicial` és **especulativa** fins validar contra un PDF
    real de bolsa inicial. Cerca text complet "AMB SERVEIS PRESTATS" /
    "SENSE SERVEIS PRESTATS" i busca el nom proper.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field, asdict
from typing import Optional


# ════════════════════════════════════════════════════════════════════════
# CATÀLEG D'ESPECIALITATS GVA (bilingüe valencià/castellà)
# ════════════════════════════════════════════════════════════════════════

CATALEG: dict[str, dict[str, str]] = {
    "120": {"nom": "Educació Infantil", "cuerpo": "Mestres"},
    "121": {"nom": "Llengua Estrangera: Anglès", "cuerpo": "Mestres"},
    "122": {"nom": "Llengua Estrangera: Francés", "cuerpo": "Mestres"},
    "123": {"nom": "Educació Física", "cuerpo": "Mestres"},
    "124": {"nom": "Música", "cuerpo": "Mestres"},
    "126": {"nom": "Audició i Llenguatge", "cuerpo": "Mestres"},
    "127": {"nom": "Pedagogia Terapèutica", "cuerpo": "Mestres"},
    "128": {"nom": "Educació Primària", "cuerpo": "Mestres"},
    "152": {"nom": "Ed. Especial: Pedagogia Terapèutica", "cuerpo": "Mestres"},
    "153": {"nom": "FPA Primària", "cuerpo": "Mestres"},
    "201": {"nom": "Filosofia", "cuerpo": "Secundària"},
    "202": {"nom": "Grec", "cuerpo": "Secundària"},
    "203": {"nom": "Llatí", "cuerpo": "Secundària"},
    "204": {"nom": "Llengua Castellana i Literatura", "cuerpo": "Secundària"},
    "205": {"nom": "Geografia i Història", "cuerpo": "Secundària"},
    "206": {"nom": "Matemàtiques", "cuerpo": "Secundària"},
    "207": {"nom": "Física i Química", "cuerpo": "Secundària"},
    "208": {"nom": "Biologia i Geologia", "cuerpo": "Secundària"},
    "209": {"nom": "Dibuix", "cuerpo": "Secundària"},
    "210": {"nom": "Francés", "cuerpo": "Secundària"},
    "211": {"nom": "Anglès", "cuerpo": "Secundària"},
    "212": {"nom": "Alemany", "cuerpo": "Secundària"},
    "213": {"nom": "Italià", "cuerpo": "Secundària"},
    "214": {"nom": "Llengua i Literatura Catalana (B)", "cuerpo": "Secundària"},
    "215": {"nom": "Portuguès", "cuerpo": "Secundària"},
    "216": {"nom": "Música", "cuerpo": "Secundària"},
    "217": {"nom": "Educació Física", "cuerpo": "Secundària"},
    "218": {"nom": "Orientació Educativa", "cuerpo": "Secundària"},
    "219": {"nom": "Tecnologia", "cuerpo": "Secundària"},
    "222": {"nom": "Formació i Orientació Laboral", "cuerpo": "Secundària"},
    "224": {"nom": "Org. i Projectes de Fabricació Mecànica", "cuerpo": "Secundària"},
    "226": {"nom": "Sistemes Electrònics", "cuerpo": "Secundària"},
    "227": {"nom": "Org. i Processos Manteniment Vehicles", "cuerpo": "Secundària"},
    "232": {"nom": "Processos i Productes en Fusta i Moble", "cuerpo": "Secundària"},
    "236": {"nom": "Assessoria i Processos d'Imatge Personal", "cuerpo": "Secundària"},
    "237": {"nom": "Processos i Productes en Arts Gràfiques", "cuerpo": "Secundària"},
    "238": {"nom": "Construccions Civils i Edificació", "cuerpo": "Secundària"},
    "240": {"nom": "Navegació i Instal·lacions Marines", "cuerpo": "Secundària"},
    "242": {"nom": "Intervenció Sociocomunitària", "cuerpo": "Secundària"},
    "243": {"nom": "Hostaleria i Turisme", "cuerpo": "Secundària"},
    "245": {"nom": "Processos i Mitjans de Comunicació", "cuerpo": "Secundària"},
    "254": {"nom": "Informàtica", "cuerpo": "Secundària"},
    "256": {"nom": "Llengua i Literatura Valenciana", "cuerpo": "Secundària"},
    "261": {"nom": "Economia", "cuerpo": "Secundària"},
    "263": {"nom": "Administració d'Empreses", "cuerpo": "Secundària"},
    "264": {"nom": "Anàlisi i Química Industrial", "cuerpo": "Secundària"},
    "265": {"nom": "Org. i Gestió Comercial", "cuerpo": "Secundària"},
    "266": {"nom": "Org. i Projectes de Sistemes Energètics", "cuerpo": "Secundària"},
    "268": {"nom": "Processos de Producció Agrària", "cuerpo": "Secundària"},
    "269": {"nom": "Processos en la Indústria Alimentària", "cuerpo": "Secundària"},
    "270": {"nom": "Processos Diagnòstic Clínic i Ortoprotèsics", "cuerpo": "Secundària"},
    "271": {"nom": "Processos Sanitaris", "cuerpo": "Secundària"},
    "272": {"nom": "Processos i Productes Tèxtil/Confecció/Pell", "cuerpo": "Secundària"},
    "273": {"nom": "Processos i Productes Vidre i Ceràmica", "cuerpo": "Secundària"},
    "274": {"nom": "Sistemes Electrotècnics i Automàtics", "cuerpo": "Secundària"},
    "275": {"nom": "Cultura Clàssica", "cuerpo": "Secundària"},
    "277": {"nom": "Àmbit Sociolingüístic", "cuerpo": "Secundària"},
    "288": {"nom": "FPA Comunicació (Castellà)", "cuerpo": "Secundària"},
    "292": {"nom": "FPA Científic/Tecnològic", "cuerpo": "Secundària"},
    "293": {"nom": "FPA Ciències Socials", "cuerpo": "Secundària"},
    "294": {"nom": "FPA Comunicació (Anglès)", "cuerpo": "Secundària"},
    "295": {"nom": "FPA Comunicació (Valencià/Anglès)", "cuerpo": "Secundària"},
    "296": {"nom": "FPA Comunicació (Francés)", "cuerpo": "Secundària"},
    "297": {"nom": "FPA Comunicació (Valencià)", "cuerpo": "Secundària"},
    "2A1": {"nom": "Instal. i Mant. Equips Tèrmics i Fluids", "cuerpo": "Secundària"},
    "2A2": {"nom": "Instal·lacions Electrotècniques", "cuerpo": "Secundària"},
    "2A4": {"nom": "Laboratori", "cuerpo": "Secundària"},
    "2A5": {"nom": "Màquines, Serveis i Producció", "cuerpo": "Secundària"},
    "2A6": {"nom": "Oficina de Projectes de Construcció", "cuerpo": "Secundària"},
    "2A7": {"nom": "Oficina de Projectes Fabricació Mec.", "cuerpo": "Secundària"},
    "2A8": {"nom": "Op. i Equips Elaboració Prod. Alimentaris", "cuerpo": "Secundària"},
    "2A9": {"nom": "Operacions de Processos", "cuerpo": "Secundària"},
    "2B1": {"nom": "Operacions i Equips Producció Agrària", "cuerpo": "Secundària"},
    "2B2": {"nom": "Procediments Diagnòstic Clínic i Ortoprotèsics", "cuerpo": "Secundària"},
    "2B3": {"nom": "Procediments Sanitaris i Assistencials", "cuerpo": "Secundària"},
    "2B4": {"nom": "Processos Comercials", "cuerpo": "Secundària"},
    "2B5": {"nom": "Processos de Gestió Administrativa", "cuerpo": "Secundària"},
    "2B6": {"nom": "Producció Tèxtil i Tractaments Fisicoquímics", "cuerpo": "Secundària"},
    "2B7": {"nom": "Serveis a la Comunitat", "cuerpo": "Secundària"},
    "2B8": {"nom": "Sistemes i Aplicacions Informàtiques", "cuerpo": "Secundària"},
    "2B9": {"nom": "Tècniques i Procediments d'Imatge i So", "cuerpo": "Secundària"},
    "2C1": {"nom": "Equips Electrònics", "cuerpo": "Secundària"},
    "3A1": {"nom": "Cuina i Pastisseria", "cuerpo": "Prof. Esp. FP"},
    "3A2": {"nom": "Estètica", "cuerpo": "Prof. Esp. FP"},
    "3A3": {"nom": "Fabricació i Instal·lació de Fusteria i Moble", "cuerpo": "Prof. Esp. FP"},
    "3A4": {"nom": "Manteniment de Vehicles", "cuerpo": "Prof. Esp. FP"},
    "3A5": {"nom": "Mecanitzat i Mant. de Màquines", "cuerpo": "Prof. Esp. FP"},
    "3A6": {"nom": "Patronatge i Confecció", "cuerpo": "Prof. Esp. FP"},
    "3A7": {"nom": "Perruqueria", "cuerpo": "Prof. Esp. FP"},
    "3A8": {"nom": "Producció d'Arts Gràfiques", "cuerpo": "Prof. Esp. FP"},
    "3A9": {"nom": "Serveis de Restauració", "cuerpo": "Prof. Esp. FP"},
    "3B1": {"nom": "Soldadura", "cuerpo": "Prof. Esp. FP"},
    "6A1": {"nom": "Arpa", "cuerpo": "Música i Arts"},
    "6A2": {"nom": "Cant", "cuerpo": "Música i Arts"},
    "6A3": {"nom": "Clarinet", "cuerpo": "Música i Arts"},
    "6A4": {"nom": "Clave", "cuerpo": "Música i Arts"},
    "6A5": {"nom": "Contrabaix", "cuerpo": "Música i Arts"},
    "6A6": {"nom": "Cor", "cuerpo": "Música i Arts"},
    "6A7": {"nom": "Fagot", "cuerpo": "Música i Arts"},
    "6A8": {"nom": "Flabiol i Tamborí", "cuerpo": "Música i Arts"},
    "6A9": {"nom": "Flauta Travessera", "cuerpo": "Música i Arts"},
    "6B0": {"nom": "Flauta de Bec", "cuerpo": "Música i Arts"},
    "6B1": {"nom": "Fonaments de Composició", "cuerpo": "Música i Arts"},
    "6B3": {"nom": "Guitarra", "cuerpo": "Música i Arts"},
    "6B4": {"nom": "Guitarra Flamenca", "cuerpo": "Música i Arts"},
    "6B5": {"nom": "Història de la Música", "cuerpo": "Música i Arts"},
    "6B7": {"nom": "Instruments de Pua", "cuerpo": "Música i Arts"},
    "6B8": {"nom": "Oboè", "cuerpo": "Música i Arts"},
    "6B9": {"nom": "Orgue", "cuerpo": "Música i Arts"},
    "6C0": {"nom": "Orquestra", "cuerpo": "Música i Arts"},
    "6C1": {"nom": "Percussió", "cuerpo": "Música i Arts"},
    "6C2": {"nom": "Piano", "cuerpo": "Música i Arts"},
    "6C3": {"nom": "Saxòfon", "cuerpo": "Música i Arts"},
    "6C5": {"nom": "Trombó", "cuerpo": "Música i Arts"},
    "6C6": {"nom": "Trompa", "cuerpo": "Música i Arts"},
    "6C7": {"nom": "Trompeta", "cuerpo": "Música i Arts"},
    "6C8": {"nom": "Tuba", "cuerpo": "Música i Arts"},
    "6C9": {"nom": "Viola", "cuerpo": "Música i Arts"},
    "6D1": {"nom": "Violí", "cuerpo": "Música i Arts"},
    "6D2": {"nom": "Violoncel", "cuerpo": "Música i Arts"},
    "6D3": {"nom": "Dansa Espanyola", "cuerpo": "Música i Arts"},
    "6D4": {"nom": "Dansa Clàssica", "cuerpo": "Música i Arts"},
    "6D5": {"nom": "Dansa Contemporània", "cuerpo": "Música i Arts"},
    "6D9": {"nom": "Cant aplicat a l'Art Dramàtic", "cuerpo": "Música i Arts"},
    "6E1": {"nom": "Dansa Aplicada a l'Art Dramàtic", "cuerpo": "Música i Arts"},
    "6E2": {"nom": "Dicció i Expressió Oral", "cuerpo": "Música i Arts"},
    "6E3": {"nom": "Direcció Escènica", "cuerpo": "Música i Arts"},
    "6E6": {"nom": "Espai Escènic", "cuerpo": "Música i Arts"},
    "6E7": {"nom": "Expressió Corporal", "cuerpo": "Música i Arts"},
    "6E8": {"nom": "Il·luminació", "cuerpo": "Música i Arts"},
    "6F3": {"nom": "Literatura Dramàtica", "cuerpo": "Música i Arts"},
    "6F4": {"nom": "Tècniques Escèniques", "cuerpo": "Música i Arts"},
    "6F6": {"nom": "Estètica i Història de l'Art", "cuerpo": "Música i Arts"},
    "6F8": {"nom": "Llenguatge Musical", "cuerpo": "Música i Arts"},
    "6G1": {"nom": "Baix Elèctric", "cuerpo": "Música i Arts"},
    "6G2": {"nom": "Dolçaina", "cuerpo": "Música i Arts"},
    "6G3": {"nom": "Guitarra Elèctrica", "cuerpo": "Música i Arts"},
    "6H1": {"nom": "Caracterització", "cuerpo": "Música i Arts"},
    "6H7": {"nom": "Indumentària", "cuerpo": "Música i Arts"},
    "6J1": {"nom": "Interpretació en el Teatre de Text", "cuerpo": "Música i Arts"},
    "6J7": {"nom": "Teoria de les Arts de l'Espectacle", "cuerpo": "Música i Arts"},
    "5A0": {"nom": "Dansa Clàssica", "cuerpo": "Catedràtics MAE"},
    "5A1": {"nom": "Composició", "cuerpo": "Catedràtics MAE"},
    "5A2": {"nom": "Direcció d'Orquestra", "cuerpo": "Catedràtics MAE"},
    "5A3": {"nom": "Història de la Música", "cuerpo": "Catedràtics MAE"},
    "5A4": {"nom": "Pedagogia", "cuerpo": "Catedràtics MAE"},
    "5A5": {"nom": "Improvisació i Acompanyament", "cuerpo": "Catedràtics MAE"},
    "5A9": {"nom": "Llengua Alemana", "cuerpo": "Catedràtics MAE"},
    "5B0": {"nom": "Llengua Francesa", "cuerpo": "Catedràtics MAE"},
    "5B1": {"nom": "Llengua Anglesa", "cuerpo": "Catedràtics MAE"},
    "5B2": {"nom": "Llengua Italiana", "cuerpo": "Catedràtics MAE"},
    "5B3": {"nom": "Repertori amb piano i veu", "cuerpo": "Catedràtics MAE"},
    "5B4": {"nom": "Repertori amb piano per a instruments", "cuerpo": "Catedràtics MAE"},
    "5C1": {"nom": "Contrabaix de Jazz", "cuerpo": "Catedràtics MAE"},
    "5C2": {"nom": "Dolçaina", "cuerpo": "Catedràtics MAE"},
    "5D0": {"nom": "Baix Elèctric", "cuerpo": "Catedràtics MAE"},
    "5D2": {"nom": "Instruments de Vent de Jazz", "cuerpo": "Catedràtics MAE"},
    "5E1": {"nom": "Cant", "cuerpo": "Catedràtics MAE"},
    "5DA": {"nom": "Instruments de Vent de Jazz: Trompeta", "cuerpo": "Catedràtics MAE"},
    "5DB": {"nom": "Instruments de Vent de Jazz: Trombó", "cuerpo": "Catedràtics MAE"},
    "5DC": {"nom": "Instruments de Vent de Jazz: Saxofon", "cuerpo": "Catedràtics MAE"},
    "5DD": {"nom": "Instruments Històrics de Corda Fregada: Violí Barroc", "cuerpo": "Catedràtics MAE"},
    "5DE": {"nom": "Instruments Històrics de Corda Fregada: Viola Barroca", "cuerpo": "Catedràtics MAE"},
    "5DF": {"nom": "Instruments Històrics de Corda Fregada: Violoncel Barroc", "cuerpo": "Catedràtics MAE"},
    "5DG": {"nom": "Instruments Històrics de Vent: Oboè Barroc", "cuerpo": "Catedràtics MAE"},
    "5DH": {"nom": "Instruments Històrics de Vent: Traverso", "cuerpo": "Catedràtics MAE"},
    "5DI": {"nom": "Instruments Històrics de Vent: Trompa Natural", "cuerpo": "Catedràtics MAE"},
    "5DJ": {"nom": "Instruments Històrics de Vent: Trompeta Natural", "cuerpo": "Catedràtics MAE"},
    "5D6": {"nom": "Teclats/Piano Jazz", "cuerpo": "Catedràtics MAE"},
    "5D9": {"nom": "Clarinet", "cuerpo": "Catedràtics MAE"},
    "5E0": {"nom": "Contrabaix", "cuerpo": "Catedràtics MAE"},
    "5E2": {"nom": "Fagot", "cuerpo": "Catedràtics MAE"},
    "5E3": {"nom": "Flauta Travessera", "cuerpo": "Catedràtics MAE"},
    "5E4": {"nom": "Guitarra", "cuerpo": "Catedràtics MAE"},
    "5E5": {"nom": "Música de Cambra", "cuerpo": "Catedràtics MAE"},
    "5E6": {"nom": "Musicologia", "cuerpo": "Catedràtics MAE"},
    "5E7": {"nom": "Oboè", "cuerpo": "Catedràtics MAE"},
    "5E9": {"nom": "Percussió", "cuerpo": "Catedràtics MAE"},
    "5F0": {"nom": "Piano", "cuerpo": "Catedràtics MAE"},
    "5F1": {"nom": "Saxofon", "cuerpo": "Catedràtics MAE"},
    "5F2": {"nom": "Trompa", "cuerpo": "Catedràtics MAE"},
    "5F3": {"nom": "Trompeta", "cuerpo": "Catedràtics MAE"},
    "5F4": {"nom": "Tuba", "cuerpo": "Catedràtics MAE"},
    "5F5": {"nom": "Viola", "cuerpo": "Catedràtics MAE"},
    "5F6": {"nom": "Violí", "cuerpo": "Catedràtics MAE"},
    "5F7": {"nom": "Violoncel", "cuerpo": "Catedràtics MAE"},
    "5F8": {"nom": "Producció i Gestió de Música i Arts Escèniques", "cuerpo": "Catedràtics MAE"},
    "5F9": {"nom": "Dansa Espanyola", "cuerpo": "Catedràtics MAE"},
    "5G1": {"nom": "Ciències de la Salut Aplicades a la Dansa", "cuerpo": "Catedràtics MAE"},
    "5G2": {"nom": "Dansa Contemporània", "cuerpo": "Catedràtics MAE"},
    "5G3": {"nom": "Història de la Dansa", "cuerpo": "Catedràtics MAE"},
    "5G5": {"nom": "Trombó", "cuerpo": "Catedràtics MAE"},
    "5G6": {"nom": "Arpa", "cuerpo": "Catedràtics MAE"},
    "5G7": {"nom": "Orgue", "cuerpo": "Catedràtics MAE"},
    "5G8": {"nom": "Direcció de Cor", "cuerpo": "Catedràtics MAE"},
    "5G9": {"nom": "Clavecí", "cuerpo": "Catedràtics MAE"},
    "5H1": {"nom": "Tecnologia Musical", "cuerpo": "Catedràtics MAE"},
    "5H4": {"nom": "Anàlisi i Pràctica de Repertori de Dansa Contemporània", "cuerpo": "Catedràtics MAE"},
    "5H5": {"nom": "Escenificació Aplicada a la Dansa", "cuerpo": "Catedràtics MAE"},
    "5L0": {"nom": "Psicopedagogia i Gestió Educativa", "cuerpo": "Catedràtics MAE"},
    "5L6": {"nom": "Dramatúrgia i Escriptura Dramàtica", "cuerpo": "Catedràtics MAE"},
    "5M3": {"nom": "Interpretació en el Teatre de Text", "cuerpo": "Catedràtics MAE"},
    "5N6": {"nom": "Il·luminació", "cuerpo": "Catedràtics MAE"},
    "7A0": {"nom": "Ceràmica", "cuerpo": "Arts Plàstiques"},
    "7A6": {"nom": "Dibuix Artístic i Color", "cuerpo": "Arts Plàstiques"},
    "7A7": {"nom": "Dibuix Tècnic", "cuerpo": "Arts Plàstiques"},
    "7A8": {"nom": "Disseny d'Interiors", "cuerpo": "Arts Plàstiques"},
    "7A9": {"nom": "Disseny de Moda", "cuerpo": "Arts Plàstiques"},
    "7B0": {"nom": "Disseny de Producte", "cuerpo": "Arts Plàstiques"},
    "7B1": {"nom": "Disseny Gràfic", "cuerpo": "Arts Plàstiques"},
    "7B2": {"nom": "Disseny Tèxtil", "cuerpo": "Arts Plàstiques"},
    "7B4": {"nom": "Fotografia", "cuerpo": "Arts Plàstiques"},
    "7B5": {"nom": "Història de l'Art", "cuerpo": "Arts Plàstiques"},
    "7B6": {"nom": "Joieria i Orfebreria", "cuerpo": "Arts Plàstiques"},
    "7B7": {"nom": "Materials i Tecnologia: Ceràmica i Vidre", "cuerpo": "Arts Plàstiques"},
    "7B8": {"nom": "Materials i Tecnologia: Disseny", "cuerpo": "Arts Plàstiques"},
    "7C0": {"nom": "Mitjans Audiovisuals", "cuerpo": "Arts Plàstiques"},
    "7C1": {"nom": "Mitjans Informàtics", "cuerpo": "Arts Plàstiques"},
    "7C2": {"nom": "Organització Industrial i Legislació", "cuerpo": "Arts Plàstiques"},
    "7C4": {"nom": "Volum", "cuerpo": "Arts Plàstiques"},
    "8A6": {"nom": "Esmalts", "cuerpo": "Mestres de Taller"},
    "8A7": {"nom": "Fotografia i Processos de Reproducció", "cuerpo": "Mestres de Taller"},
    "8A8": {"nom": "Modelisme i Maquetisme", "cuerpo": "Mestres de Taller"},
    "8A9": {"nom": "Motles i Reproduccions", "cuerpo": "Mestres de Taller"},
    "8B1": {"nom": "Talla en Pedra i Fusta", "cuerpo": "Mestres de Taller"},
    "8B2": {"nom": "Tècniques Ceràmiques", "cuerpo": "Mestres de Taller"},
    "8B3": {"nom": "Tècniques de Gravat i Estampació", "cuerpo": "Mestres de Taller"},
    "8B4": {"nom": "Tècniques de Joieria i Bijuteria", "cuerpo": "Mestres de Taller"},
    "8B6": {"nom": "Tècniques de Patronatge i Confecció", "cuerpo": "Mestres de Taller"},
    "8B9": {"nom": "Tècniques Tèxtils", "cuerpo": "Mestres de Taller"},
    "401": {"nom": "Alemany", "cuerpo": "EOI"},
    "402": {"nom": "Àrab", "cuerpo": "EOI"},
    "404": {"nom": "Xinès", "cuerpo": "EOI"},
    "406": {"nom": "Espanyol per a Estrangers", "cuerpo": "EOI"},
    "407": {"nom": "Euskera", "cuerpo": "EOI"},
    "408": {"nom": "Francés", "cuerpo": "EOI"},
    "410": {"nom": "Grec Modern", "cuerpo": "EOI"},
    "411": {"nom": "Anglès", "cuerpo": "EOI"},
    "412": {"nom": "Italià", "cuerpo": "EOI"},
    "413": {"nom": "Japonès", "cuerpo": "EOI"},
    "414": {"nom": "Neerlandès", "cuerpo": "EOI"},
    "415": {"nom": "Portuguès", "cuerpo": "EOI"},
    "416": {"nom": "Romanès", "cuerpo": "EOI"},
    "417": {"nom": "Rus", "cuerpo": "EOI"},
    "418": {"nom": "Valencià", "cuerpo": "EOI"},
    "420": {"nom": "Polonès", "cuerpo": "EOI"},
    "421": {"nom": "Finès", "cuerpo": "EOI"},
}


def _norm_text(s: str) -> str:
    """Normalitza per comparar: sense diacrítics, majúscules, sense puntuació especial."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("·", "").replace("'", "").replace("'", "").replace("'", "")
    return s.upper().strip()


# Índex invers nom→codi (només nom valencià del catàleg + variants
# castellanes equivalents per als més comuns que poden aparèixer als PDFs).
_NOMS_INDEX: dict[str, str] = {}
for _codi, _info in CATALEG.items():
    _NOMS_INDEX[_norm_text(_info["nom"])] = _codi

# Variants de nom comunes en castellà que els PDFs poden incloure quan
# Conselleria publica en bilingüe. Mantingut curt — l'identificació
# principal és per CODI, el nom només valida.
_VARIANTS_CASTELLA: dict[str, str] = {
    "EDUCACION INFANTIL": "120",
    "LENGUA EXTRANJERA: INGLES": "121",
    "LENGUA EXTRANJERA: FRANCES": "122",
    "EDUCACION FISICA": "123",
    "MUSICA": "124",
    "AUDICION Y LENGUAJE": "126",
    "PEDAGOGIA TERAPEUTICA": "127",
    "EDUCACION PRIMARIA": "128",
    "FILOSOFIA": "201",
    "GRIEGO": "202",
    "LATIN": "203",
    "LENGUA CASTELLANA Y LITERATURA": "204",
    "GEOGRAFIA E HISTORIA": "205",
    "MATEMATICAS": "206",
    "FISICA Y QUIMICA": "207",
    "BIOLOGIA Y GEOLOGIA": "208",
    "DIBUJO": "209",
    "FRANCES": "210",
    "INGLES": "211",
    "ALEMAN": "212",
    "ITALIANO": "213",
    "PORTUGUES": "215",
    "TECNOLOGIA": "219",
    "ECONOMIA": "261",
    "INFORMATICA": "254",
    "DISENO GRAFICO": "7B1",
    "MATERIALES Y TECNOLOGIA: DISENO": "7B8",
    "MEDIOS INFORMATICOS": "7C1",
    "DIBUJO ARTISTICO Y COLOR": "7A6",
    "PRODUCCION DE ARTES GRAFICAS": "3A8",
}
for _nom_cast, _codi in _VARIANTS_CASTELLA.items():
    _NOMS_INDEX[_nom_cast] = _codi


def nom_oficial(codi: str) -> Optional[str]:
    """Retorna el nom oficial (en valencià, com publica Conselleria)."""
    info = CATALEG.get(codi.upper())
    return info["nom"] if info else None


def codi_des_de_nom(nom: str) -> Optional[str]:
    return _NOMS_INDEX.get(_norm_text(nom))


def es_especialitat_valida(codi: str) -> bool:
    return codi.upper() in CATALEG


def cuerpo_de_codi(codi: str) -> Optional[str]:
    info = CATALEG.get(codi.upper())
    return info["cuerpo"] if info else None


# ════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ════════════════════════════════════════════════════════════════════════

@dataclass
class EntradaBolsa:
    posicio: int
    nom: str
    codiEspecialitat: str
    nomEspecialitat: str
    cuerpo: str                  # "mestres" / "secundaria" / "fp" / ...
    estat: str                   # "adjudicat" / "pendent"
    tipusSubstitucio: Optional[str] = None
    localitat: Optional[str] = None
    codiCentre: Optional[str] = None
    nomCentre: Optional[str] = None
    nomEspPuesto: Optional[str] = None
    codiPuesto: Optional[str] = None
    jornada: Optional[str] = None


@dataclass
class PuestoDocente:
    codigoEspecialidad: str
    nombreEspecialidad: str
    centro: str
    municipio: str
    provincia: str
    tipoSubstitucion: Optional[str] = None
    jornada: Optional[str] = None
    fechaCese: Optional[str] = None
    observaciones: Optional[str] = None


@dataclass
class DocumentGVA:
    filename: str
    tipus: str                   # "lis_mae", "lis_sec", "pue_prov", ...
    publishedDate: str           # ISO 8601
    entrades: list[EntradaBolsa] = field(default_factory=list)
    puestos: list[PuestoDocente] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "tipus": self.tipus,
            "publishedDate": self.publishedDate,
            "entrades": [asdict(e) for e in self.entrades],
            "puestos":  [asdict(p) for p in self.puestos],
            "totalEntrades": len(self.entrades),
        }


@dataclass
class EntradaBolsaInicial:
    """Entrada d'una bolsa de inici de curs (par_def_int_lis_*)."""
    nom: str
    codiEspecialitat: str
    nomEspecialitat: str
    cuerpo: str
    posicio: int
    teServeisPrestats: bool


@dataclass
class DocumentBolsaInicial:
    filename: str
    publishedDate: str
    entrades: list[EntradaBolsaInicial] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "publishedDate": self.publishedDate,
            "entrades": [asdict(e) for e in self.entrades],
            "totalEntrades": len(self.entrades),
        }


# ════════════════════════════════════════════════════════════════════════
# REGEX
# ════════════════════════════════════════════════════════════════════════

# Detecta capçalera amb codi al FINAL (lis_sec format real):
#   "MANTENIMENT DE VEHICLES 3A4"
#   "OP . I EQU. D ELABORACIÓ DE PRODUCTES ALIMENTARIS 2A8"
#   "MATEMÀTIQUES 206"
RE_ESP_HEADER_TAIL = re.compile(r"^(.+?)\s+([0-9]{3}|[0-9][A-Z][0-9])\s*$")

# Detecta capçalera amb codi entre parèntesis al final (bolsa inicial sec):
#   "Col·lectiuCUINA I PASTISSERIA (3A1)"
#   "Col·lectiuINSTAL. I MANT. D'EQUIPS TÈRMICS I DE FLUIDS (2A1)"
RE_ESP_HEADER_PAREN = re.compile(r"^Col[·\.]?lectiu\s*(.+?)\s*\(([0-9]{3}|[0-9][A-Z][0-9])\)\s*$")

# Detecta capçalera amb codi al davant (pue_prov):
#   "ESPECIALIDAD/ESPECIALITAT: 3A4 - MANTENIMIENTO DE VEHÍCULOS"
RE_ESP_HEADER_PUE = re.compile(
    r"^ESPECIALIDAD/ESPECIALITAT:\s*([0-9]{3}|[0-9][A-Z][0-9])\s*-\s*(.+?)\s*$"
)

# Detecta capçalera de format DIFÍCIL COBERTURA (pue_par i pue_prov de divendres).
# Quan pypdf extreu el text d'un PDF DC, l'ordre es desordena i el codi
# apareix DAVANT del text "ESPECIALIDAD" (sense guió). Exemple:
#   "2A2 ESPECIALIDAD/ESPECIALITAT: INSTALACIONES ELECTROTÉCNICAS"
RE_ESP_HEADER_PUE_DC = re.compile(
    r"^([0-9]{3}|[0-9][A-Z][0-9])\s+ESPECIALIDAD/ESPECIALITAT:\s*(.+?)\s*$"
)

# Cuerpo de bolsa/lis_sec, format text d'una línia:
RE_CUERPO_TEXT = re.compile(
    r"^(PROFESSORS|MAESTROS|MESTRES|CATEDR\u00c0TICS|PROFESSORS D'|"
    r"PROFESSORS ESPECIALISTES|PROFESSORS DE M\u00daSICA|PROFESSORS D'ARTS|"
    r"PROFESSORES DE|PROFESORES DE|CATEDR\u00c1TICOS).*",
    re.IGNORECASE,
)

# Línia d'entrada lis_*: "1 MORENO MORENO, PEDRO" o "100/27 NOM, COGNOMS"
# Captura grup 1 = posició (potser N/M), grup 2 = nom
RE_ENTRADA_LIS = re.compile(
    r"^(\d{1,5}(?:/\d{1,5})?)\s+([A-ZÀ-ÿÑÇ\.\-\sÀ-ý]+,\s*[A-ZÀ-ÿÑÇ\.\-\sÀ-ý]+?)"
    r"(?:\s*\(\s*Esp:\s*[0-9A-Z]+\s*\))?\s*$"
)

# Línia d'entrada bolsa inicial: "1 ALMELA PELLICER, MARIA JOSEFA AMB SERVEIS PRI"
# Estructura: <num> <NOM, COGNOMS> (AMB|SENSE)\s*SERVEIS\s*<resta>
RE_ENTRADA_BOLSA = re.compile(
    r"^(\d{1,6})\s+(.+?)\s+(AMB\s+SERVEIS|SENSE\s+SERVEIS|SENSE\s*SERVEIS|AMB\s*SERVEIS)\s*(.*?)\s*$",
    re.IGNORECASE,
)
# A vegades el text està concatenat: "SENSE SERVEISINF" — versió tolerant
RE_BOLSA_TOLERANT = re.compile(
    r"^(\d{1,6})\s+(.+?)\s+(AMB\s+SERVEIS|SENSE\s+SERVEIS|SENSESERVEIS|AMBSERVEIS)([A-Z\s]*)\s*$",
    re.IGNORECASE,
)

# Línia d'entrada pue_prov: "1 SUSTITUCIÓN INDETERMINADA CHESTE - 46018761 - ..."
# Format: <num> <TIPUS_SUBST> <LOCALITAT> - <CODI_CENTRE> - <NOM_CENTRE> <LLOC> <HORES?> <LING.> [obs]
RE_ENTRADA_PUE = re.compile(
    r"^(\d{1,4})\s+(SUSTITUCI[ÓO]N\s+(?:DETERMINADA|INDETERMINADA)|"
    r"SUBSTITUCI[ÓO]\s+(?:DETERMINADA|INDETERMINADA)|VACANTE|VACANT)\s+"
    r"(.+?)\s*-\s*(\d{8})\s*-\s*(.+?)\s+(\d{6})\s+(.*?)\s*$",
    re.IGNORECASE,
)

# Format DIFÍCIL COBERTURA (pue_par i pue_prov de divendres):
#   "18 ELDA - 03005768 - IES LA TORRETA NO 211258 Sust. Ind."
#   "18 ALACANT - 03002007 - CIPFP POLITÈCNIC MARÍTIM PESQUER DEL MEDITERRANI NO 915499 Vacante"
# Camps: hores, localitat, codi_centre, nom_centre, req(NO/SI), lloc, tipus_subst_abreviat
RE_ENTRADA_PUE_DC = re.compile(
    r"^(\d{1,3})\s+"                          # hores
    r"(.+?)\s*-\s*(\d{8})\s*-\s*(.+?)\s+"     # localitat - codi_centre - nom_centre
    r"(NO|SI)\s+(\d{6})\s+"                    # req lloc
    r"(Sust\.\s+(?:Det|Ind)\.|Vacante|Vacant)" # tipus_subst abreviat
    r"\s*$",
    re.IGNORECASE,
)

# Província (nomes en alguns documents): "València", "Alacant", "Castelló"
# Província — format normal (pue_prov dimarts/dijous):
#   "València", "Alacant", "Castelló"
# Format DC (pue_par i pue_prov de divendres):
#   "PROVÍNCIA/PROVINCIA: ALICANTE", "PROVÍNCIA/PROVINCIA: VALENCIA", "PROVÍNCIA/PROVINCIA: CASTELLÓN"
RE_PROVINCIA = re.compile(r"^(València|Alacant|Castell[óo])\s*$", re.IGNORECASE)
RE_PROVINCIA_DC = re.compile(
    r"^PROV[IÍ]NCIA/PROVINCIA:\s*(ALICANTE|VALENCIA|CASTELL[OÓ]N)\s*$",
    re.IGNORECASE,
)

# Estats lis_*
RE_ESTAT_DESACTIVAT = re.compile(r"^Desactivat\s*$", re.IGNORECASE)
RE_ESTAT_ADJUDICAT = re.compile(r"^Adjudicat\s*$", re.IGNORECASE)
RE_ESTAT_NO_ADJUDICAT = re.compile(r"^No\s+adjudicat\s*$", re.IGNORECASE)
RE_ESTAT_NO_PARTICIPAT = re.compile(r"^No\s+ha\s+participat\s*$", re.IGNORECASE)

# Headers a ignorar
RE_IGNORAR = re.compile(
    r"^(POSICI|POSICIÓN|ORDRE|ORDEN|NOM|NOMBRE|ESTAT|ESTADO|"
    r"PÀGINA|PÁGINA|PAGE|Pàg\s+\d+\s+de\s+\d+|"
    r"GENERALITAT|CONSELLERIA|RESOLUCI[ÓO]|"
    r"Avgda\.|Listado|Llistat|Bolsa|Borsa|"
    r"\d{1,2}/\d{2}/\d{4}|"
    r"ADJUDICACI[ÓO]N?\s+DE\s+PERSONAL|"
    r"Llocs\s+Ofertats|Puestos\s+Ofertados|"
    r"Altres\s+Cossos|Otros\s+Cuerpos|"
    r"PROVINCIA/PROVINCIA|LOCALIDAD\s*/\s*LOCALITAT|"
    r"CUERPO/COS|ESPECIALIDAD/ESPECIALITAT|"
    r"\(\*\)\s*Habilitaci|"
    r"nn\s*/\s*mm|"
    r"^$)",
    re.IGNORECASE,
)


# ════════════════════════════════════════════════════════════════════════
# UTILS DE NORMALITZACIÓ DE NOMS
# ════════════════════════════════════════════════════════════════════════

def _es_nom_persona(s: str) -> bool:
    """Heurística: línia és un nom (COGNOMS, NOM)."""
    if "," not in s:
        return False
    parts = s.split(",", 1)
    if len(parts) != 2:
        return False
    cognoms, nom = parts[0].strip(), parts[1].strip()
    if len(cognoms) < 2 or len(nom) < 2:
        return False
    # Verificació: no comença amb dígit, té només lletres + espais + apòstrofs
    if cognoms[0].isdigit() or nom[0].isdigit():
        return False
    return True


def _detectar_capçalera_esp_lis(line: str) -> Optional[tuple[str, str]]:
    """
    Detecta capçalera d'especialitat al format `lis_*` real:
      "MANTENIMENT DE VEHICLES 3A4" → ('3A4', 'Manteniment de vehicles')
    Retorna (codi, nom_oficial) o None.
    """
    m = RE_ESP_HEADER_TAIL.match(line.strip())
    if not m:
        return None
    nom_pdf = m.group(1).strip()
    codi = m.group(2).upper()
    if not es_especialitat_valida(codi):
        return None
    info = CATALEG[codi]
    return (codi, info["nom"])


def _detectar_capçalera_esp_bolsa(line: str) -> Optional[tuple[str, str]]:
    """
    Detecta capçalera de bolsa inicial:
      "Col·lectiuCUINA I PASTISSERIA (3A1)" → ('3A1', 'Cuina i Pastisseria')
    """
    m = RE_ESP_HEADER_PAREN.match(line.strip())
    if not m:
        return None
    nom_pdf = m.group(1).strip()
    codi = m.group(2).upper()
    if not es_especialitat_valida(codi):
        return None
    info = CATALEG[codi]
    return (codi, info["nom"])


def _detectar_capçalera_esp_pue(line: str) -> Optional[tuple[str, str]]:
    """
    Detecta capçalera al format pue_prov:
      "ESPECIALIDAD/ESPECIALITAT: 3A4 - MANTENIMIENTO DE VEHÍCULOS"  (normal)
      "3A4 ESPECIALIDAD/ESPECIALITAT: MANTENIMIENTO DE VEHÍCULOS"    (DC)
    """
    line_strip = line.strip()
    # Format normal (codi després)
    m = RE_ESP_HEADER_PUE.match(line_strip)
    if not m:
        # Format DC (codi davant)
        m = RE_ESP_HEADER_PUE_DC.match(line_strip)
    if not m:
        return None
    codi = m.group(1).upper()
    if not es_especialitat_valida(codi):
        return None
    info = CATALEG[codi]
    return (codi, info["nom"])


def _extreu_posicio(token: str) -> int:
    """'21' → 21. '21/6' → 21. Si no és vàlid, 0."""
    if "/" in token:
        token = token.split("/", 1)[0]
    return int(token) if token.isdigit() else 0


def _preprocessar_text(text: str) -> list[str]:
    """Línies netes, sense buides."""
    out: list[str] = []
    for ln in text.replace("\r", "").split("\n"):
        ln = ln.strip()
        if ln and len(ln) > 0:
            out.append(ln)
    return out


# ════════════════════════════════════════════════════════════════════════
# PARSER LIS_DOC (lis_mae / lis_sec) - bolsa actualitzada del dia
# ════════════════════════════════════════════════════════════════════════

def parse_lis_doc(text: str, filename: str, tipus: str, published_date: str) -> DocumentGVA:
    """
    Parser per lis_mae i lis_sec.

    Format real (validat contra 260421_lis_sec.pdf i 260120_lis_sec.pdf):
    
    Bloc d'entrada normal:
      <num> <NOM, COGNOMS>
      <Estat>            ← "Desactivat", "No adjudicat", "No ha participat", "Ha participat"
    
    Bloc d'entrada amb adjudicació concreta:
      <NUM_ORDRE>                                      ← p.ex. "26"
      SUBSTITUCIÓ DETERMINADA / INDETERMINADA
      <LOCALITAT>(<CODI_CENTRE>)<NOM_CENTRE>           ← "CANALS(46002571)IES FRANCESC GIL"
      <CODI_ESP> / <NOM_ESP>                           ← "206 / MATEMÀTIQUES"
      <CODI_LLOC>                                      ← "205356"
      Jornada completa / parcial
      <NOM_INTERÍ_ADJUDICAT> <PETICIÓ>                 ← "VAÑO MAESTRE, FRANCISCO Voluntaria"
      Adjudicat
    
    L'interí adjudicat NO té número al davant (perquè el num és al començament del bloc).
    El proper interí del ranking apareix despres com a entrada normal "<num+1> NOM".
    """
    doc = DocumentGVA(filename=filename, tipus=tipus, publishedDate=published_date)
    cuerpo_doc = "Mestres" if "lis_mae" in filename.lower() else None

    lines = _preprocessar_text(text)
    n = len(lines)
    codi_actual = ""
    nom_esp_actual = ""
    cuerpo_actual = cuerpo_doc or ""

    # Per detectar blocs d'adjudicació: número solitari seguit de "SUBSTITUCIÓ"
    re_num_solitari = re.compile(r"^(\d{1,5})\s*$")
    re_subst = re.compile(r"^(SUBSTITUCI[ÓO][N]?|VACANTE|VACANT)\s+(DETERMINADA|INDETERMINADA)\s*$",
                          re.IGNORECASE)
    re_centre_adj = re.compile(
        r"^(.+?)\((\d{8})\)(.+?)\s*$"
    )  # "CANALS(46002571)IES FRANCESC GIL"
    re_esp_lloc = re.compile(
        r"^([0-9]{3}|[0-9][A-Z][0-9])\s*/\s*(.+?)\s*$"
    )  # "206 / MATEMÀTIQUES"
    re_codi_lloc = re.compile(r"^(\d{6})\s*$")  # "205356"
    re_jornada = re.compile(r"^Jornada\s+(.+?)\s*$", re.IGNORECASE)
    re_nom_adjudicat = re.compile(
        r"^([A-ZÀ-ÿÑÇ\.\-\sÀ-ý]+,\s*[A-ZÀ-ÿÑÇ\.\-\sÀ-ý]+?)"
        r"\s+(?:Petici[óo]n:\s*)?(?:Voluntaria|For[zç]osa|Voluntari[oa]s?)\s*\d*\s*$"
    )
    re_estat_ha_participat = re.compile(r"^Ha\s+participat\s*$", re.IGNORECASE)

    i = 0
    while i < n:
        t = lines[i]

        # Saltar headers
        if RE_IGNORAR.match(t):
            i += 1
            continue

        # Detectar línia de cuerpo
        if RE_CUERPO_TEXT.match(t):
            tu = t.upper()
            if "MESTRES" in tu or "MAESTROS" in tu:
                cuerpo_actual = "Mestres"
            elif "ESPECIALISTES" in tu and "SECTORS" in tu:
                cuerpo_actual = "Prof. Esp. FP"
            elif "ENSENYAMENT SECUNDARI" in tu or "ENSEÑANZA SECUNDARIA" in tu:
                cuerpo_actual = "Secundària"
            elif "MÚSICA" in tu or "MUSICA" in tu or "ARTS" in tu:
                cuerpo_actual = "Música i Arts"
            elif "ARTS PLÀSTIQUES" in tu or "PLÁSTICAS" in tu:
                cuerpo_actual = "Arts Plàstiques"
            elif "IDIOMES" in tu or "IDIOMAS" in tu:
                cuerpo_actual = "EOI"
            elif "CATEDR" in tu:
                cuerpo_actual = "Catedràtics MAE"
            else:
                cuerpo_actual = cuerpo_doc or "Secundària"
            i += 1
            continue

        # Detectar capçalera d'especialitat
        cap = _detectar_capçalera_esp_lis(t)
        if cap:
            codi_actual = cap[0]
            nom_esp_actual = cap[1]
            cuerpo_cataleg = cuerpo_de_codi(codi_actual)
            if cuerpo_cataleg:
                cuerpo_actual = cuerpo_cataleg
            i += 1
            continue

        # Per als mestres, una sola llista
        if cuerpo_doc == "Mestres" and not codi_actual:
            codi_actual = "MESTRES"
            nom_esp_actual = "Mestres"
            cuerpo_actual = "Mestres"

        # ─── DETECCIÓ BLOC D'ADJUDICACIÓ ─────────────────────────────────
        # Patró: num_solitari → SUBSTITUCIÓ → centre → esp_lloc → codi_lloc → jornada → nom → Adjudicat
        m_num_sol = re_num_solitari.match(t)
        if m_num_sol and i + 7 < n:
            # Mirem si les pròximes línies casen amb el patró d'adjudicació
            l_subst = lines[i + 1] if i + 1 < n else ""
            l_centre = lines[i + 2] if i + 2 < n else ""
            l_esp = lines[i + 3] if i + 3 < n else ""
            l_codi_lloc = lines[i + 4] if i + 4 < n else ""
            l_jornada = lines[i + 5] if i + 5 < n else ""
            l_nom = lines[i + 6] if i + 6 < n else ""
            l_estat = lines[i + 7] if i + 7 < n else ""

            if (re_subst.match(l_subst) and
                re_centre_adj.match(l_centre) and
                re_esp_lloc.match(l_esp) and
                re_codi_lloc.match(l_codi_lloc) and
                re_jornada.match(l_jornada) and
                l_estat.strip() == "Adjudicat"):
                
                # Extreure dades
                posicio = int(m_num_sol.group(1))
                tipus_subst = l_subst.strip()
                
                m_centre = re_centre_adj.match(l_centre)
                localitat = m_centre.group(1).strip()
                codi_centre = m_centre.group(2)
                nom_centre = m_centre.group(3).strip()
                
                m_esp_lloc = re_esp_lloc.match(l_esp)
                codi_lloc_esp = m_esp_lloc.group(1).upper()
                nom_esp_lloc = m_esp_lloc.group(2).strip()
                
                m_codi = re_codi_lloc.match(l_codi_lloc)
                codi_lloc = m_codi.group(1)
                
                jornada = l_jornada.strip()
                
                # Extreure nom interí (treure "Voluntaria", "Petición: ...", etc.)
                m_nom = re_nom_adjudicat.match(l_nom)
                if m_nom:
                    nom_interi = m_nom.group(1).strip()
                else:
                    # Fallback: prendre tot el text fins el primer "Voluntaria/Forçosa/Petición"
                    nom_interi = re.sub(
                        r"\s+(?:Petici[óo]n:\s*)?(?:Voluntaria|For[zç]osa|Voluntari[oa]s?).*$",
                        "", l_nom
                    ).strip()
                
                if codi_actual and _es_nom_persona(nom_interi):
                    doc.entrades.append(EntradaBolsa(
                        posicio=posicio,
                        nom=nom_interi,
                        codiEspecialitat=codi_actual,
                        nomEspecialitat=nom_esp_actual,
                        cuerpo=cuerpo_actual,
                        estat="adjudicat",
                        tipusSubstitucio=tipus_subst,
                        localitat=localitat,
                        codiCentre=codi_centre,
                        nomCentre=nom_centre,
                        nomEspPuesto=nom_esp_lloc,
                        codiPuesto=codi_lloc,
                        jornada=jornada,
                    ))
                    i += 8  # saltar tot el bloc
                    continue
        # ─── FI DETECCIÓ BLOC D'ADJUDICACIÓ ───────────────────────────────

        # Detectar entrada normal: <num> <NOM, COGNOMS>
        m_ent = RE_ENTRADA_LIS.match(t)
        if m_ent:
            pos_str, nom = m_ent.group(1), m_ent.group(2).strip()
            posicio = _extreu_posicio(pos_str)
            if posicio > 0 and codi_actual and _es_nom_persona(nom):
                # Llegir l'estat de la línia següent
                estat = "pendent"
                avanç_estat = 0
                if i + 1 < n:
                    seg = lines[i + 1]
                    if RE_ESTAT_DESACTIVAT.match(seg):
                        estat = "desactivat"
                        avanç_estat = 1
                    elif RE_ESTAT_ADJUDICAT.match(seg):
                        # Cas raro: "Adjudicat" sol després del nom, sense bloc
                        estat = "adjudicat"
                        avanç_estat = 1
                    elif RE_ESTAT_NO_ADJUDICAT.match(seg):
                        estat = "no_adjudicat"
                        avanç_estat = 1
                    elif RE_ESTAT_NO_PARTICIPAT.match(seg):
                        estat = "no_participat"
                        avanç_estat = 1
                    elif re_estat_ha_participat.match(seg):
                        estat = "ha_participat"
                        avanç_estat = 1

                doc.entrades.append(EntradaBolsa(
                    posicio=posicio,
                    nom=nom,
                    codiEspecialitat=codi_actual,
                    nomEspecialitat=nom_esp_actual,
                    cuerpo=cuerpo_actual,
                    estat=estat,
                ))
                i += 1 + avanç_estat
                continue

        i += 1

    return doc


# ════════════════════════════════════════════════════════════════════════
# PARSER PUE_PROV (puestos provisionals)
# ════════════════════════════════════════════════════════════════════════

def parse_pue_prov(text: str, filename: str, tipus: str, published_date: str) -> DocumentGVA:
    """
    Format real (validat contra 260421_pue_prov.pdf):
      ESPECIALIDAD/ESPECIALITAT: 3A4 - MANTENIMIENTO DE VEHÍCULOS
      PROVINCIA/PROVINCIA:
      LOCALIDAD / LOCALITAT - CENTRO / CENTRE TIPUS/TIPO LLOC OBSERV./OBSERV. ITI/COMP. HORES.REQ. LING.
      València
      1 SUSTITUCIÓN INDETERMINADA CHESTE - 46018761 - CIPFP COMPLEJO EDUCATIVO DE CHESTE 876774 NO
      2 SUSTITUCIÓN DETERMINADA VALÈNCIA - 46023547 - IES BENICALAP 212863 NO
    """
    doc = DocumentGVA(filename=filename, tipus=tipus, publishedDate=published_date)
    lines = _preprocessar_text(text)
    n = len(lines)
    codi_actual = ""
    nom_esp_actual = ""
    provincia_actual = ""
    
    i = 0
    while i < n:
        t = lines[i]
        
        # Capçalera d'especialitat
        cap = _detectar_capçalera_esp_pue(t)
        if cap:
            codi_actual = cap[0]
            nom_esp_actual = cap[1]
            i += 1
            continue
        
        # Província (format normal o DC)
        if RE_PROVINCIA.match(t):
            provincia_actual = t.strip()
            i += 1
            continue
        m_prov_dc = RE_PROVINCIA_DC.match(t)
        if m_prov_dc:
            # Mapeig majúscules → forma habitual
            prov_norm = {
                "ALICANTE":  "Alacant",
                "VALENCIA":  "València",
                "CASTELLÓN": "Castelló",
                "CASTELLON": "Castelló",
            }.get(m_prov_dc.group(1).upper(), m_prov_dc.group(1))
            provincia_actual = prov_norm
            i += 1
            continue
        
        # Saltar headers
        if RE_IGNORAR.match(t):
            i += 1
            continue
        
        # Entrada (format normal o DC)
        m_pue = RE_ENTRADA_PUE.match(t)
        m_pue_dc = None
        if not m_pue:
            m_pue_dc = RE_ENTRADA_PUE_DC.match(t)

        if m_pue and codi_actual:
            num_ord, tipus_subst, localitat, codi_centre, nom_centre, lloc, resta = m_pue.groups()
            # `resta` pot incloure HORES (ex. "11,5"), LING ("ING."), OBSERV ("Centre singular...")
            # i acabar amb "NO" o "SI" (HORES.REQ.LING) — analitzem-ho mínimament
            tokens = resta.strip().split()
            jornada = None
            ling = None
            obs = []
            for tok in tokens:
                if "," in tok and tok.replace(",", "").replace(".", "").isdigit():
                    jornada = tok  # ex: "11,5"
                elif tok.upper() in ("ING.", "FRA.", "VAL.", "ALE.", "ITA.", "POR."):
                    ling = tok
                elif tok.upper() in ("NO", "SI"):
                    pass  # flag ITI/COMP, ignorat
                else:
                    obs.append(tok)
            obs_str = " ".join(obs) if obs else None

            doc.puestos.append(PuestoDocente(
                codigoEspecialidad=codi_actual,
                nombreEspecialidad=nom_esp_actual,
                centro=nom_centre.strip(),
                municipio=localitat.strip(),
                provincia=provincia_actual,
                tipoSubstitucion=tipus_subst.strip(),
                jornada=jornada,
                fechaCese=None,
                observaciones=obs_str,
            ))
        elif m_pue_dc and codi_actual:
            # Format DIFÍCIL COBERTURA
            hores, localitat, codi_centre, nom_centre, req, lloc, tipus_subst_abrev = m_pue_dc.groups()
            # Mapejar tipus subst abreviat a forma completa (consistent amb format normal)
            tipus_map = {
                "sust. ind.": "SUSTITUCIÓN INDETERMINADA",
                "sust. det.": "SUSTITUCIÓN DETERMINADA",
                "vacante":    "VACANTE",
                "vacant":     "VACANT",
            }
            tipus_norm = tipus_map.get(tipus_subst_abrev.lower().strip(), tipus_subst_abrev.strip())

            doc.puestos.append(PuestoDocente(
                codigoEspecialidad=codi_actual,
                nombreEspecialidad=nom_esp_actual,
                centro=nom_centre.strip(),
                municipio=localitat.strip(),
                provincia=provincia_actual,
                tipoSubstitucion=tipus_norm,
                jornada=hores,
                fechaCese=None,
                observaciones="DIFÍCIL COBERTURA",
            ))
        i += 1
    
    return doc


# ════════════════════════════════════════════════════════════════════════
# PARSER BOLSA INICIAL (par_def_int_lis_*)
# ════════════════════════════════════════════════════════════════════════

def parse_bolsa_inicial(text: str, filename: str, published_date: str) -> DocumentBolsaInicial:
    """
    Parser per bolsas de inici de curs (par_def_int_lis_*.pdf).
    
    Format real bolsa MESTRES (validat contra ini_2025_par_def_int_lis_mae.pdf):
      Cuerpo unic = mestres, sense capceleres d'especialitat.
      Per cada interi una linia:
        "1 ALMELA PELLICER, MARIA JOSEFA AMB SERVEIS PRI"
        "5 SOLDADO RIBES, LUISA ROSA AMB SERVEIS INF PRI ING FRA"
        "15401 GALINDO NIEVES, MARTA SENSE SERVEISINF"  (text concatenat!)
      Les abreviacions al final son habilitacions, no especialitats actives.
      L'especialitat principal pot derivar-se: PRI (Prim), INF (Inf), ING (Anglès), 
      FRA (Francès), AL (Audició), PT (Pedag. Terapèut.), MUS (Música), EF (Ed. Físic).
      
      RESOLUCIÓ: per als mestres, NO tenim posició per especialitat, sino una posició 
      global a la bolsa de mestres. Marquem totes les habilitacions com a entrades 
      separades amb la mateixa posicio global.
    
    Format real bolsa SECUNDÀRIA (ini_2025_par_def_int_lis_sec.pdf):
      Té capçaleres d'especialitat tipus:
        "Col·lectiuCUINA I PASTISSERIA (3A1)"
      Per cada entrada:
        "1 MASIA VALLES, ELISABET AMB SERVEIS"
        "4 FERRANDO BADENES, OSCAR AMB SERVEIS (*)"  ((*) = habilitacio desactivada)
    """
    doc = DocumentBolsaInicial(filename=filename, publishedDate=published_date)
    lines = _preprocessar_text(text)
    n = len(lines)
    
    es_mestres = "lis_mae" in filename.lower()
    
    # ────────── Parsejat MESTRES (bolsa única) ──────────
    if es_mestres:
        for ln in lines:
            if RE_IGNORAR.match(ln):
                continue
            # Provem regex tolerant primer (cas concatenat)
            m = RE_BOLSA_TOLERANT.match(ln)
            if not m:
                m = RE_ENTRADA_BOLSA.match(ln)
            if not m:
                continue
            num_str = m.group(1)
            nom = m.group(2).strip()
            servei_text = m.group(3).upper().replace(" ", "")
            te_serveis = "AMB" in servei_text and "SENSE" not in servei_text
            habilitacions_str = m.group(4).strip() if len(m.groups()) >= 4 else ""
            
            if not _es_nom_persona(nom):
                continue
            
            try:
                posicio = int(num_str)
            except ValueError:
                continue
            
            # Habilitacions: divides per espais, codis tipus "PRI", "INF", "ING", etc.
            # Mapeig a codis del catàleg:
            mapeig_habilitacions = {
                "INF": "120", "PRI": "128", "ING": "121", "FRA": "122",
                "EF": "123", "MUS": "124", "AL": "126", "PT": "127",
            }
            habilitacions = []
            for h in habilitacions_str.split():
                h = h.strip()
                if h in mapeig_habilitacions:
                    habilitacions.append(mapeig_habilitacions[h])
            
            # Si no s'identifiquen, posem ALMENYS PRI (Educació Primària)
            if not habilitacions:
                habilitacions = ["128"]
            
            # Una entrada per cada especialitat habilitada
            for codi_esp in habilitacions:
                info = CATALEG[codi_esp]
                doc.entrades.append(EntradaBolsaInicial(
                    nom=nom,
                    codiEspecialitat=codi_esp,
                    nomEspecialitat=info["nom"],
                    cuerpo=info["cuerpo"],
                    posicio=posicio,
                    teServeisPrestats=te_serveis,
                ))
        return doc
    
    # ────────── Parsejat SECUNDÀRIA (per especialitat) ──────────
    codi_actual = ""
    nom_esp_actual = ""
    cuerpo_actual = ""
    
    i = 0
    while i < n:
        t = lines[i]
        
        if RE_IGNORAR.match(t):
            i += 1
            continue
        
        # Detectar capçalera de cuerpo (PROFESSORS...)
        if RE_CUERPO_TEXT.match(t):
            tu = t.upper()
            if "ESPECIALISTES" in tu and "SECTORS" in tu:
                cuerpo_actual = "Prof. Esp. FP"
            elif "ENSENYAMENT SECUNDARI" in tu:
                cuerpo_actual = "Secundària"
            elif "MÚSICA" in tu or "ARTS" in tu:
                cuerpo_actual = "Música i Arts"
            elif "PLÀSTIQUES" in tu or "PLASTICAS" in tu:
                cuerpo_actual = "Arts Plàstiques"
            elif "IDIOMES" in tu:
                cuerpo_actual = "EOI"
            elif "CATEDR" in tu:
                cuerpo_actual = "Catedràtics MAE"
            i += 1
            continue
        
        # Detectar capçalera d'especialitat amb parèntesis
        cap = _detectar_capçalera_esp_bolsa(t)
        if cap:
            codi_actual = cap[0]
            nom_esp_actual = cap[1]
            cuerpo_cataleg = cuerpo_de_codi(codi_actual)
            if cuerpo_cataleg:
                cuerpo_actual = cuerpo_cataleg
            i += 1
            continue
        
        # Detectar entrada
        m = RE_ENTRADA_BOLSA.match(t)
        if not m:
            m = RE_BOLSA_TOLERANT.match(t)
        if m and codi_actual:
            num_str = m.group(1)
            nom = m.group(2).strip()
            servei_text = m.group(3).upper().replace(" ", "")
            te_serveis = "AMB" in servei_text and "SENSE" not in servei_text
            
            if _es_nom_persona(nom):
                try:
                    posicio = int(num_str)
                    doc.entrades.append(EntradaBolsaInicial(
                        nom=nom,
                        codiEspecialitat=codi_actual,
                        nomEspecialitat=nom_esp_actual,
                        cuerpo=cuerpo_actual,
                        posicio=posicio,
                        teServeisPrestats=te_serveis,
                    ))
                except ValueError:
                    pass
        i += 1
    
    return doc


# ════════════════════════════════════════════════════════════════════════
# DISPATCHER
# ════════════════════════════════════════════════════════════════════════

def parse_document(text: str, filename: str, tipus: str, published_date: str = "") -> DocumentGVA:
    """Despatxador segons el tipus."""
    if tipus in ("lis_mae", "lis_sec"):
        return parse_lis_doc(text, filename, tipus, published_date)
    elif tipus in ("pue_prov", "pue_par"):
        return parse_pue_prov(text, filename, tipus, published_date)
    elif tipus == "par":
        return parse_lis_doc(text, filename, tipus, published_date)
    else:
        return DocumentGVA(filename=filename, tipus=tipus, publishedDate=published_date)
