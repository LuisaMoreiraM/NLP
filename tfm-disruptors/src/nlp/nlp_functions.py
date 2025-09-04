import json
import spacy
import pandas as pd
from spacy.pipeline import EntityRuler
import re
from plotnine import ggplot, aes, geom_bar, geom_text, facet_wrap, theme, element_text, labs, after_stat
from plotnine.positions import position_stack, position_fill

### 1. Funciones de normalización

def unify_dashes(s: str) -> str:
    """
    Unifica guiones “raros” a '-' ASCII
    """
    return (s.replace("‐", "-").replace("–", "-").replace("—", "-") .replace("−", "-"))

# Tokenizador no nlp. Crea el patrón para los tokens con las características puestas (conserva alfanuméricos, guiones y comas)
# (?:..)* estamos diciendo que repita 0 o más veces lo que está dentro del paréntesis
# debe ir fuera para no tner que compilar cada vez
_token_re = re.compile(r"[A-Za-z0-9]+(?:[-,][A-Za-z0-9]+)*") 

def get_tokens(text):
    """
    Devuelve tokens conservando letras/números y conectores.
    No requiere `nlp`.
    """
    text = unify_dashes(str(text))
    return _token_re.findall(text)

def to_lowercase(words):
    """
    Función que dada una lista de palabras, las transforma a minúsculas
    """
    return [word.lower() for word in words]

# Limpieza de tokens, pero manteniendo alfanuméricos, guiones y comas
def no_symbols(words):
    """
    Elimina todos los símbolos excepto números, comas, guiones, por cadena vacía
    """
    return [re.sub(r"[^a-zA-Z0-9,\-]", "", word) for word in words if word]

def normalize(text: str) -> str:
    """
    Normalización que:
    - Unifica guiones
    - Tokeniza sin `nlp`
    - Minúsculas
    - Elimina símbolos restantes
    """
    words = get_tokens(text)
    words = to_lowercase(words)
    words = no_symbols(words)
    return " ".join(words)


def normalize_for_pattern(s: str):
    """Tokens en minúsculas, quita espacios al inicio y al final, split por espacio o guion. No normalizamos más aquí para conservar forma."""
    s = unify_dashes((s or "").strip().lower())
    return [t for t in re.split(r"[\s\-]+", s) if t]

### 2. Funciones de creación de patrones

# Utilidades para la función construcción de patrones
def make_ent_id(cas: str, ec: str) -> str:
    cas = "" if cas is None else str(cas).strip()
    ec  = "" if ec  is None else str(ec).strip()
    if cas and ec: return f"CAS:{cas}|EC:{ec}"
    if cas:        return f"CAS:{cas}"
    if ec:         return f"EC:{ec}"
    return "" # sin CAS ni EC para no poner None 


def regex_compact_phrase(tokens):
    """
    Construye una REGEX para un solo token que representa toda la frase,
    insertando -? entre todas las piezas: ^t1-?t2-?...-?tn$
    Sirve para cazar 'butylatedhydroxytoluene', 'butylated-hydroxytoluene' o 'butylated-hydroxy-toluene'
    si vienen como UN SOLO TOKEN.
    """
    if not tokens:
        return None
    pieces = [re.escape(t) for t in tokens] # escape para que los caracteres especiales se traten como caracteres literales del patrón
    return "^" + "-?".join(pieces) + "$"

def sequence_with_optional_dashes_between_tokens(tokens):
    """
    Construye una secuencia de patrones spaCy que permite guiones opcionales
    ENTRE tokens (aunque el alias original no los tuviera).
    Así caza: "butylated hydroxy toluene" y "butylated-hydroxy-toluene".
    """
    seq = []
    for i, t in enumerate(tokens):
        seq.append({"LOWER": t})
        if i < len(tokens) - 1: # si no es el último token
            seq.append({"ORTH": "-", "OP": "?"})
    return seq


# Construcción de patrones
def build_patterns_from_df(df):
    """
    Construye patrones para el EntityRuler a partir de un DataFrame con columnas:
    ['CAS Number','EC Number','nombre_etiqueta'].

    Genera tres tipos de patrones por alias:
      A) Multi-token exacto: [{"LOWER": t1}, {"LOWER": t2}, ...]
      B) REGEX de UN SOLO token, cubre tokens separados por: espacios/sin espacios/guiones
      C) Multi-token con guion opcional entre subpartes, cubre casos como '4-methyl…' y 'p-methoxy…'
         cuando spaCy tokeniza como ["4","-","methyl…"] o ["p","-","methoxy…"]).
    """

    pats = []
    seen = set()  # para evitar duplicados exactos los vamos guardando aquí, lo que meta aquí tiene que ser tupla porque es hashable

    # Trabajamos con las 3 columnas necesarias
    base = df[["CAS Number", "EC Number", "nombre_etiqueta"]].copy()
    base["EC Number"] = base["EC Number"].fillna("")


    for (cas, ec), group in base.groupby(["CAS Number", "EC Number"]):
        ent_id = make_ent_id(cas, ec)
        for name in group["nombre_etiqueta"].dropna().unique(): # no hay valores nulos pero por si acaso lo pongo
            toks = normalize_for_pattern(name) # normalización ligera
            if not toks:
                continue

            # (A) Multi-token exacto
            signatureA = ("A", ent_id, tuple("LOWER:" + t for t in toks))
            if signatureA not in seen:
                patternA = [{"LOWER": t} for t in toks]
                pats.append({"label":"DISRUPTOR","pattern":patternA,"id":ent_id})
                seen.add(signatureA)

            # B) REGEX de un solo token con -? entre todos
            if len(toks) >= 2:
                regex = regex_compact_phrase(toks)
                signatureB = ("B", ent_id, regex)
                if signatureB not in seen and regex:
                    patternB = [{"LOWER": {"REGEX": regex}}]
                    pats.append({"label":"DISRUPTOR","pattern":patternB,"id":ent_id})
                    seen.add(signatureB)
            
            # C) multi-token con guion opcional ENTRE tokens
            if len(toks) >= 2:
                patternC = sequence_with_optional_dashes_between_tokens(toks)
                sig_elems = []
                for i, t in enumerate(toks):
                    sig_elems.append(f"LOWER:{t}")
                    if i < len(toks) - 1:
                        sig_elems.append("ORTH:-?")
                signatureC = ("C", ent_id, tuple(sig_elems))
                if signatureC not in seen:
                    pats.append({"label":"DISRUPTOR","pattern":patternC,"id":ent_id})
                    seen.add(signatureC)


    return pats


### 3. Funciones para evaluación de algoritmo

def match_row(row):
    """
    Función para hacer match de una fila con las entidades detectadas.
    Con zip emparejamos las entidades detectadas con sus IDs, normalizaciones y versiones compactas.
    """
    # a) por ID (si tenemos CAS/EC)
    target_id = make_ent_id(row["CAS Number"], row["EC Number"])
    if target_id:
        for entity, id in zip(row["detected_entities"], row["detected_ids"]):
            if id == target_id:
                return 1, "id", entity

    return 0, "none", None


### 4. Funciones para visualización

def facet_bar_pct(
    df, xcol, fill_col, facet_col,
    alpha=0.6, edge_color="blue",
    label_size=9, 
    nudge_y=0,
    rotate=30, order=None,
    title=None,
    fig_width=14, fig_height=8,
    group=0,
    title_size= 12
    #bar_text=0.5 desactivo porque antes lo ponía dentro de vjust y no funciona cuando llamo a la función
):
    """
    Crea un gráfico de barras facetado por una variable categórica.
    """

    plot = (
        ggplot(df)
        + aes(x=xcol, fill=fill_col)
        + geom_bar(alpha=alpha, color=edge_color)
        + geom_text(
            aes(label=after_stat("count / sum(count) * 100"), group=group),
            stat="count",
            position=position_stack(vjust=0.5),
            size=label_size,
            nudge_y=nudge_y,
            format_string="{:.1f}%"
        )
        + facet_wrap(f"~ {facet_col}")
        + theme(
            axis_text_x=element_text(rotation=rotate, ha="right"), 
            figure_size=(fig_width, fig_height)
        )
        + labs(
            x=xcol, y=fill_col,
            title=title or f"Distribución por {xcol} con la variable {facet_col}",
            size=title_size
        )
    )

    return plot


### 5. FUNCIONES DESACTIVADAS que usaba en evaluación


# def norm_compact(s: str) -> str:
#     """ 
#     Quita espacios y guiones a normalize, nos va a servir para hacer evaluaciones.
#     """
#     return normalize(s).replace("-", "").replace(" ", "")

# def norm_series(s: pd.Series) -> list:
#     """
#     Normaliza una serie de pandas y devuelve una lista ordenada de valores únicos.
#     """
#     return sorted({normalize(x) for x in s.dropna().astype(str)})


# # agrupamos por CAS y EC y sacamos solo nombre_etiqueta
# # Se utiliza en la función alias_set_for_row
# alias_por_sustancia = (disruptores_final
#                        .groupby(["CAS Number", "EC Number"])["nombre_etiqueta"]
#                        .apply(norm_series)
#                        .to_dict())
# print("Ejemplo alias_por_sustancia:", next(iter(alias_por_sustancia.items())))


# # Funciones de match por fila (prioridad: ID > alias exacto > alias compacto)

# def alias_set_for_row(row):
#     key = (str(row["CAS Number"]), str(row["EC Number"]) if pd.notna(row["EC Number"]) else "")
#     return set(alias_por_sustancia.get(key, []))


# def match_row(row):
#     """
#     Función para hacer match de una fila con las entidades detectadas.
#     Con zip emparejamos las entidades detectadas con sus IDs, normalizaciones y versiones compactas.
#     """
#     # a) por ID (si tenemos CAS/EC)
#     target_id = make_ent_id(row["CAS Number"], row["EC Number"])
#     if target_id:
#         for entity, id in zip(row["detected_entities"], row["detected_ids"]):
#             if id == target_id:
#                 return 1, "id", entity

#     # b) por alias exacto (texto normalizado)
#     alias = alias_set_for_row(row) 
#     if alias:
#         for entity_norm, entity in zip(row["detected_norm"], row["detected_entities"]):
#             if entity_norm in alias:
#                 return 1, "alias_exact", entity

#     # c) por alias compacto (sin espacios ni guiones)
#     alias_compact = {norm_compact(a) for a in alias}
#     if alias_compact:
#         for entity_compact, entity in zip(row["detected_compact"], row["detected_entities"]):
#             if entity_compact in alias_compact:
#                 return 1, "alias_compact", entity

#     return 0, "none", None