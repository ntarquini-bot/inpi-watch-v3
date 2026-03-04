import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urljoin

import pandas as pd
import requests
from rapidfuzz import fuzz, process
import pdfplumber


INPI_BOLETINES_URL = "https://portaltramites.inpi.gob.ar/Boletines?Tipo_Item=3"
CSV_PATH = "Marcas registradas.csv"

FUZZY_ALERT = 92
FUZZY_REVIEW = 88

CORE_TERMS_MANUAL = ["TARQUINI"]

GENERIC_TOKENS = {
    "EL","LA","LOS","LAS","DE","DEL","Y","EN","AL","A",
    "COLOR","COLORES","NATURAL","MICRO","BASE","PINTURA","REVESTIMIENTO",
    "SA","S.A","S A","SRL","S.R.L","SOCIEDAD","ANONIMA","ARGENTINA",
    "INTERIOR","EXTERIOR","BLANCO","NEGRO","GRIS","MATE","SATINADO",
    "LAVABLE","ACRILICO","LATEX","SELLADOR","FIJADOR","IMPRIMACION"
}


def normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Z0-9\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(s: str):
    toks = re.split(r"[\s\-]+", normalize(s))
    return [t for t in toks if t and t not in GENERIC_TOKENS and len(t) >= 3]


def build_core_terms_from_watchlist(watch_all: list[str], min_len: int = 5):
    core = set(CORE_TERMS_MANUAL)
    for m in watch_all:
        for tok in tokenize(m):
            if len(tok) >= min_len and tok not in GENERIC_TOKENS:
                core.add(tok)
    return sorted(core)


def contains_core_terms(s: str, core_terms: list[str]) -> bool:
    ns = normalize(s)
    return any(term in ns for term in core_terms)


def read_csv_safely(path: str) -> pd.DataFrame:
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(
                path,
                dtype=str,
                keep_default_na=False,
                encoding=enc,
                sep=None,
                engine="python"
            )
            print(f"[CSV] OK encoding={enc} cols={len(df.columns)} rows={len(df)}")
            return df
        except Exception as e:
            last_err = e
    raise last_err


def load_watchlist():
    df = read_csv_safely(CSV_PATH)

    marca_col = None
    for c in df.columns:
        cu = str(c).upper()
        if "MARCA" in cu or "DENOMIN" in cu or "SIGNO" in cu:
            marca_col = c
            break
    if marca_col is None:
        marca_col = df.columns[0]

    df["__marca__"] = df[marca_col].map(normalize)
    watch_all = sorted(set(df["__marca__"].tolist()))
    return df, watch_all


def fetch_boletin_pdf_links():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36"
    }
    r = requests.get(INPI_BOLETINES_URL, timeout=30, headers=headers)
    r.raise_for_status()
    html = r.text

    base = "https://portaltramites.inpi.gob.ar"
    urls = re.findall(r'''(?:href|src)\s*=\s*["']([^"']+)["']''', html, flags=re.IGNORECASE)

    pdf_like = []
    for u in urls:
        u_low = u.lower()
        if ".pdf" in u_low or ("uploads/boletines" in u_low) or ("download" in u_low and "bolet" in u_low):
            pdf_like.append(urljoin(base, u))

    return sorted(set(pdf_like)), html


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    chunks = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return "\n".join(chunks)


def parse_candidates_from_text(text: str):
    candidates = []
    for line in text.splitlines():
        ln = line.strip()
        if not ln or len(ln) > 220:
            continue
        alpha = sum(ch.isalpha() for ch in ln)
        if alpha >= 6:
            candidates.append((ln, normalize(ln)))
    return candidates


def best_fuzzy_match(candidate_norm: str, choices: list[str]):
    if not candidate_norm or not choices:
        return ("", 0)
    match, score, _ = process.extractOne(candidate_norm, choices, scorer=fuzz.WRatio)
    return match, score


def dedup(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r["marca_solicitada"], r["fuente"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def main():
    run_dt = datetime.now(timezone(timedelta(hours=-3)))
    os.makedirs("out", exist_ok=True)

    _, watch_all = load_watchlist()
    core_terms_auto = build_core_terms_from_watchlist(watch_all, min_len=5)

    pdf_links, boletines_html = fetch_boletin_pdf_links()

    # Guardar HTML SIEMPRE
    with open("out/boletines_page.html", "w", encoding="utf-8") as f:
        f.write(boletines_html)

    # Si no hay PDFs, NO fallar: dejar reporte + html para debug
    if not pdf_links:
        with open("out/reporte_inpi.md", "w", encoding="utf-8") as f:
            f.write("# Reporte INPI – Vigilancia de marcas\n\n")
            f.write(f"**Fecha/hora (AR):** {run_dt.strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write("## Error\n\n")
            f.write("No se encontraron links a PDFs en la página de boletines.\n\n")
            f.write("Se adjunta `boletines_page.html` para ajustar el extractor.\n")
        print("[INPI] No PDFs found. Report + HTML generated.")
        return

    # Por seguridad: revisar últimos 2
    pdf_links = pdf_links[-2:]

    alerts, review, scanned = [], [], []

    for link in pdf_links:
        pdf_bytes = requests.get(link, timeout=60).content
        text = extract_text_from_pdf(pdf_bytes)

        if len(text.strip()) < 200:
            scanned.append(link)
            continue

        for raw_line, cand_norm in parse_candidates_from_text(text):
            if contains_core_terms(cand_norm, core_terms_auto):
                alerts.append({
                    "riesgo": "ALTO",
                    "marca_solicitada": raw_line,
                    "motivo": "Contiene término distintivo (CORE auto)",
                    "score": "",
                    "match_con_nuestra": "CORE",
                    "fuente": link
                })
                continue

            best, score = best_fuzzy_match(cand_norm, watch_all)
            if score >= FUZZY_ALERT:
                alerts.append({
                    "riesgo": "MEDIO",
                    "marca_solicitada": raw_line,
                    "motivo": f"Fuzzy >= {FUZZY_ALERT}",
                    "score": score,
                    "match_con_nuestra": best,
                    "fuente": link
                })
            elif FUZZY_REVIEW <= score < FUZZY_ALERT:
                review.append({
                    "marca_solicitada": raw_line,
                    "motivo": f"Fuzzy {FUZZY_REVIEW}-{FUZZY_ALERT-1}",
                    "score": score,
                    "match_con_nuestra": best,
                    "fuente": link
                })

    alerts = dedup(alerts)
    review = dedup(review)

    with open("out/reporte_inpi.md", "w", encoding="utf-8") as f:
        f.write("# Reporte INPI – Vigilancia de marcas\n\n")
        f.write(f"**Fecha/hora (AR):** {run_dt.strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write("**Boletines revisados:**\n")
        for l in pdf_links:
            f.write(f"- {l}\n")
        f.write("\n")
        f.write(f"**CORE auto:** {len(core_terms_auto)} términos (min_len=5)\n\n")

        if scanned:
            f.write("## Atención: PDF sin texto (posible escaneo)\n")
            for l in scanned:
                f.write(f"- {l}\n")
            f.write("\n> Si esto pasa seguido, agregamos OCR.\n\n")

        f.write("## Alertas\n\n")
        if alerts:
            for a in alerts[:120]:
                f.write(f"- **Riesgo {a['riesgo']}** | **Solicitada:** `{a['marca_solicitada']}`\n")
                f.write(f"  - Motivo: {a['motivo']}\n")
                if a["score"] != "":
                    f.write(f"  - Score: {a['score']} (match: `{a['match_con_nuestra']}`)\n")
                f.write(f"  - Fuente: {a['fuente']}\n\n")
        else:
            f.write("Sin coincidencias relevantes hoy.\n\n")

        f.write("## Revisión manual (posibles parecidos)\n\n")
        if review:
            for r in review[:200]:
                f.write(f"- `{r['marca_solicitada']}` | score {r['score']} (match `{r['match_con_nuestra']}`)\n")
                f.write(f"  - Fuente: {r['fuente']}\n")
        else:
            f.write("Sin items para revisar hoy.\n")

    print("[OK] Report generated: out/reporte_inpi.md")


if __name__ == "__main__":
    main()
