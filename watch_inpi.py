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


def download_bytes(url: str, timeout: int = 60) -> tuple[bytes, str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Referer": INPI_BOLETINES_URL,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    }
    r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
    ctype = (r.headers.get("Content-Type") or "").lower()
    return r.content, ctype, r.url


def looks_like_html(b: bytes) -> bool:
    head = b[:1024].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html") or b"<html" in head


def is_probably_pdf(content: bytes, content_type: str) -> bool:
    # Regla fuerte: header PDF
    if content[:5] == b"%PDF-":
        return True
    # Si parece HTML, NO es PDF aunque el server diga "pdf"
    if looks_like_html(content):
        return False
    # A veces el header no está en byte 0 por basura inicial: lo buscamos cerca
    if b"%PDF" in content[:2048]:
        return True
    # Si content-type tiene pdf pero no vemos HTML, lo dejamos pasar (último recurso)
    if "pdf" in (content_type or ""):
        return True
    return False


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    chunks = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return "\n".join(chunks)


def parse_candidates_from_text(text: str):
    out = []
    for line in text.splitlines():
        ln = line.strip()
        if not ln or len(ln) > 220:
            continue
        if sum(ch.isalpha() for ch in ln) >= 6:
            out.append((ln, normalize(ln)))
    return out


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


def safe_slug(s: str, max_len: int = 80) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    return s[-max_len:]


def write_report(run_dt, pdf_links, core_terms_auto, scanned, alerts, review):
    with open("out/reporte_inpi.md", "w", encoding="utf-8") as f:
        f.write("# Reporte INPI – Vigilancia de marcas\n\n")
        f.write(f"**Fecha/hora (AR):** {run_dt.strftime('%Y-%m-%d %H:%M')}\n\n")

        f.write("**Boletines/links revisados:**\n")
        for l in pdf_links:
            f.write(f"- {l}\n")
        f.write("\n")

        f.write(f"**CORE auto:** {len(core_terms_auto)} términos (min_len=5)\n\n")

        if scanned:
            f.write("## Observaciones (no-PDF / PDF inválido / escaneado)\n\n")
            for s in scanned:
                f.write(f"- {s}\n")
            f.write("\n")

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


def main():
    run_dt = datetime.now(timezone(timedelta(hours=-3)))
    os.makedirs("out", exist_ok=True)

    # CSV
    _, watch_all = load_watchlist()
    core_terms_auto = build_core_terms_from_watchlist(watch_all, min_len=5)

    # INPI
    pdf_links, boletines_html = fetch_boletin_pdf_links()
    with open("out/boletines_page.html", "w", encoding="utf-8") as f:
        f.write(boletines_html)

    # Si no hay links, generar reporte y salir (sin fallar)
    if not pdf_links:
        write_report(run_dt, [], core_terms_auto, ["No se encontraron links a PDFs en la página."], [], [], [])
        print("[INPI] No PDFs found. Report generated.")
        return

    # últimos 2
    pdf_links = pdf_links[-2:]

    alerts, review, scanned = [], [], []

    for link in pdf_links:
        content, ctype, final_url = download_bytes(link, timeout=60)

        if not is_probably_pdf(content, ctype):
            name = safe_slug(final_url)
            with open(f"out/not_pdf_{name}.bin", "wb") as f:
                f.write(content[:200000])
            scanned.append(f"{final_url} (NO PDF; content-type={ctype})")
            continue

        # Try/except SIEMPRE: no queremos que caiga el workflow
        try:
            text = extract_text_from_pdf(content)
        except Exception as e:
            scanned.append(f"{final_url} (PDF inválido/protegido: {e})")
            continue

        if len(text.strip()) < 200:
            scanned.append(f"{final_url} (PDF sin texto / escaneado)")
            continue

        for raw_line, cand_norm in parse_candidates_from_text(text):
            if contains_core_terms(cand_norm, core_terms_auto):
                alerts.append({
                    "riesgo": "ALTO",
                    "marca_solicitada": raw_line,
                    "motivo": "Contiene término distintivo (CORE auto)",
                    "score": "",
                    "match_con_nuestra": "CORE",
                    "fuente": final_url
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
                    "fuente": final_url
                })
            elif FUZZY_REVIEW <= score < FUZZY_ALERT:
                review.append({
                    "marca_solicitada": raw_line,
                    "motivo": f"Fuzzy {FUZZY_REVIEW}-{FUZZY_ALERT-1}",
                    "score": score,
                    "match_con_nuestra": best,
                    "fuente": final_url
                })

    alerts = dedup(alerts)
    review = dedup(review)

    write_report(run_dt, pdf_links, core_terms_auto, scanned, alerts, review)
    print("[OK] Report generated: out/reporte_inpi.md")


if __name__ == "__main__":
    main()
