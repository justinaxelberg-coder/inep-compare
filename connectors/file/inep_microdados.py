"""
INEP Microdados connector — Censo da Educação Superior.

Downloads and parses the annual Censo CSV files to build the master HEI registry.
The Censo is the authoritative denominator for all coverage calculations.

Data source: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Column mapping from Censo raw names to our schema
# Column names vary slightly across years — this covers 2020–2023
COLUMN_MAP = {
    # e-MEC institution code
    "CO_IES": "e_mec_code",
    # Names
    "NO_IES": "name",
    "SG_IES": "abbreviation",
    # Category (universidade, centro universitário, faculdade, IF, CEFET, etc.)
    "TP_CATEGORIA_ADMINISTRATIVA": "org_type_code",   # 1=federal,2=estadual,3=municipal,4-7=private
    "TP_ORGANIZACAO_ACADEMICA": "category_code",       # 1=univ,2=centro univ,3=faculdade,4=IF,5=CEFET
    # Location
    "NO_MUNICIPIO_IES": "city",
    "SG_UF_IES": "state",
    "CO_REGIAO_IES": "region_code",                   # 1=N,2=NE,3=SE,4=S,5=CO
    # Research indicators (available from 2019+)
    "QT_DOC_EX_DOUT": "faculty_with_phd",
    "QT_DOC_EX_TOTAL": "faculty_total",
    "QT_CURSO": "n_courses",
    "QT_MAT": "total_enrollment",
}

ORG_TYPE_MAP = {
    1: "federal",
    2: "estadual",
    3: "municipal",
    4: "privada_com_fins",
    5: "privada_sem_fins",
    6: "privada_especial",
    7: "privada_comunitaria",
}

CATEGORY_MAP = {
    1: "universidade",
    2: "centro_universitario",
    3: "faculdade",
    4: "instituto_federal",
    5: "cefet",
}

REGION_MAP = {
    1: "Norte",
    2: "Nordeste",
    3: "Sudeste",
    4: "Sul",
    5: "Centro-Oeste",
}


def to_sinaes_type(org_type: str, category: str) -> str:
    """
    Map INEP org_type × category to FitnessScorer institution type vocabulary.

    SINAES types:
      federal_university, state_university, private_university,
      federal_institute, community_university, isolated_faculty, other
    """
    if category in ("instituto_federal", "cefet"):
        return "federal_institute"
    if org_type == "federal":
        if category == "universidade":
            return "federal_university"
        return "other"
    if org_type in ("estadual", "municipal"):
        if category == "universidade":
            return "state_university"
        return "other"
    if org_type == "privada_comunitaria":
        if category in ("universidade", "centro_universitario"):
            return "community_university"
        return "isolated_faculty"
    if org_type in ("privada_com_fins", "privada_sem_fins", "privada_especial"):
        if category == "universidade":
            return "private_university"
        return "isolated_faculty"
    return "other"


class INEPMicrodadosConnector:
    """
    Parses INEP Censo da Educação Superior CSV files into the master HEI registry.

    Usage:
        connector = INEPMicrodadosConnector(data_dir="data/raw/inep")
        registry = connector.load(year=2023)
        registry.to_csv("registry/institutions.csv", index=False)
    """

    def __init__(self, data_dir: str | Path = "data/raw/inep"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def load(self, year: int = 2023) -> pd.DataFrame:
        """
        Load and normalise the Censo for a given year.
        Returns a DataFrame conforming to the institution schema.
        """
        path = self._find_censo_file(year)
        if path is None:
            raise FileNotFoundError(
                f"Censo file for {year} not found in {self.data_dir}.\n"
                f"Download from: https://www.gov.br/inep/pt-br/acesso-a-informacao/"
                f"dados-abertos/microdados/censo-da-educacao-superior\n"
                f"Expected filename pattern: IES_*{year}*.CSV or MICRODADOS_CADASTRO_IES_{year}*.CSV"
            )

        logger.info(f"Loading Censo {year} from {path}")

        # Censo CSVs use Latin-1 encoding and semicolon separators
        raw = pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False)
        logger.info(f"Loaded {len(raw):,} rows from Censo {year}")

        return self._normalise(raw, year)

    def _find_censo_file(self, year: int) -> Path | None:
        """Locate the IES-level Censo file for the given year."""
        patterns = [
            f"*IES*{year}*.CSV",
            f"*IES*{year}*.csv",
            f"*CADASTRO_IES*{year}*.CSV",
            f"*CADASTRO_IES*{year}*.csv",
        ]
        for pattern in patterns:
            matches = list(self.data_dir.glob(pattern))
            if matches:
                return matches[0]
        return None

    def _normalise(self, raw: pd.DataFrame, year: int) -> pd.DataFrame:
        """Map raw Censo columns to the institution schema."""
        # Identify which raw columns are actually present
        available = {k: v for k, v in COLUMN_MAP.items() if k in raw.columns}
        missing = [k for k in COLUMN_MAP if k not in raw.columns]
        if missing:
            logger.warning(f"Censo {year}: columns not found (may vary by year): {missing}")

        df = raw[list(available.keys())].rename(columns=available).copy()

        # Decode categorical codes
        if "org_type_code" in df.columns:
            df["org_type"] = df["org_type_code"].map(ORG_TYPE_MAP)
        if "category_code" in df.columns:
            df["category"] = df["category_code"].map(CATEGORY_MAP)
        if "region_code" in df.columns:
            df["region"] = df["region_code"].map(REGION_MAP)

        # Map to FitnessScorer vocabulary
        if "org_type" in df.columns and "category" in df.columns:
            df["sinaes_type"] = df.apply(
                lambda r: to_sinaes_type(
                    str(r.get("org_type", "")), str(r.get("category", ""))
                ), axis=1
            )

        # Normalise e-MEC code to string, zero-padded to 6 digits
        df["e_mec_code"] = df["e_mec_code"].astype(str).str.zfill(6)

        # Derived: PhD faculty share (useful for stratification)
        if "faculty_with_phd" in df.columns and "faculty_total" in df.columns:
            df["phd_faculty_share"] = (
                df["faculty_with_phd"] / df["faculty_total"].replace(0, pd.NA)
            ).round(4)

        # Add source year
        df["censo_year"] = year

        # Placeholder columns for crosswalk (populated separately)
        for col in ["ror_id", "isni", "scopus_affiliation_id",
                    "openalex_institution_id", "wikidata_qid",
                    "typology_code"]:
            if col not in df.columns:
                df[col] = pd.NA

        # Drop raw code columns now that we have labels
        df = df.drop(columns=["org_type_code", "category_code", "region_code"],
                     errors="ignore")

        logger.info(
            f"Normalised registry: {len(df):,} institutions, "
            f"{df['category'].value_counts().to_dict()}"
        )

        return df

    def summary(self, df: pd.DataFrame) -> dict:
        """Return a summary dict for logging and reporting."""
        return {
            "total_institutions": len(df),
            "by_category": df["category"].value_counts().to_dict() if "category" in df.columns else {},
            "by_org_type": df["org_type"].value_counts().to_dict() if "org_type" in df.columns else {},
            "by_region": df["region"].value_counts().to_dict() if "region" in df.columns else {},
            "with_ror_id": int(df["ror_id"].notna().sum()) if "ror_id" in df.columns else 0,
        }

    def download(self, year: int = 2023, force: bool = False) -> Path:
        """
        Download the Censo IES CSV for the given year from INEP's open data portal.
        Fetches the ZIP, extracts only the IES-level CSV, saves to self.data_dir.

        Args:
            year:  Censo year (2019–2023 confirmed available)
            force: Re-download even if file already exists

        Returns: Path to the extracted IES CSV
        """
        import io
        import zipfile
        import httpx

        # Check if already downloaded
        existing = self._find_censo_file(year)
        if existing and not force:
            logger.info(f"Censo {year} already downloaded: {existing}")
            return existing

        url = (
            f"https://download.inep.gov.br/microdados/"
            f"microdados_censo_da_educacao_superior_{year}.zip"
        )
        logger.info(f"Downloading Censo {year} from {url}")

        try:
            with httpx.Client(timeout=300, follow_redirects=True) as client:
                response = client.get(url)
                response.raise_for_status()
        except httpx.HTTPError as e:
            raise RuntimeError(
                f"Failed to download Censo {year}: {e}\n"
                f"Download manually from: https://www.gov.br/inep/pt-br/acesso-a-informacao/"
                f"dados-abertos/microdados/censo-da-educacao-superior\n"
                f"Place the IES CSV in: {self.data_dir}"
            ) from e

        logger.info(f"Downloaded {len(response.content) / 1e6:.1f} MB — extracting IES file")

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            ies_files = [
                name for name in zf.namelist()
                if "IES" in name.upper() and name.upper().endswith(".CSV")
                and "CURSO" not in name.upper()
                and "DOCENTE" not in name.upper()
                and "ALUNO" not in name.upper()
            ]
            if not ies_files:
                raise RuntimeError(
                    f"No IES CSV found in Censo {year} ZIP. "
                    f"Files in ZIP: {zf.namelist()}"
                )
            ies_name = ies_files[0]
            out_path = self.data_dir / Path(ies_name).name
            with zf.open(ies_name) as src, open(out_path, "wb") as dst:
                dst.write(src.read())

        logger.info(f"Censo {year} IES file saved: {out_path}")
        return out_path
