import pytest
import pandas as pd
from pathlib import Path
from connectors.file.inep_microdados import INEPMicrodadosConnector, to_sinaes_type

# Minimal synthetic Censo CSV (Latin-1, semicolon-separated — matches real Censo format)
# Note: connector decodes raw INEP codes (e.g. TP_CATEGORIA_ADMINISTRATIVA=1 → org_type="federal")
# via ORG_TYPE_MAP and CATEGORY_MAP before calling to_sinaes_type
#
# TP_CATEGORIA_ADMINISTRATIVA: 1=federal, 2=estadual, 3=municipal, 4=privada_com_fins,
#                               5=privada_sem_fins, 6=privada_especial, 7=privada_comunitaria
# TP_ORGANIZACAO_ACADEMICA:    1=universidade, 2=centro_universitario, 3=faculdade,
#                               4=instituto_federal, 5=cefet
#
# Row 1982 (PUC-Campinas): TP_CATEGORIA_ADMINISTRATIVA=7 (privada_comunitaria), TP_ORGANIZACAO_ACADEMICA=1 (universidade)
#   → to_sinaes_type("privada_comunitaria", "universidade") = "community_university"  ✓
SYNTHETIC_CENSO = (
    "CO_IES;NO_IES;SG_IES;TP_CATEGORIA_ADMINISTRATIVA;TP_ORGANIZACAO_ACADEMICA;"
    "NO_MUNICIPIO_IES;SG_UF_IES;CO_REGIAO_IES;QT_DOC_EX_DOUT;QT_DOC_EX_TOTAL;QT_CURSO;QT_MAT\n"
    "572;Universidade Federal do ABC;UFABC;1;1;Santo Andre;SP;3;350;500;45;12000\n"
    "97;Universidade Federal de Sao Paulo;UNIFESP;1;1;Sao Paulo;SP;3;1200;1800;60;18000\n"
    "283;Universidade Federal do Para;UFPA;1;1;Belem;PA;1;800;1200;80;25000\n"
    "524;Instituto Federal de Educacao SP;IFSP;1;4;Sao Paulo;SP;3;120;400;30;8000\n"
    "1982;Pontificia Universidade Catolica de Campinas;PUC-Campinas;7;1;Campinas;SP;3;200;400;35;15000\n"
    "9999;Faculdade Privada XYZ;FAC-XYZ;4;3;Rio de Janeiro;RJ;3;10;50;5;800\n"
    "8888;Centro Universitario Comunitario;CEUNI;7;2;Porto Alegre;RS;4;50;100;12;3000\n"
)

@pytest.fixture
def connector(tmp_path):
    c = INEPMicrodadosConnector(data_dir=str(tmp_path))
    p = tmp_path / "MICRODADOS_CADASTRO_IES_2023.CSV"
    p.write_bytes(SYNTHETIC_CENSO.encode("latin-1"))
    return c

@pytest.fixture
def registry(connector):
    return connector.load(year=2023)

# --- to_sinaes_type ---

def test_federal_university():
    assert to_sinaes_type("federal", "universidade") == "federal_university"

def test_state_university_estadual():
    assert to_sinaes_type("estadual", "universidade") == "state_university"

def test_state_university_municipal():
    assert to_sinaes_type("municipal", "universidade") == "state_university"

def test_private_university():
    assert to_sinaes_type("privada_com_fins", "universidade") == "private_university"

def test_federal_institute():
    assert to_sinaes_type("federal", "instituto_federal") == "federal_institute"

def test_federal_institute_cefet():
    assert to_sinaes_type("federal", "cefet") == "federal_institute"

def test_community_university():
    assert to_sinaes_type("privada_comunitaria", "universidade") == "community_university"

def test_community_centro_universitario():
    assert to_sinaes_type("privada_comunitaria", "centro_universitario") == "community_university"

def test_isolated_faculty():
    assert to_sinaes_type("privada_com_fins", "faculdade") == "isolated_faculty"

def test_unknown_combination():
    assert to_sinaes_type("unknown", "unknown") == "other"

# --- connector.load() ---

def test_load_returns_dataframe(registry):
    assert isinstance(registry, pd.DataFrame)

def test_load_row_count(registry):
    assert len(registry) == 7

def test_load_e_mec_code_string(registry):
    assert registry["e_mec_code"].dtype == object

def test_load_e_mec_zero_padded(registry):
    assert "000572" in registry["e_mec_code"].values

def test_load_has_sinaes_type(registry):
    assert "sinaes_type" in registry.columns

def test_load_ufabc_sinaes_type(registry):
    row = registry[registry["e_mec_code"] == "000572"].iloc[0]
    assert row["sinaes_type"] == "federal_university"

def test_load_ifsp_sinaes_type(registry):
    row = registry[registry["e_mec_code"] == "000524"].iloc[0]
    assert row["sinaes_type"] == "federal_institute"

def test_load_puc_sinaes_type(registry):
    row = registry[registry["e_mec_code"] == "001982"].iloc[0]
    assert row["sinaes_type"] == "community_university"

def test_load_phd_faculty_share(registry):
    row = registry[registry["e_mec_code"] == "000572"].iloc[0]
    assert abs(row["phd_faculty_share"] - 0.7) < 0.01

def test_load_region(registry):
    row = registry[registry["e_mec_code"] == "000283"].iloc[0]
    assert row["region"] == "Norte"

def test_summary_total(connector, registry):
    s = connector.summary(registry)
    assert s["total_institutions"] == 7

def test_file_not_found_raises(tmp_path):
    c = INEPMicrodadosConnector(data_dir=str(tmp_path))
    with pytest.raises(FileNotFoundError):
        c.load(year=2022)
