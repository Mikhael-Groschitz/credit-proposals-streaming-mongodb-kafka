import pytest

from app.config import Config

_VARIAVEIS_CONFIG = [
    "MONGO_HOST", "MONGO_PORT", "MONGO_REPLICA_SET", "MONGO_DB", "MONGO_COLLECTION",
    "EVENTS_PER_SECOND", "RATIO_CONSIGNADO", "RATIO_CARTAO", "RATIO_FGTS",
    "RATIO_NEW_VS_TRANSITION", "RATIO_DELETE", "RATIO_CORRECAO", "RANDOM_SEED", "LOG_LEVEL",
]


@pytest.fixture(autouse=True)
def _ambiente_limpo(monkeypatch):
    for nome in _VARIAVEIS_CONFIG:
        monkeypatch.delenv(nome, raising=False)


def test_from_env_usa_defaults_quando_env_vazio():
    config = Config.from_env()

    assert config.mongo_host == "mongo"
    assert config.mongo_port == 27017
    assert config.mongo_uri == "mongodb://mongo:27017/?replicaSet=rs0"
    assert config.random_seed is None
    assert config.events_per_second == 5.0


def test_mongo_uri_usa_host_do_servico_compose_nunca_localhost(monkeypatch):
    monkeypatch.setenv("MONGO_HOST", "mongo")
    config = Config.from_env()
    assert "localhost" not in config.mongo_uri
    assert "mongo:27017" in config.mongo_uri


def test_events_per_second_zero_levanta_erro(monkeypatch):
    monkeypatch.setenv("EVENTS_PER_SECOND", "0")
    with pytest.raises(ValueError):
        Config.from_env()


def test_events_per_second_nao_numerico_levanta_erro(monkeypatch):
    monkeypatch.setenv("EVENTS_PER_SECOND", "abc")
    with pytest.raises(ValueError):
        Config.from_env()


def test_ratio_fora_do_intervalo_zero_um_levanta_erro(monkeypatch):
    monkeypatch.setenv("RATIO_DELETE", "1.5")
    with pytest.raises(ValueError):
        Config.from_env()


def test_todos_ratios_de_tipo_produto_zero_levanta_erro(monkeypatch):
    monkeypatch.setenv("RATIO_CONSIGNADO", "0")
    monkeypatch.setenv("RATIO_CARTAO", "0")
    monkeypatch.setenv("RATIO_FGTS", "0")
    with pytest.raises(ValueError):
        Config.from_env()


def test_random_seed_vazio_e_none():
    config = Config.from_env()
    assert config.random_seed is None


def test_random_seed_definido_e_convertido_para_inteiro(monkeypatch):
    monkeypatch.setenv("RANDOM_SEED", "42")
    config = Config.from_env()
    assert config.random_seed == 42
