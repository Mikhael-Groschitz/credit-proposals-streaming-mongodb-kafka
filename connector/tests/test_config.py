import pytest

from app.config import Config

_VARIAVEIS_CONFIG = [
    "MONGO_HOST", "MONGO_PORT", "MONGO_REPLICA_SET", "MONGO_DB", "MONGO_COLLECTION",
    "KAFKA_BOOTSTRAP_INTERNO", "KAFKA_TOPIC_PROPOSTAS", "KAFKA_DELIVERY_TIMEOUT_MS",
    "KAFKA_REQUEST_TIMEOUT_MS", "CHECKPOINT_COLLECTION", "CHECKPOINT_ID",
    "BATCH_MAX_SIZE", "BATCH_MAX_INTERVALO_MS", "RETRY_BACKOFF_INICIAL_MS",
    "RETRY_BACKOFF_MAXIMO_MS", "LOG_LEVEL",
]


@pytest.fixture(autouse=True)
def _ambiente_limpo(monkeypatch):
    for nome in _VARIAVEIS_CONFIG:
        monkeypatch.delenv(nome, raising=False)


def test_from_env_usa_defaults_quando_env_vazio():
    config = Config.from_env()

    assert config.mongo_host == "mongo"
    assert config.kafka_bootstrap_servers == "kafka:19092"
    assert config.kafka_topic == "propostas.cdc"
    assert config.checkpoint_collection == "checkpoints"
    assert config.checkpoint_id == "conector-propostas"
    assert config.batch_max_size == 50
    assert config.batch_max_intervalo_ms == 2000


def test_mongo_uri_usa_host_do_servico_compose_nunca_localhost(monkeypatch):
    monkeypatch.setenv("MONGO_HOST", "mongo")
    config = Config.from_env()
    assert "localhost" not in config.mongo_uri
    assert "mongo:27017" in config.mongo_uri


def test_batch_max_size_zero_levanta_erro(monkeypatch):
    monkeypatch.setenv("BATCH_MAX_SIZE", "0")
    with pytest.raises(ValueError):
        Config.from_env()


def test_batch_max_intervalo_negativo_levanta_erro(monkeypatch):
    monkeypatch.setenv("BATCH_MAX_INTERVALO_MS", "-1")
    with pytest.raises(ValueError):
        Config.from_env()


def test_retry_backoff_maximo_menor_que_inicial_levanta_erro(monkeypatch):
    monkeypatch.setenv("RETRY_BACKOFF_INICIAL_MS", "5000")
    monkeypatch.setenv("RETRY_BACKOFF_MAXIMO_MS", "1000")
    with pytest.raises(ValueError):
        Config.from_env()


def test_kafka_delivery_timeout_nao_numerico_levanta_erro(monkeypatch):
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", "abc")
    with pytest.raises(ValueError):
        Config.from_env()
