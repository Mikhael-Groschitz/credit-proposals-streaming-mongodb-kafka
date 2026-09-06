from app.config import Config
from app.generator import GeradorDeCarga, _PropostaAtiva


class RepositorioFalso:

    def __init__(self) -> None:
        self.inseridos: list[dict] = []
        self.status_atualizados: list[tuple[str, str]] = []
        self.correcoes: list[tuple[str, dict]] = []
        self.deletados: list[str] = []

    def inserir(self, documento: dict) -> None:
        self.inseridos.append(documento)

    def atualizar_status(self, id_proposta: str, novo_status: str) -> None:
        self.status_atualizados.append((id_proposta, novo_status))

    def aplicar_correcao_retroativa(self, id_proposta: str, campos: dict) -> None:
        self.correcoes.append((id_proposta, campos))

    def deletar(self, id_proposta: str) -> None:
        self.deletados.append(id_proposta)


def _config(**overrides) -> Config:
    base = dict(
        mongo_host="mongo",
        mongo_port=27017,
        mongo_replica_set="rs0",
        mongo_db="creditodb",
        mongo_collection="propostas",
        events_per_second=1.0,
        ratio_consignado=1.0,
        ratio_cartao=0.0,
        ratio_fgts=0.0,
        ratio_new_vs_transition=1.0,
        ratio_delete=0.0,
        ratio_correcao=0.0,
        random_seed=None,
        log_level="INFO",
    )
    base.update(overrides)
    return Config(**base)


def test_evento_cria_proposta_quando_pool_esta_vazio():
    repo = RepositorioFalso()
    gerador = GeradorDeCarga(_config(ratio_new_vs_transition=0.0), repo)

    gerador.executar_um_evento()

    assert len(repo.inseridos) == 1
    assert len(gerador._propostas_ativas) == 1


def test_correcao_retroativa_nunca_envia_o_campo_status():
    repo = RepositorioFalso()
    gerador = GeradorDeCarga(_config(), repo)
    gerador._propostas_ativas["id-1"] = _PropostaAtiva(tipo_produto="cartao", status="em_analise")

    gerador._aplicar_correcao("id-1")

    assert len(repo.correcoes) == 1
    id_proposta, campos = repo.correcoes[0]
    assert id_proposta == "id-1"
    assert "status" not in campos
    assert gerador._propostas_ativas["id-1"].status == "em_analise"


def test_deletar_remove_a_proposta_do_pool_em_memoria():
    repo = RepositorioFalso()
    gerador = GeradorDeCarga(_config(), repo)
    gerador._propostas_ativas["id-1"] = _PropostaAtiva(
        tipo_produto="cartao", status="em_analise", marcada_para_delecao=True,
    )

    gerador._deletar("id-1")

    assert repo.deletados == ["id-1"]
    assert "id-1" not in gerador._propostas_ativas


def test_transicao_para_estado_terminal_remove_do_pool(monkeypatch):
    import app.generator as generator_module

    monkeypatch.setattr(generator_module, "proximo_status", lambda status: "paga")

    repo = RepositorioFalso()
    gerador = GeradorDeCarga(_config(), repo)
    proposta = _PropostaAtiva(tipo_produto="cartao", status="aprovada")
    gerador._propostas_ativas["id-1"] = proposta

    gerador._transicionar("id-1", proposta)

    assert repo.status_atualizados == [("id-1", "paga")]
    assert "id-1" not in gerador._propostas_ativas


def test_transicao_para_estado_nao_terminal_mantem_no_pool(monkeypatch):
    import app.generator as generator_module

    monkeypatch.setattr(generator_module, "proximo_status", lambda status: "pendente_documentacao")

    repo = RepositorioFalso()
    gerador = GeradorDeCarga(_config(), repo)
    proposta = _PropostaAtiva(tipo_produto="cartao", status="em_analise")
    gerador._propostas_ativas["id-1"] = proposta

    gerador._transicionar("id-1", proposta)

    assert repo.status_atualizados == [("id-1", "pendente_documentacao")]
    assert gerador._propostas_ativas["id-1"].status == "pendente_documentacao"
