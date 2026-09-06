from app.checkpoint import RepositorioCheckpoint


class ColecaoFalsa:
    def __init__(self):
        self.documentos: dict[str, dict] = {}

    def find_one(self, filtro):
        return self.documentos.get(filtro["_id"])

    def replace_one(self, filtro, documento, upsert=False):
        self.documentos[filtro["_id"]] = documento


class BancoFalso:
    def __init__(self):
        self._colecoes: dict[str, ColecaoFalsa] = {}

    def __getitem__(self, nome):
        return self._colecoes.setdefault(nome, ColecaoFalsa())


def test_carregar_sem_checkpoint_retorna_none():
    repo = RepositorioCheckpoint(BancoFalso(), "checkpoints", "conector-propostas")
    assert repo.carregar_resume_token() is None


def test_salvar_e_carregar_resume_token():
    repo = RepositorioCheckpoint(BancoFalso(), "checkpoints", "conector-propostas")

    repo.salvar_resume_token({"_data": "abc"})

    assert repo.carregar_resume_token() == {"_data": "abc"}


def test_salvar_sobrescreve_o_checkpoint_anterior():
    repo = RepositorioCheckpoint(BancoFalso(), "checkpoints", "conector-propostas")

    repo.salvar_resume_token({"_data": "primeiro"})
    repo.salvar_resume_token({"_data": "segundo"})

    assert repo.carregar_resume_token() == {"_data": "segundo"}


def test_checkpoints_de_ids_diferentes_nao_se_misturam():
    banco = BancoFalso()
    repo_a = RepositorioCheckpoint(banco, "checkpoints", "conector-a")
    repo_b = RepositorioCheckpoint(banco, "checkpoints", "conector-b")

    repo_a.salvar_resume_token({"_data": "a"})

    assert repo_a.carregar_resume_token() == {"_data": "a"}
    assert repo_b.carregar_resume_token() is None
