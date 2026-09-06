from __future__ import annotations

from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.collection import Collection


class RepositorioPropostas:
    def __init__(self, mongo_uri: str, nome_banco: str, nome_colecao: str) -> None:
        self._client: MongoClient = MongoClient(mongo_uri)
        self._colecao: Collection = self._client[nome_banco][nome_colecao]

    def inserir(self, documento: dict) -> None:
        self._colecao.insert_one(dict(documento))

    def atualizar_status(self, id_proposta: str, novo_status: str) -> None:
        self._colecao.update_one(
            {"id_proposta": id_proposta},
            {"$set": {"status": novo_status, "data_atualizacao": datetime.now(timezone.utc)}},
        )

    def aplicar_correcao_retroativa(self, id_proposta: str, campos_corrigidos: dict) -> None:
        campos = dict(campos_corrigidos)
        campos.pop("status", None)
        campos["data_atualizacao"] = datetime.now(timezone.utc)
        self._colecao.update_one({"id_proposta": id_proposta}, {"$set": campos})

    def deletar(self, id_proposta: str) -> None:
        self._colecao.delete_one({"id_proposta": id_proposta})

    def fechar(self) -> None:
        self._client.close()
