from __future__ import annotations

from datetime import datetime, timezone


class RepositorioCheckpoint:
    def __init__(self, database, nome_colecao: str, checkpoint_id: str) -> None:
        self._colecao = database[nome_colecao]
        self._checkpoint_id = checkpoint_id

    def carregar_resume_token(self) -> dict | None:
        documento = self._colecao.find_one({"_id": self._checkpoint_id})
        if documento is None:
            return None
        return documento["resume_token"]

    def salvar_resume_token(self, resume_token: dict) -> None:
        self._colecao.replace_one(
            {"_id": self._checkpoint_id},
            {
                "_id": self._checkpoint_id,
                "resume_token": resume_token,
                "atualizado_em": datetime.now(timezone.utc),
            },
            upsert=True,
        )
