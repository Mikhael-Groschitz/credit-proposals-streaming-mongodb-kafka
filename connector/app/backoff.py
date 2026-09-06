from __future__ import annotations


def proximo_backoff_ms(atual_ms: int, maximo_ms: int) -> int:
    return min(atual_ms * 2, maximo_ms)
