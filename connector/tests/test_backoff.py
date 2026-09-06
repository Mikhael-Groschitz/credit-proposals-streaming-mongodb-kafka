from app.backoff import proximo_backoff_ms


def test_dobra_o_valor_atual():
    assert proximo_backoff_ms(1000, 30000) == 2000


def test_nao_ultrapassa_o_maximo():
    assert proximo_backoff_ms(20000, 30000) == 30000


def test_no_maximo_permanece_no_maximo():
    assert proximo_backoff_ms(30000, 30000) == 30000
