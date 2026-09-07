SELECT
    janela_inicio,
    tipo_produto,
    volume,
    valor_total,
    round(valor_total / volume, 2) AS ticket_medio
FROM delta_scan('s3://gold/volume_valor_por_produto')
ORDER BY janela_inicio, tipo_produto;
