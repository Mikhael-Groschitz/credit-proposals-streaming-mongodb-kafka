SELECT
    tipo_produto,
    status_decisao,
    count(*) AS quantidade_de_propostas,
    round(avg(tempo_decisao_segundos) / 60.0, 1) AS tempo_medio_minutos
FROM delta_scan('s3://gold/tempo_decisao')
GROUP BY tipo_produto, status_decisao
ORDER BY tipo_produto, status_decisao;
