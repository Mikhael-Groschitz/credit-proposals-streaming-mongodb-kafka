SELECT
    tipo_violacao,
    tipo_produto,
    count(*) AS quantidade,
    max(detectado_em) AS ultima_deteccao
FROM delta_scan('s3://gold/qualidade_violacoes')
GROUP BY tipo_violacao, tipo_produto
ORDER BY quantidade DESC;
