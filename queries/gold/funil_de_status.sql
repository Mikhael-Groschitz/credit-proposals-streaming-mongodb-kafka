SELECT
    tipo_produto,
    status,
    sum(quantidade) AS quantidade_total
FROM delta_scan('s3://gold/funil_status')
GROUP BY tipo_produto, status
ORDER BY tipo_produto, quantidade_total DESC;
