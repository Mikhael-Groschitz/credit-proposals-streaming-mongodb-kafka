WITH decisoes AS (
    SELECT
        tipo_produto,
        status,
        sum(quantidade) AS quantidade
    FROM delta_scan('s3://gold/funil_status')
    WHERE status IN ('aprovada', 'recusada')
    GROUP BY tipo_produto, status
)
SELECT
    tipo_produto,
    sum(quantidade) FILTER (WHERE status = 'aprovada') AS aprovadas,
    sum(quantidade) FILTER (WHERE status = 'recusada') AS recusadas,
    round(
        sum(quantidade) FILTER (WHERE status = 'aprovada')::DOUBLE
        / nullif(sum(quantidade), 0),
        4
    ) AS taxa_de_aprovacao
FROM decisoes
GROUP BY tipo_produto
ORDER BY tipo_produto;
