SELECT sum(quantidade) AS total_propostas_abertas_no_gold
FROM delta_scan('s3://gold/contagem_atual_por_status');
