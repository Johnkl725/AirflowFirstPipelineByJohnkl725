-- consulta.sql
-- Ejemplo: consulta automatizada para tabla_auditoria_votados
SELECT "categoria", 
SUM("votos"),
"top(1)" OVER PARTITION BY "categoria" ORDER BY SUM("votos") DESC
FROM tabla_auditoria_votados tav 
GROUP BY "categoria";


SELECT categoria,
persona,
votos
FROM(
    select 
    categoria,
    persona,
    votos,
    row_number() over (PARTITION BY categoria ORDER BY votos DESC) as ranking
    FROM tabla_auditoria_votados
) t
where ranking = 1;
-- Puedes agregar más sentencias aquí
