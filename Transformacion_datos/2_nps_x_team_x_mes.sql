
##  Visualizar el NPS por cada equipo de representantes y por mes.###



-- Crear una tabla temporal para almacenar la última interacción de cada caso
WITH last_interaction AS (
    SELECT 
        case_id,
        MAX(int_date) AS last_date
    FROM Interacciones
    GROUP BY case_id
),

-- Unir la tabla de interacciones con la tabla de representantes
case_representatives AS (
    SELECT 
        i.case_id,
        i.representante,
        r.team
    FROM Interacciones i
    JOIN last_interaction li ON i.case_id = li.case_id AND i.int_date = li.last_date
    JOIN Representantes r ON i.representante = r.representante
),

-- Calcular el NPS por equipo y por mes
nps_by_team AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        team,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN case_representatives USING(case_id)
    GROUP BY month, team
)

-- Calcular el NPS final
SELECT 
    month,
    team,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_by_team
ORDER BY month, team;