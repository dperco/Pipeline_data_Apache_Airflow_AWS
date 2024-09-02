####  Calcular el NPS para dos categorías: casos por mes que fueron derivados al menos una vez y
 casos que no tuvieron ninguna derivación ####



-- Crear una tabla temporal para almacenar los casos derivados al menos una vez
WITH derived_cases AS (
    SELECT DISTINCT case_id
    FROM Interacciones
    WHERE interaction_type = 'rep_derivation'
),

-- Crear una tabla temporal para almacenar los casos que no fueron derivados
non_derived_cases AS (
    SELECT DISTINCT case_id
    FROM Interacciones
    WHERE case_id NOT IN (SELECT case_id FROM derived_cases)
),

-- Calcular el NPS para casos derivados
nps_derived AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN derived_cases USING(case_id)
    GROUP BY month
),

-- Calcular el NPS para casos no derivados
nps_non_derived AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN non_derived_cases USING(case_id)
    GROUP BY month
)

-- Unir los resultados y calcular el NPS
SELECT 
    month,
    'derived' AS category,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_derived
UNION ALL
SELECT 
    month,
    'non_derived' AS category,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_non_derived
ORDER BY month, category;