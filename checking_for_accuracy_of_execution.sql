INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH plan_price_result AS(
	SELECT SUM(plan_price)
	FROM dm_optimatica_plan_fact_aggregate
	WHERE dealer_name = 'Авторитэйл Регион, Казань'
		AND EXTRACT(YEAR FROM "month") = 2022
),
fact_rpice_result AS(
	SELECT SUM(fact_rpice)
	FROM dm_optimatica_plan_fact_aggregate
	WHERE dealer_name = 'Авторитэйл Регион, Казань'
		AND EXTRACT(YEAR FROM "month") = 2022
),
plan_true AS (
	SELECT plan_budget
	FROM sttgaz.aux_optimatica_year_plans AS p
	LEFT JOIN sttgaz.aux_optimatica_dealers AS d
		ON p.dealer_id = d.id
	WHERE dealer_name = 'Авторитэйл Регион, Казань'
		AND "year" = 2022
),
fact_true AS (
	SELECT fact_budget
	FROM sttgaz.aux_optimatica_year_plans AS p
	LEFT JOIN sttgaz.aux_optimatica_dealers AS d
		ON p.dealer_id = d.id
	WHERE dealer_name = 'Авторитэйл Регион, Казань'
		AND "year" = 2022
)
SELECT
	'dm_optimatica_plan_fact_aggregate',
	'checking_for_accuracy_of_execution',
	NOW(),
	ROUND((SELECT * FROM plan_price_result) / (SELECT * FROM plan_true),0) = 
	ROUND((SELECT * FROM fact_rpice_result) / (SELECT * FROM fact_true),0);