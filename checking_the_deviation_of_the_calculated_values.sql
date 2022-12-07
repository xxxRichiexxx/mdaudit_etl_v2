WITH sq AS (	
	SELECT
        plan_id,
        SUM(plan_price) AS plan_price,
        SUM(fact_rpice) AS fact_rpice
	FROM dm_optimatica_plan_fact_aggregate
	GROUP BY plan_id
)
SELECT 
	p.id,
	plan_budget,
	fact_budget,
	plan_price,
	fact_rpice,
	((plan_price - plan_budget) / plan_budget) * 100,
	((fact_rpice - fact_budget) / fact_budget) * 100
FROM sq
right JOIN sttgaz.aux_optimatica_year_plans AS p
	ON sq.plan_id = p.id