CREATE OR REPLACE VIEW dm_optimatica_plan_fact_aggregate AS
SELECT
	item_id,
	plan_id,
	dealer_name,
	deleted,
	frozen,
	"month",
	period_from,
	period_to,
	media,
	model,
	plan_price,
	SUM(placement_price) AS fact_rpice
FROM sttgaz.dm_optimatica_plan_fact
GROUP BY
	item_id,
	plan_id,
	dealer_name,
	deleted,
	frozen,
	"month",
	period_from,
	period_to,
	media,
	model,
	plan_price;