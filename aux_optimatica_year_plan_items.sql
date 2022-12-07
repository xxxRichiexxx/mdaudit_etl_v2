TRUNCATE TABLE sttgaz.aux_optimatica_year_plan_items;

INSERT INTO sttgaz.aux_optimatica_year_plan_items
("item_id", "plan_id", "created_at", "deleted", "frozen",
"month", "period_from", "period_to", "media", "model", "total_price")
SELECT
    i.Id,
    p.id,
    i.CreatedAt,
    i.Deleted,
    i.Frozen,
    i.Month,
    (i.PeriodFrom AT TIME ZONE 'Europe/Moscow')::date,
    (i.PeriodTo AT TIME ZONE 'Europe/Moscow')::date,
    i.Media,
    i.Model,
    i.TotalPrice
FROM sttgaz.stage_optimatica_YearPlanItem AS i 
JOIN sttgaz.aux_optimatica_year_plans AS p
    ON i.Plan_Id = p.plan_id;