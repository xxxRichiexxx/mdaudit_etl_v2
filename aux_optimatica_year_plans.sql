TRUNCATE TABLE sttgaz.aux_optimatica_year_plans;

INSERT INTO sttgaz.aux_optimatica_year_plans
("plan_id", "deleted", "frozen", "created_at", "dealer_id", "year", "period_from", 
"period_to", "specialization", "minimum_budget", "plan_budget", "fact_budget", "budget", "state")
SELECT
    p.Id                AS plan_id,
    p.Deleted           AS deleted,
    p.Frozen            AS frozen,
    p.CreatedAt         AS created_at,
    d.id                AS dealer_id,
    p."Year"            AS year,
    (p.PeriodFrom AT TIME ZONE 'Europe/Moscow')::date,
    (p.PeriodTo AT TIME ZONE 'Europe/Moscow')::date,
    p.Specialization    AS specialization,
    p.MinimumBudget     AS minimum_budget,
    p.PlanBudget        AS plan_budget,
    p.FactBudget        AS fact_budget,
    p.Budget            AS budget,
    p.State             AS state
FROM sttgaz.stage_optimatica_YearPlan AS p
JOIN sttgaz.aux_optimatica_dealers AS d
    ON p.Dealer_Id = d.dealer_id;
