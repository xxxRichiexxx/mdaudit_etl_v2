TRUNCATE TABLE sttgaz.aux_optimatica_placements;

INSERT INTO sttgaz.aux_optimatica_placements
("placement_id","created_at","deleted","frozen","dealer_id",
"period_from","period_to","specialization","media","model","site",
"description","publish_count","measure_unit","price","state")
SELECT
    p.Id,
    p.CreatedAt,
    p.Deleted,
    p.Frozen,
    d.id,
    (p.PeriodFrom AT TIME ZONE 'Europe/Moscow')::date,
    (p.PeriodTo AT TIME ZONE 'Europe/Moscow')::date,
    p.Specialization,
    p.Media,
    p.Model,
    p.Site,
    p.Description,
    p.PublishCount,
    p.MeasureUnit,
    p.Price,
    p.State
FROM sttgaz.stage_optimatica_Placement AS p
JOIN sttgaz.aux_optimatica_dealers AS d
    ON p.Dealer_Id = d.dealer_id;
