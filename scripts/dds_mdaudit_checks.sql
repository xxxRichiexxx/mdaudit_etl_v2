DELETE FROM sttgaz.dds_mdaudit_checks;

INSERT INTO sttgaz.dds_mdaudit_checks
(check_id, template_id, shop_id, division_id, resolver_id, resolve_date,
start_time, finish_time, last_modified_at, grade, comment, status)
SELECT
    c.id                AS check_id,
    t.id                AS template_id,
    s.id                AS shop_id,
    d.id                AS division_id,
    res.id              AS resolver_id,
    resolve_date,
    start_time,
    finish_time,
    last_modified_at,
    grade,
    comment,
    status
FROM sttgaz.stage_mdaudit_checks    AS c
JOIN sttgaz.dds_mdaudit_templates   AS t
    ON c.template_id = t.template_id
JOIN sttgaz.dds_mdaudit_shops       AS s 
    ON c.shop_id = s.shop_id
JOIN sttgaz.dds_mdaudit_regions     AS r  
    ON c.region_id = r.region_id
JOIN sttgaz.dds_mdaudit_divisions   AS d 
    ON c.division_id = d.division_id
JOIN sttgaz.dds_mdaudit_resolvers   AS res 
    ON c.resolver_id = res.resolver_id;