CREATE OR REPLACE VIEW sttgaz.dm_mdaudit_detailed AS
SELECT
	c.check_id 													AS id,
	t.template_id,
	t.template_name,
	s.shop_id,
	s.sap 														AS shop_sap,
	s.locality 													AS shop_locality,
	s.address,
	s.city,
	s.latitude,
	s.longitude,
	s.active,
	reg.region_id,
	reg.region_name,
	d.division_id,
	d.division_name,
	r.resolver_id,
	r.resolver_first_name,
	r.resolver_last_name,
	c.resolve_date,
	c.start_time,
	c.finish_time,
	c.last_modified_at,
	c.grade,
	c."comment",
	c.status,
	a.answer_id 												AS "answers.id",
	a.question_id 												AS "answers.question_id",
	a.name 														AS "answers.name",
	a.answer 													AS "answers.answer",
	a.weight 													AS "answers.weight",
	a."comment" 												AS "answers.comment",
	CASE
		WHEN a.answer = 100 THEN 0
		ELSE a.answer * a.weight
	END															AS "Фактическая оценка",
	CASE
		WHEN a.answer = 100 THEN 0
		ELSE 5 * a.weight
	END 														AS "Плановая оценка",
	CASE
		WHEN "Плановая оценка" = 0 THEN NULL
		ELSE ("Фактическая оценка" / "Плановая оценка") * 100
	END 														AS "Коэффициент соответствия",
	CASE
	WHEN DATE_TRUNC('month', c.finish_time)::date = '2022-09-01'
		THEN '2022-10-01'::date
	ELSE
		DATE_TRUNC('month', c.finish_time)::date
	END 														AS "Дата для агрегации"	
FROM sttgaz.aux_mdaudit_answers 		AS a
LEFT JOIN sttgaz.aux_mdaudit_checks 	AS c
	ON a.check_id = c.id 
LEFT JOIN sttgaz.aux_mdaudit_shops 		AS s 
	ON c.shop_id = s.id 
LEFT JOIN sttgaz.aux_mdaudit_divisions 	AS d 
	ON c.division_id = d.id 
LEFT JOIN sttgaz.aux_mdaudit_resolvers 	AS r
	ON c.resolver_id = r.id 
LEFT JOIN sttgaz.aux_mdaudit_templates 	AS t 
	ON c.template_id = t.id 
LEFT JOIN sttgaz.aux_mdaudit_regions 	AS reg 
	ON s.region_id = reg.id
WHERE a.answer <> 100
	AND ("Дата для агрегации" > (NOW() - INTERVAL '13 MONTH'))
	AND "Дата для агрегации" <> DATE_TRUNC('MONTH', NOW());
	