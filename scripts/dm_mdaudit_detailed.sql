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
	CASE
		WHEN SPLIT_PART(s.sap, '-', 1) IN ('112', '212', '312', '412') THEN 'Беларусь'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('113', '213', '313', '413') THEN 'Казахстан'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('114', '214', '314', '414') THEN 'Армения'
		WHEN SPLIT_PART(s.sap, '-', 1) IN ('115', '215', '315', '415') THEN 'Азербайджан'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('116', '216', '316', '416') THEN 'Грузия'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('117', '217', '317', '417') THEN 'Киргизия'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('118', '218', '318', '418') THEN 'Узбекистан'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('119', '219', '319', '419') THEN 'Преднестровье'
    	WHEN SPLIT_PART(s.sap, '-', 1) IN ('120', '220', '320', '420') THEN 'Молдова'
		WHEN SPLIT_PART(s.sap, '-', 1) IN ('000', '111', '211', '311', '411', '511', '5111', '611', '711') THEN 'РФ'
		ELSE 'Прочее'
	END															AS "country",
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
FROM sttgaz.dds_mdaudit_answers 		AS a
LEFT JOIN sttgaz.dds_mdaudit_checks 	AS c
	ON a.check_id = c.id 
LEFT JOIN sttgaz.dds_mdaudit_shops 		AS s 
	ON c.shop_id = s.id 
LEFT JOIN sttgaz.dds_mdaudit_divisions 	AS d 
	ON c.division_id = d.id 
LEFT JOIN sttgaz.dds_mdaudit_resolvers 	AS r
	ON c.resolver_id = r.id 
LEFT JOIN sttgaz.dds_mdaudit_templates 	AS t 
	ON c.template_id = t.id 
LEFT JOIN sttgaz.dds_mdaudit_regions 	AS reg 
	ON s.region_id = reg.id
WHERE a.answer <> 100
	AND ("Дата для агрегации" > (NOW() - INTERVAL '13 MONTH'))
	AND "Дата для агрегации" <> DATE_TRUNC('MONTH', NOW());
	
GRANT SELECT ON TABLE sttgaz.dm_mdaudit_detailed TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_mdaudit_detailed IS 'Витрина с детальными данными MD Audit';
