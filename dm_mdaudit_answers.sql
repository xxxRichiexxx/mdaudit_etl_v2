CREATE OR REPLACE VIEW sttgaz.dm_mdaudit_answers AS
WITH sq1 AS (
	SELECT 	
			s.shop_id 																	AS shop_id,
	        t.template_name 															AS template_name,
	        reg.region_name 															AS region_name,
	        d.division_name 															AS division_name,
			CASE
			WHEN date_trunc('month', c.finish_time)::date = '2022-09-01'
				THEN '2022-10-01'::date
			ELSE
	        	date_trunc('month', c.finish_time)::date
			END 																		AS month,
	        a.name 																		AS answer_name,
	        ((sum((a.weight * a.answer)) / sum((a.weight * 5))) * 100::numeric(18,0)) 	AS progress_procent
	FROM sttgaz.dds_mdaudit_answers 									AS a
	LEFT JOIN sttgaz.dds_mdaudit_checks 								AS c
		ON a.check_id = c.id 
	LEFT JOIN sttgaz.dds_mdaudit_shops 									AS s 
		ON c.shop_id = s.id 
	LEFT JOIN sttgaz.dds_mdaudit_divisions 								AS d 
		ON c.division_id = d.id 
	LEFT JOIN sttgaz.dds_mdaudit_resolvers 								AS r
		ON c.resolver_id = r.id 
	LEFT JOIN sttgaz.dds_mdaudit_templates 								AS t 
		ON c.template_id = t.id 
	LEFT JOIN sttgaz.dds_mdaudit_regions 								AS reg 
		ON s.region_id = reg.id
	WHERE a.answer <> 100 AND a.weight >= 1
	GROUP BY s.shop_id, t.template_name, reg.region_name, d.division_name, month, a.name
)
SELECT 
	*,
	LAG(progress_procent) OVER (PARTITION BY shop_id, region_name, division_name, template_name, answer_name ORDER BY "month") AS previous_progress
FROM sq1
ORDER BY shop_id, region_name, division_name, template_name, answer_name, "month";
