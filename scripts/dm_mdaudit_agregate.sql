BEGIN TRANSACTION;

DROP VIEW IF EXISTS sttgaz.dm_mdaudit_agregate_v;
CREATE OR REPLACE VIEW sttgaz.dm_mdaudit_agregate_v AS
WITH 
all_data AS(
	SELECT
		d."Дата для агрегации",
		d.template_name,
		d.shop_sap,
		d.shop_locality,
		d.division_name,
		d.region_name,
		d.country,
		SUM(d."Фактическая оценка")*100 /SUM(d."Плановая оценка") 	AS "%"
		FROM sttgaz.dm_mdaudit_detailed 							AS d
		WHERE "Плановая оценка" <> 0
			AND d.region_name iLIKE '%продажи'
		GROUP BY
			d."Дата для агрегации",
			d.template_name,
			d.shop_sap,
			d.shop_locality,
			d.division_name,
			d.region_name,
			d.country
),
koeff_sootv_visit_zvonok AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		ROUND(AVG("%"), 2)											AS "Коэффициент соответствия ВИЗИТ+ЗВОНОК"
	FROM all_data
	WHERE template_name IN ('ТП Звонок в ДЦ BUS', 'ТП Звонок в ДЦ LCV', 'ТП Визит в ДЦ BUS', 'ТП Визит в ДЦ LCV')
	GROUP BY
		"Дата для агрегации",
		shop_sap
),
koeff_sootv_visit AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		ROUND(AVG("%"), 2)											AS "Коэффициент соответствия ВИЗИТ"
	FROM all_data
	WHERE template_name IN ('ТП Визит в ДЦ BUS', 'ТП Визит в ДЦ LCV')
	GROUP BY
		"Дата для агрегации",
		shop_sap
),
koeff_sootv_zvonok AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		ROUND(AVG("%"), 2)											AS "Коэффициент соответствия ЗВОНОК"
	FROM all_data
	WHERE template_name IN ('ТП Звонок в ДЦ BUS', 'ТП Звонок в ДЦ LCV')
	GROUP BY
		"Дата для агрегации",
		shop_sap
),
rabotos_avtootvetchik AS (
	SELECT
		"Дата для агрегации",
		shop_sap,
		CASE
			WHEN "%" >= 0 AND "%" < 100 THEN 0
			ELSE "%"
		END 														AS "Работоспособность Автоответчика"
	FROM all_data
	WHERE template_name = 'ТП Автоответчик ДЦ'
),
rabotos_obratn_zvonok AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		CASE
			WHEN "%" >= 0 AND "%" < 100 THEN 0
			ELSE "%"
		END 														AS "Работоспособность Обратного звонка"
	FROM all_data
	WHERE template_name = 'ТП Обратный звонок'
),
rabotos_online_cons AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		CASE
			WHEN "%" >= 0 AND "%" < 100 THEN 0
			ELSE "%"
		END 														AS "Работоспособность Онлайн-консультант"
	FROM all_data
	WHERE template_name = 'ТП Онлайн-консультант'
),
proverka_znaniy AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		CASE
			WHEN "%" >= 0 AND "%" < 80 THEN 0
			ELSE ROUND("%", 2)
		END 														AS "Проверка знаний"
	FROM all_data
	WHERE template_name iLIKE 'ПЗ%'
),
matrix AS(
	SELECT DISTINCT "Дата для агрегации", shop_sap, shop_locality, division_name, region_name, country
	FROM all_data
)
SELECT
	m."Дата для агрегации",
	m.shop_sap,
	m.shop_locality,
	m.division_name,
	m.region_name,
	m.country,
	"Коэффициент соответствия ВИЗИТ+ЗВОНОК",
	"Коэффициент соответствия ВИЗИТ",
	AVG("Коэффициент соответствия ВИЗИТ") OVER (PARTITION BY m."Дата для агрегации", m.region_name)					AS "Коэффициент соответствия ВИЗИТ по региону",
	AVG("Коэффициент соответствия ВИЗИТ") OVER (PARTITION BY m."Дата для агрегации", m.division_name, m.country)	AS "Коэффициент соответствия ВИЗИТ по стране",
	"Коэффициент соответствия ЗВОНОК",
	AVG("Коэффициент соответствия ЗВОНОК") OVER (PARTITION BY m."Дата для агрегации", m.region_name) 				AS "Коэффициент соответствия ЗВОНОК по региону",
	AVG("Коэффициент соответствия ЗВОНОК") OVER (PARTITION BY m."Дата для агрегации", m.division_name, m.country) 	AS "Коэффициент соответствия ЗВОНОК по стране",
	"Работоспособность Автоответчика",
	AVG("Работоспособность Автоответчика") OVER (PARTITION BY m."Дата для агрегации", m.region_name) 				AS "Работоспособность Автоответчика по региону",
	AVG("Работоспособность Автоответчика") OVER (PARTITION BY m."Дата для агрегации", m.division_name, m.country) 	AS "Работоспособность Автоответчика по стране",
	"Работоспособность Обратного звонка",
	AVG("Работоспособность Обратного звонка") OVER (PARTITION BY m."Дата для агрегации", m.region_name) 			AS "Работоспособность Обратного звонка по региону",
	AVG("Работоспособность Обратного звонка") OVER (PARTITION BY m."Дата для агрегации", m.division_name, m.country) AS "Работоспособность Обратного звонка по стране",
	"Работоспособность Онлайн-консультант",
	AVG("Работоспособность Онлайн-консультант") OVER (PARTITION BY m."Дата для агрегации", m.region_name) 			AS "Работоспособность Онлайн-консультант по региону",
	AVG("Работоспособность Онлайн-консультант") OVER (PARTITION BY m."Дата для агрегации", m.division_name, m.country) AS "Работоспособность Онлайн-консультант по стране",
	"Проверка знаний",
	AVG("Проверка знаний") OVER (PARTITION BY m."Дата для агрегации", m.region_name) 								AS "Проверка знаний по региону",
	AVG("Проверка знаний") OVER (PARTITION BY m."Дата для агрегации", m.division_name, m.country) 					AS "Проверка знаний по стране"
FROM matrix															AS m
LEFT JOIN koeff_sootv_visit_zvonok 									AS vs
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(vs."Дата для агрегации", vs.shop_sap)
LEFT JOIN koeff_sootv_zvonok 										AS s
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(s."Дата для агрегации", s.shop_sap)
LEFT JOIN koeff_sootv_visit											AS v
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(v."Дата для агрегации", v.shop_sap)
LEFT JOIN rabotos_avtootvetchik										AS a
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(a."Дата для агрегации", a.shop_sap)
LEFT JOIN rabotos_obratn_zvonok										AS oz
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(oz."Дата для агрегации", oz.shop_sap)
LEFT JOIN proverka_znaniy											AS pz
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(pz."Дата для агрегации", pz.shop_sap)
LEFT JOIN rabotos_online_cons										AS oc
	ON HASH(m."Дата для агрегации", m.shop_sap) = 
	   HASH(oc."Дата для агрегации", oc.shop_sap);


GRANT SELECT ON TABLE sttgaz.dm_mdaudit_agregate_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_mdaudit_agregate_v IS 'Витрина с Четырьмя метриками для дилерских центров';

COMMIT TRANSACTION;
