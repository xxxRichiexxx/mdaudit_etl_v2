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
		SUM(d."Фактическая оценка")*100 /SUM(d."Плановая оценка") 	AS "%"
		FROM sttgaz.dm_mdaudit_detailed 							AS d
		WHERE "Плановая оценка" <> 0
		GROUP BY
			d."Дата для агрегации",
			d.template_name,
			d.shop_sap,
			d.shop_locality
),
koeff_sootv_visit_zvonok AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		shop_locality,
		AVG("%") 												AS "Коэффициент соответствия ВИЗИТ+ЗВОНОК"
	FROM all_data
	WHERE template_name IN ('ТП Звонок в ДЦ BUS', 'ТП Звонок в ДЦ LCV', 'ТП Визит в ДЦ BUS', 'ТП Визит в ДЦ LCV')
	GROUP BY
		"Дата для агрегации",
		shop_sap,
		shop_locality
),
koeff_sootv_visit AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		shop_locality,
		AVG("%") 												AS "Коэффициент соответствия ВИЗИТ"
	FROM all_data
	WHERE template_name IN ('ТП Визит в ДЦ BUS', 'ТП Визит в ДЦ LCV')
	GROUP BY
		"Дата для агрегации",
		shop_sap,
		shop_locality
),
koeff_sootv_zvonok AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		shop_locality,
		AVG("%") 												AS "Коэффициент соответствия ЗВОНОК"
	FROM all_data
	WHERE template_name IN ('ТП Звонок в ДЦ BUS', 'ТП Звонок в ДЦ LCV')
	GROUP BY
		"Дата для агрегации",
		shop_sap,
		shop_locality
),
rabotos_avtootvetchik AS (
	SELECT
		"Дата для агрегации",
		shop_sap,
		shop_locality,
		CASE
			WHEN "%" >= 0 AND "%" < 100 THEN 0
			ELSE "%"
		END 													AS "Работоспособность Автоответчика"
	FROM all_data
	WHERE template_name = 'ТП Автоответчик ДЦ'
),
rabotos_obratn_zvonok AS(
	SELECT
		"Дата для агрегации",
		shop_sap,
		shop_locality,
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
		shop_locality,
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
		shop_locality,
		CASE
			WHEN "%" >= 0 AND "%" < 80 THEN 0
			ELSE "%"
		END 														AS "Проверка знаний"
	FROM all_data
	WHERE template_name iLIKE 'ПЗ%'
),
matrix AS(
	SELECT DISTINCT "Дата для агрегации", shop_sap, shop_locality
	FROM all_data
)
SELECT
	m."Дата для агрегации",
	m.shop_sap,
	m.shop_locality,
	"Коэффициент соответствия ВИЗИТ+ЗВОНОК",
	"Коэффициент соответствия ВИЗИТ",
	"Коэффициент соответствия ЗВОНОК",
	"Работоспособность Автоответчика",
	"Работоспособность Обратного звонка",
	"Работоспособность Онлайн-консультант",
	"Проверка знаний"
FROM matrix															AS m
LEFT JOIN koeff_sootv_visit_zvonok 									AS vs
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(vs."Дата для агрегации", vs.shop_sap, vs.shop_locality)
LEFT JOIN koeff_sootv_zvonok 										AS s
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(s."Дата для агрегации", s.shop_sap, s.shop_locality)
LEFT JOIN koeff_sootv_visit											AS v
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(v."Дата для агрегации", v.shop_sap, v.shop_locality)
LEFT JOIN rabotos_avtootvetchik										AS a
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(a."Дата для агрегации", a.shop_sap, a.shop_locality)
LEFT JOIN rabotos_obratn_zvonok										AS oz
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(oz."Дата для агрегации", oz.shop_sap, oz.shop_locality)
LEFT JOIN proverka_znaniy											AS pz
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(pz."Дата для агрегации", pz.shop_sap, pz.shop_locality)
LEFT JOIN rabotos_online_cons										AS oc
	ON HASH(m."Дата для агрегации", m.shop_sap, m.shop_locality) = 
	   HASH(oc."Дата для агрегации", oc.shop_sap, oc.shop_locality);


GRANT SELECT ON TABLE sttgaz.dm_mdaudit_agregate_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_mdaudit_agregate_v IS 'Витрина с Четырьмя метриками для дилерских центров';

COMMIT TRANSACTION;
