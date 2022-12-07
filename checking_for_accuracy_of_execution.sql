INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'dm_mdaudit_answers',
	'checking_for_accuracy_of_execution',
	NOW(),
	AVG(progress_procent) > 84 AND AVG(progress_procent) < 85
FROM sttgaz.dm_mdaudit_answers
WHERE "month" = '2022-03-01' AND 
	answer_name LIKE '%Взял номер телефона Клиента (или спросил не менее 3-х раз в ходе встречи)%'
	AND template_name = 'ТП Визит в ДЦ LCV';