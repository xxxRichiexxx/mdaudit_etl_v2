INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
		WITH sq1 AS(
			SELECT COUNT(id) FROM sttgaz.stage_mdaudit_checks
		),
		sq2 AS(
			SELECT COUNT(DISTINCT id) FROM sttgaz.stage_mdaudit_checks
		),
		sq3 AS(
			SELECT COUNT(check_id) FROM sttgaz.dds_mdaudit_checks
		),
		sq4 AS(
			SELECT COUNT(DISTINCT check_id) FROM sttgaz.dds_mdaudit_checks
		),
		sq5 AS(
			SELECT 
			   (SELECT * FROM sq1)=(SELECT * FROM sq2) AND
			   (SELECT * FROM sq1)=(SELECT * FROM sq3) AND
			   (SELECT * FROM sq1)=(SELECT * FROM sq4)
		)
		SELECT
			'stage_mdaudit_checks, stage_mdaudit_answers, dds_mdaudit_checks,  dds_mdaudit_checks',
			'checking_for_duplicates',
			NOW(),
			(SELECT * FROM sq5);