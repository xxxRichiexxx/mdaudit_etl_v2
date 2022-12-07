DELETE FROM sttgaz.dds_mdaudit_answers;

INSERT INTO sttgaz.dds_mdaudit_answers
(answer_id, check_id, question_id, name, answer, weight, comment)
SELECT 
    a.id AS answer_id,
    c.id AS check_id,
    question_id,
    name,
    answer,
    weight,
    a.comment    
FROM sttgaz.dds_mdaudit_checks      AS c
JOIN sttgaz.dds_mdaudit_shops       AS s 
    ON c.shop_id = s.id 
JOIN sttgaz.stage_mdaudit_answers   AS a 
    ON a.check_id = c.check_id AND a.shop_id = s.shop_id;