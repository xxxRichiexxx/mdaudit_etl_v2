DELETE FROM sttgaz.aux_mdaudit_resolvers;

INSERT INTO sttgaz.aux_mdaudit_resolvers
(resolver_id, resolver_first_name, resolver_last_name)
SELECT DISTINCT resolver_id, resolver_first_name, resolver_last_name
FROM sttgaz.stage_mdaudit_checks;