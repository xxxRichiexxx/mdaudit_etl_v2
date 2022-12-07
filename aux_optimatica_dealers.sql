TRUNCATE TABLE sttgaz.aux_optimatica_dealers;

INSERT INTO sttgaz.aux_optimatica_dealers
("dealer_name", "dealer_id", "dealer_city", "dealer_address")
SELECT DISTINCT
    "Dealer",
    "Dealer_Id",
    "DealerCity",
    "DealerLegalAddress"
FROM sttgaz.stage_optimatica_YearPlan;
