DELETE FROM sttgaz.dds_mdaudit_shops;

INSERT INTO sttgaz.dds_mdaudit_shops
(shop_id, active, sap, locality, address, city, latitude, longitude, region_id)
SELECT
    s.id AS shop_id,
    active,
    sap,
    locality,
    address,
    city,
    latitude,
    longitude,
    r.id AS region_id
FROM sttgaz.stage_mdaudit_shops AS s
JOIN sttgaz.dds_mdaudit_regions AS r 
 ON s.regionId = r.region_id;
 