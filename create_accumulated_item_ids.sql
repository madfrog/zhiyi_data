CREATE TABLE IF NOT EXISTS tmp_accumulated_item_ids (
    item_id STRING
);

-- 向 tmp_accumulated_item_ids 表中追加新的 item_id
INSERT INTO tmp_accumulated_item_ids
SELECT DISTINCT item_id
FROM tmp_sampled_items_10k
WHERE item_id IS NOT NULL;