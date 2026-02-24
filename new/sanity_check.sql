-- =========================================================
-- 最终抽到多少 items、多少 shops
-- =========================================================
SELECT COUNT(1) AS n_items, COUNT(DISTINCT shop_id) AS n_shops FROM tmp_sampled_items;

-- =========================================================
-- 每个 shop 是否真的 2 个 cell
-- =========================================================
SELECT shop_id, COUNT(1) AS n_cells
FROM tmp_sampled_shop_cells
GROUP BY shop_id
HAVING COUNT(1) <> 2;

-- =========================================================
-- 每个 shop×cell 是否真的 10 个 item
-- =========================================================
SELECT shop_id, category_id, price_band, COUNT(1) AS n_items
FROM tmp_sampled_items
GROUP BY shop_id, category_id, price_band
HAVING COUNT(1) <> 10;