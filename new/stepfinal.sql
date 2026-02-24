-- =========================================================
-- FINAL-0. 为每个抽样 item 定义 anchor_date，并取 anchor_date 当天价格与 (g-1) 的低频快照
-- =========================================================
CREATE TABLE IF NOT EXISTS final_item_anchor_snapshot AS
WITH params AS (
  SELECT '2025-12-31' AS E  -- TODO: 观察期终点
),
base AS (
  SELECT
    si.item_id,
    si.shop_id,
    si.category_id,
    si.price_band,          -- 这是旧的 band（若你后面用新 band，可忽略此列）
    et.g,
    CASE
      WHEN et.g IS NOT NULL THEN dateadd(et.g, -1)
      ELSE dateadd(p.E, -1)
    END AS anchor_date
  FROM tmp_sampled_items si
  LEFT JOIN tmp_eligible_treated et
    ON si.item_id = et.item_id
  JOIN params p ON 1=1
),
snap AS (
  SELECT
    b.item_id,
    b.shop_id,
    b.category_id,
    b.g,
    b.anchor_date,
    d.cprice                         AS anchor_price,
    d.day_30_sales_volume             AS day_30_sales_volume_gm1,
    d.add_volume                      AS add_volume_gm1
  FROM base b
  LEFT JOIN item_daily_filter_v4 d
    ON b.item_id = d.item_id
   AND to_date(d.insert_time) = b.anchor_date
)
SELECT * FROM snap;

-- =========================================================
-- FINAL-1. Item 层（含基于 g-1 / anchor_date 价格的类目内等频 price_band_new）
-- =========================================================
CREATE TABLE IF NOT EXISTS final_item AS
WITH params AS (
  SELECT 4 AS BANDS   -- TODO: 分箱数，可改 5/10
),
joined AS (
  SELECT
    a.item_id,
    it.shop_id,
    it.category_id,
    it.category_name,
    a.g,
    a.anchor_date,
    a.anchor_price,
    it.properties,
    it.pic_url,
    it.pic_url_list,
    it.first_day_sales_volume,
    it.first_week_sales_volume,
    it.first_month_sales_volume,
    it.start_sell_date,
    it.nearly_sale_time,
    it.sale_time,
    it.comment_count,
    it.max_bad_rate,
    it.bad_rate
  FROM final_item_anchor_snapshot a
  JOIN item_info_combine_v2 it
    ON a.item_id = it.item_id
),
banded AS (
  SELECT
    j.*,
    ntile((SELECT BANDS FROM params))
      OVER (PARTITION BY j.category_id ORDER BY j.anchor_price) AS price_band_new
  FROM joined j
)
SELECT
  item_id,
  shop_id,
  category_id,
  category_name,
  price_band_new AS price_band,
  properties,
  pic_url,
  pic_url_list,
  first_day_sales_volume,
  first_week_sales_volume,
  first_month_sales_volume,
  start_sell_date,
  nearly_sale_time,
  sale_time
FROM banded;

-- =========================================================
-- FINAL-2. Shop 层
-- =========================================================
CREATE TABLE IF NOT EXISTS final_shop AS
SELECT
  s.shop_id,
  s.seller_id,
  s.rank_type,
  s.fans_num_no_unit AS fan_num,
  s.main_industry,
  s.good_comment_rate,
  s.total_sales_volume AS shop_total_sales_volume,
  s.total_item_amount
FROM (
  SELECT DISTINCT shop_id FROM final_item
) x
LEFT JOIN dws_shop_info_combine_v8 s
  ON x.shop_id = s.shop_id;

-- =========================================================
-- FINAL-3. Item–Comment 层（仅 treated item 的 g±W 窗口）
--        并补充 item-level comment_count/max_bad_rate/bad_rate
-- =========================================================
CREATE TABLE IF NOT EXISTS final_item_comment AS
WITH params AS (
  SELECT 60 AS W
),
treated_in_sample AS (
  SELECT
    fi.item_id,
    et.g
  FROM final_item fi
  JOIN tmp_eligible_treated et
    ON fi.item_id = et.item_id
),
item_stats AS (
  SELECT
    item_id,
    comment_count,
    max_bad_rate,
    bad_rate
  FROM item_info_combine_v2
)
SELECT
  c.item_id,
  c.comment_id,
  c.comment_seller_name AS reviewer_id,
  c.comment_time,
  c.comment_images,
  st.comment_count AS rcomment_count,
  st.max_bad_rate,
  st.bad_rate,
  c.comment_content
FROM ods_taobao_comment_content_info c
JOIN treated_in_sample ts
  ON c.item_id = ts.item_id
JOIN params p
  ON to_date(c.comment_time) BETWEEN dateadd(ts.g, -p.W) AND dateadd(ts.g, p.W)
LEFT JOIN item_stats st
  ON c.item_id = st.item_id;

-- =========================================================
-- FINAL-4. Item–Day 层（全年面板 + 必要标注与时变变量）
-- =========================================================
CREATE TABLE IF NOT EXISTS final_item_day AS
WITH params AS (
  SELECT '2025-01-01' AS S, '2025-12-31' AS E
),
day_panel AS (
  SELECT
    d.item_id,
    d.shop_id,
    d.pt,
    d.day_sales_volume,
    d.cprice AS price
  FROM tmp_item_day_y1 d
  JOIN final_item fi
    ON d.item_id = fi.item_id
  JOIN params p
    ON d.pt BETWEEN p.S AND p.E
),
t0 AS (
  SELECT item_id, t0 FROM tmp_item_t0
),
g AS (
  SELECT item_id, g FROM tmp_item_g
),
daily_comments AS (
  SELECT
    c.item_id,
    to_date(c.comment_time) AS pt,
    COUNT(1) AS daily_comment_cnt
  FROM ods_taobao_comment_content_info c
  GROUP BY c.item_id, to_date(c.comment_time)
),
snap AS (
  SELECT
    item_id,
    day_30_sales_volume_gm1,
    add_volume_gm1
  FROM final_item_anchor_snapshot
)
SELECT
  p.item_id,
  p.pt,
  t.t0 AS first_actual_sale_date,
  gg.g AS first_img_comment_date,
  CASE WHEN gg.g IS NOT NULL THEN datediff(p.pt, gg.g) ELSE NULL END AS event_time,
  CASE WHEN t.t0 IS NOT NULL THEN datediff(p.pt, t.t0) ELSE NULL END AS item_age,
  p.day_sales_volume AS outcome_day_sales_volume,
  p.price,
  s.day_30_sales_volume_gm1,
  s.add_volume_gm1,
  COALESCE(dc.daily_comment_cnt, 0) AS daily_comment_cnt
FROM day_panel p
LEFT JOIN t0 t
  ON p.item_id = t.item_id
LEFT JOIN g gg
  ON p.item_id = gg.item_id
LEFT JOIN snap s
  ON p.item_id = s.item_id
LEFT JOIN daily_comments dc
  ON p.item_id = dc.item_id
 AND p.pt = dc.pt;