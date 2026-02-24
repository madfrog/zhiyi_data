-- =========================================================
-- 0A. 一年 item-day 面板（核心字段）
-- =========================================================
set odps.sql.type.system.odps2=true;
CREATE TABLE IF NOT EXISTS tmp_item_day_y1 AS
SELECT 
  d.item_id, 
  d.shop_id, 
  to_date(d.insert_time) AS pt, 
  d.day_sales_volume, 
  d.cprice, 
  d.category_id, 
  d.root_category_id
FROM data_infra_datahub.item_daily_filter_v4 d
WHERE d.item_id IS NOT NULL 
  AND d.shop_id IS NOT NULL
  AND to_date(d.insert_time) BETWEEN '2025-01-01' AND '2025-12-31'
  AND pt >= '20170101' AND pt <= '20261231';


-- =========================================================
-- 0B. t0_i = first_actual_sale_date（首次销量>0）
-- =========================================================
CREATE TABLE IF NOT EXISTS tmp_item_t0 AS
SELECT
  item_id,
  MIN(pt) AS t0
FROM tmp_item_day_y1
WHERE day_sales_volume > 0
GROUP BY item_id;


-- =========================================================
-- 0C. g_i = first_img_comment_date（comment_images 非空）
-- =========================================================

CREATE TABLE IF NOT EXISTS tmp_item_g AS
SELECT
  c.item_id,
  MIN(to_date(c.comment_time)) AS g
FROM data_infra_datahub.ods_taobao_comment_content_info c
WHERE c.item_id IS NOT NULL
  AND c.comment_images IS NOT NULL
  AND c.comment_images != ''
  AND c.comment_images != '\\N'
  AND c.comment_images != '[]'
  AND length(trim(c.comment_images)) > 0
  AND to_date(c.comment_time) BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
GROUP BY c.item_id;



-- -- =========================================================
-- -- 0D. treated eligible items（严格满足边界/新品/面板）
-- -- =========================================================

CREATE TABLE IF NOT EXISTS tmp_eligible_treated AS
WITH params AS (
  SELECT
    DATE '2025-01-01' AS S,
    DATE '2025-12-31' AS E,
    60 AS W
),
treated_base AS (
  SELECT
    g.item_id,
    g.g,
    t.t0
  FROM tmp_item_g g
  JOIN tmp_item_t0 t
    ON g.item_id = t.item_id
),
panel_cnt AS (
  SELECT
    tb.item_id,
    tb.g,
    tb.t0,
    SUM(CASE 
          WHEN d.pt BETWEEN dateadd(tb.g, -60, 'dd') AND dateadd(tb.g, -1, 'dd') 
          THEN 1 ELSE 0 
        END) AS pre_days,
    SUM(CASE 
          WHEN d.pt BETWEEN tb.g AND dateadd(tb.g, 60, 'dd') 
          THEN 1 ELSE 0 
        END) AS post_days
  FROM treated_base tb
  JOIN tmp_item_day_y1 d
    ON tb.item_id = d.item_id
  GROUP BY tb.item_id, tb.g, tb.t0
)
SELECT /*+ MAPJOIN(p) */  -- ✅ 正确位置：SELECT 后，字段前
  pc.item_id,
  pc.g,
  pc.t0
FROM panel_cnt pc
JOIN params p   -- 不需要 ON 条件（MAPJOIN 允许广播）
WHERE pc.g BETWEEN dateadd(p.S, p.W, 'dd') AND dateadd(p.E, -p.W, 'dd')
  AND datediff(pc.g, pc.t0) >= 21
  AND pc.pre_days >= 50
  AND pc.post_days >= 50;


-- -- =========================================================
-- -- 0E. control eligible items（later-treated / never-treated 的抽样池）
-- --     实用门槛：一年面板记录 >=100 且 t0 <= E-60
-- -- =========================================================

CREATE TABLE IF NOT EXISTS tmp_eligible_control AS
WITH params AS (
  SELECT
    DATE '2025-01-01' AS S,
    DATE '2025-12-31' AS E,
    60 AS W
),
day_cnt AS (
  SELECT
    d.item_id,
    COUNT(1) AS day_rows
  FROM tmp_item_day_y1 d
  GROUP BY d.item_id
),
base AS (
  SELECT
    t.item_id,
    t.t0,
    dc.day_rows
  FROM tmp_item_t0 t
  JOIN day_cnt dc
    ON t.item_id = dc.item_id
)
SELECT /*+ MAPJOIN(p) */  -- ✅ MAPJOIN hint 放在 SELECT 后
  b.item_id,
  b.t0,
  b.day_rows
FROM base b
JOIN params p   -- ✅ 不需要 ON 1=1（MAPJOIN 允许广播）
WHERE b.day_rows >= 100
  AND b.t0 <= dateadd(p.E, -p.W, 'dd');

-- -- =========================================================
-- -- 0F. eligible item universe（treated eligible + control eligible）
-- --     并构造 category × price_band cell 所需字段
-- -- =========================================================
CREATE TABLE IF NOT EXISTS tmp_eligible_universe AS
WITH params AS (
  SELECT 4 AS BANDS  -- TODO: 价位分箱数（等频），可改 5/10
),
u0 AS (
  SELECT item_id, 'treated' AS elig_type FROM tmp_eligible_treated
  UNION ALL
  SELECT item_id, 'control' AS elig_type FROM tmp_eligible_control
),
price_fallback AS (
  SELECT
    item_id,
    CAST(AVG(cprice) AS BIGINT) AS avg_cprice_y1
  FROM tmp_item_day_y1
  GROUP BY item_id
),
u AS (
  SELECT
    u0.item_id,
    u0.elig_type,
    it.shop_id,
    it.category_id,
    it.category_name,
    COALESCE(it.usual_cprice, it.cprice, pf.avg_cprice_y1) AS price_ref
  FROM u0
  JOIN item_info_combine_v2 it
    ON u0.item_id = it.item_id
  LEFT JOIN price_fallback pf
    ON u0.item_id = pf.item_id
  WHERE it.shop_id IS NOT NULL
    AND it.category_id IS NOT NULL
    AND COALESCE(it.usual_cprice, it.cprice, pf.avg_cprice_y1) IS NOT NULL
),
banded AS (
  SELECT
    u.*,
    ntile((SELECT BANDS FROM params)) OVER (PARTITION BY u.category_id ORDER BY u.price_ref) AS price_band
  FROM u
)
SELECT * FROM banded;