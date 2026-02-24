-- ********************************************************************--
-- author:算法组外部专家-xinyucao
-- create time:2026-02-22 20:41:43
-- ********************************************************************--
-- =========================================================
-- 1. 过去3个月销量分层抽店（默认抽 1000 家）
-- =========================================================
set odps.sql.type.system.odps2=true;

CREATE TABLE IF NOT EXISTS tmp_sampled_shops AS
WITH params AS (
  SELECT
    DATE '2025-12-31' AS E,
    90 AS D90,
    1000 AS N_SHOPS,
    0.30 AS P_BIG,
    0.50 AS P_MID,
    0.20 AS P_SMALL
),
shop_sales AS (
  SELECT /*+ MAPJOIN(p) */
    d.shop_id,
    SUM(COALESCE(d.day_sales_volume, 0)) AS sales_3m
  FROM tmp_item_day_y1 d
  JOIN params p
  JOIN tmp_eligible_universe u
    ON d.item_id = u.item_id
  WHERE d.pt BETWEEN dateadd(p.E, -p.D90 + 1, 'dd') AND p.E
  GROUP BY d.shop_id
),
-- Step 1: 先计算 size_bucket
ranked_step1 AS (
  SELECT
    s.*,
    ntile(3) OVER (ORDER BY s.sales_3m DESC) AS size_bucket
  FROM shop_sales s
),
-- Step 2: 再基于 size_bucket 计算行号
ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY size_bucket
      ORDER BY rand()
    ) AS rn_in_bucket
  FROM ranked_step1
),
quota AS (
  SELECT /*+ MAPJOIN(p) */
    CAST(ROUND(p.N_SHOPS * p.P_BIG) AS BIGINT) AS q_big,
    CAST(ROUND(p.N_SHOPS * p.P_MID) AS BIGINT) AS q_mid,
    CAST(p.N_SHOPS - ROUND(p.N_SHOPS * p.P_BIG) - ROUND(p.N_SHOPS * p.P_MID) AS BIGINT) AS q_small
  FROM params p
)
SELECT /*+ MAPJOIN(q) */
  r.shop_id,
  r.sales_3m,
  CASE r.size_bucket WHEN 1 THEN 'big' WHEN 2 THEN 'mid' ELSE 'small' END AS shop_size
FROM ranked r
JOIN quota q
WHERE (r.size_bucket = 1 AND r.rn_in_bucket <= q.q_big)
   OR (r.size_bucket = 2 AND r.rn_in_bucket <= q.q_mid)
   OR (r.size_bucket = 3 AND r.rn_in_bucket <= q.q_small);


-- =========================================================
-- 2. 店内 cell 构造 + 权重 + treated_cnt（用于可识别性约束）
-- =========================================================

CREATE TABLE IF NOT EXISTS tmp_shop_cell_stats AS
WITH params AS (
  SELECT
    DATE '2025-12-31' AS E,   -- ✅ 关键修复：显式 DATE 类型
    90 AS D90
),
u AS (
  SELECT
    eu.item_id,
    eu.shop_id,
    eu.category_id,
    eu.price_band,
    CASE WHEN et.item_id IS NOT NULL THEN 1 ELSE 0 END AS is_eligible_treated
  FROM tmp_eligible_universe eu
  LEFT JOIN tmp_eligible_treated et
    ON eu.item_id = et.item_id
),
cell_sales AS (
  SELECT /*+ MAPJOIN(p) */  -- ✅ 启用 MAPJOIN 广播小表
    d.shop_id,
    u.category_id,
    u.price_band,
    SUM(COALESCE(d.day_sales_volume, 0)) AS cell_sales_3m
  FROM tmp_item_day_y1 d
  JOIN params p   -- 普通 JOIN，由 MAPJOIN 处理
  JOIN u
    ON d.item_id = u.item_id
  WHERE d.pt BETWEEN dateadd(p.E, -p.D90 + 1, 'dd') AND p.E  -- ✅ 在 WHERE 中过滤（合法）
  GROUP BY d.shop_id, u.category_id, u.price_band
),
treated_cnt AS (
  SELECT
    u.shop_id,
    u.category_id,
    u.price_band,
    SUM(u.is_eligible_treated) AS eligible_treated_cnt
  FROM u
  GROUP BY u.shop_id, u.category_id, u.price_band
)
SELECT
  cs.shop_id,
  cs.category_id,
  cs.price_band,
  cs.cell_sales_3m,
  tc.eligible_treated_cnt
FROM cell_sales cs
JOIN treated_cnt tc
  ON cs.shop_id = tc.shop_id
 AND cs.category_id = tc.category_id
 AND cs.price_band = tc.price_band;


-- -- =========================================================
-- -- 3. 每个 shop 按 cell_sales 权重抽 2 个 cell（只在 treated_cnt>=3 内抽）
-- -- =========================================================

CREATE TABLE IF NOT EXISTS tmp_sampled_shop_cells AS
WITH candidates AS (
  SELECT
    sc.*,
    CASE WHEN sc.cell_sales_3m > 0 THEN (-log(rand()) / sc.cell_sales_3m) ELSE 999999999 END AS wkey
  FROM tmp_shop_cell_stats sc
  JOIN tmp_sampled_shops ss
    ON sc.shop_id = ss.shop_id
  WHERE sc.eligible_treated_cnt >= 2
)
SELECT
  shop_id,
  category_id,
  price_band,
  cell_sales_3m,
  eligible_treated_cnt
FROM (
  SELECT
    c.*,
    row_number() OVER (PARTITION BY c.shop_id ORDER BY c.wkey ASC) AS rn
  FROM candidates c
) t
WHERE rn <= 2;

-- -- =========================================================
-- -- 4. 每个 shop×cell 抽 10 items：先 treated 3 个，再补满 10
-- -- =========================================================
CREATE TABLE IF NOT EXISTS tmp_sampled_items AS
WITH cell_items AS (
  SELECT
    eu.item_id,
    eu.shop_id,
    eu.category_id,
    eu.price_band,
    CASE WHEN et.item_id IS NOT NULL THEN 1 ELSE 0 END AS is_eligible_treated
  FROM tmp_eligible_universe eu
  JOIN tmp_sampled_shop_cells sc
    ON eu.shop_id = sc.shop_id
   AND eu.category_id = sc.category_id
   AND eu.price_band = sc.price_band
  LEFT JOIN tmp_eligible_treated et
    ON eu.item_id = et.item_id
),
pick_treated3 AS (
  SELECT item_id, shop_id, category_id, price_band
  FROM (
    SELECT
      ci.*,
      row_number() OVER (
        PARTITION BY ci.shop_id, ci.category_id, ci.price_band
        ORDER BY rand()
      ) AS rn
    FROM cell_items ci
    WHERE ci.is_eligible_treated = 1
  ) t
  WHERE rn <= 3
),
pick_rest7 AS (
  SELECT item_id, shop_id, category_id, price_band
  FROM (
    SELECT
      ci.*,
      row_number() OVER (
        PARTITION BY ci.shop_id, ci.category_id, ci.price_band
        ORDER BY rand()
      ) AS rn
    FROM cell_items ci
    LEFT JOIN pick_treated3 p3
      ON ci.item_id = p3.item_id
    WHERE p3.item_id IS NULL
  ) r
  WHERE rn <= 7
),
union10 AS (
  SELECT * FROM pick_treated3
  UNION ALL
  SELECT * FROM pick_rest7
)
SELECT
  u.item_id,
  u.shop_id,
  u.category_id,
  u.price_band
FROM union10 u;