-- =========================================================
-- 1. 过去3个月销量分层抽店（默认抽 1000 家）
-- =========================================================
CREATE TABLE IF NOT EXISTS tmp_sampled_shops AS
WITH params AS (
  SELECT
    '2025-12-31' AS E,     -- TODO: 观察期终点
    90 AS D90,
    1000 AS N_SHOPS,       -- TODO: 目标店铺数
    0.30 AS P_BIG,
    0.50 AS P_MID,
    0.20 AS P_SMALL
),
shop_sales AS (
  SELECT
    d.shop_id,
    SUM(COALESCE(d.day_sales_volume,0)) AS sales_3m
  FROM tmp_item_day_y1 d
  JOIN params p
    ON d.pt BETWEEN dateadd(p.E, -p.D90 + 1) AND p.E
  JOIN tmp_eligible_universe u
    ON d.item_id = u.item_id
  GROUP BY d.shop_id
),
ranked AS (
  SELECT
    s.*,
    ntile(3) OVER (ORDER BY s.sales_3m DESC) AS size_bucket, -- 1=大,2=中,3=小（按销量分位）
    row_number() OVER (PARTITION BY ntile(3) OVER (ORDER BY s.sales_3m DESC) ORDER BY rand()) AS rn_in_bucket
  FROM shop_sales s
),
quota AS (
  SELECT
    CAST(ROUND(p.N_SHOPS * p.P_BIG) AS BIGINT)   AS q_big,
    CAST(ROUND(p.N_SHOPS * p.P_MID) AS BIGINT)   AS q_mid,
    CAST(p.N_SHOPS - ROUND(p.N_SHOPS * p.P_BIG) - ROUND(p.N_SHOPS * p.P_MID) AS BIGINT) AS q_small
  FROM params p
)
SELECT
  r.shop_id,
  r.sales_3m,
  CASE r.size_bucket WHEN 1 THEN 'big' WHEN 2 THEN 'mid' ELSE 'small' END AS shop_size
FROM ranked r
CROSS JOIN quota q
WHERE (r.size_bucket = 1 AND r.rn_in_bucket <= q.q_big)
   OR (r.size_bucket = 2 AND r.rn_in_bucket <= q.q_mid)
   OR (r.size_bucket = 3 AND r.rn_in_bucket <= q.q_small);


-- =========================================================
-- 2. 店内 cell 构造 + 权重 + treated_cnt（用于可识别性约束）
-- =========================================================
CREATE TABLE IF NOT EXISTS tmp_shop_cell_stats AS
WITH params AS (
  SELECT '2025-12-31' AS E, 90 AS D90
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
  SELECT
    d.shop_id,
    u.category_id,
    u.price_band,
    SUM(COALESCE(d.day_sales_volume,0)) AS cell_sales_3m
  FROM tmp_item_day_y1 d
  JOIN params p
    ON d.pt BETWEEN dateadd(p.E, -p.D90 + 1) AND p.E
  JOIN u
    ON d.item_id = u.item_id
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


-- =========================================================
-- 3. 每个 shop 按 cell_sales 权重抽 2 个 cell（只在 treated_cnt>=3 内抽）
-- =========================================================
CREATE TABLE IF NOT EXISTS tmp_sampled_shop_cells AS
WITH candidates AS (
  SELECT
    sc.*,
    CASE WHEN sc.cell_sales_3m > 0 THEN (-ln(rand()) / sc.cell_sales_3m) ELSE (-ln(rand()) / 1) END AS wkey
  FROM tmp_shop_cell_stats sc
  JOIN tmp_sampled_shops ss
    ON sc.shop_id = ss.shop_id
  WHERE sc.eligible_treated_cnt >= 2
),
cand_cnt AS (
  SELECT shop_id, COUNT(*) AS n_cand
  FROM candidates
  GROUP BY shop_id
),
ranked AS (
  SELECT
    c.*,
    cc.n_cand,
    row_number() OVER (PARTITION BY c.shop_id ORDER BY c.wkey ASC) AS rn
  FROM candidates c
  JOIN cand_cnt cc
    ON c.shop_id = cc.shop_id
)
SELECT
  shop_id,
  category_id,
  price_band,
  cell_sales_3m,
  eligible_treated_cnt
FROM ranked
WHERE rn <= CASE WHEN n_cand >= 2 THEN 2 ELSE n_cand END;

-- =========================================================
-- 4 (updated for Version B): 每个 shop×cell 抽 10 items
-- 先抽 treated_base=2（若不足则全取），再补满到 10
-- =========================================================
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
treated2 AS (
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
  WHERE rn <= 2
),
rest8 AS (
  SELECT item_id, shop_id, category_id, price_band
  FROM (
    SELECT
      ci.*,
      row_number() OVER (
        PARTITION BY ci.shop_id, ci.category_id, ci.price_band
        ORDER BY rand()
      ) AS rn
    FROM cell_items ci
    LEFT JOIN treated2 t2
      ON ci.item_id = t2.item_id
    WHERE t2.item_id IS NULL
  ) r
  WHERE rn <= 8
),
union10 AS (
  SELECT * FROM treated2
  UNION ALL
  SELECT * FROM rest8
)
SELECT
  item_id,
  shop_id,
  category_id,
  price_band
FROM union10;

