-- ********************************************************************
-- author: 算法组外部专家-xinyucao
-- create time: 2026-02-12 19:05:08
-- 功能：两阶段分层抽样（按粉丝数分层 → 抽 shop → 抽 item）
--       并排除 tmp_accumulated_item_ids 中已抽样的 item_id
-- 输出：约 10,000 条未重复的商品记录（4 × 250 × 10）
-- ********************************************************************

CREATE TABLE IF NOT EXISTS tmp_sampled_items_10k_30 
LIFECYCLE 30
AS
WITH 
-- Step 0: 获取已抽样的 item_id 集合（用于排除）
excluded_items AS (
    SELECT DISTINCT item_id 
    FROM tmp_accumulated_item_ids
    WHERE item_id IS NOT NULL
),

-- Step 1: 从剩余数据中计算粉丝数 min/max（用于等宽分箱）
fan_bounds AS (
    SELECT 
        MIN(fans_num_no_unit) AS min_fan,
        MAX(fans_num_no_unit) AS max_fan
    FROM tmp_item_full_info_with_first_img_comment_30 t
    LEFT JOIN excluded_items e ON t.item_id = e.item_id
    WHERE 
        e.item_id IS NULL  -- 排除已抽样商品
        AND t.fans_num_no_unit IS NOT NULL 
        AND t.fans_num_no_unit >= 0
        AND t.item_id IS NOT NULL
        AND t.item_shop_id IS NOT NULL
),

-- Step 2: 为每条记录分配 fan_level（1～4），同样排除已抽样商品
-- Step 2: 分配 fan_level
items_with_level AS (
    SELECT /*+ MAPJOIN(fb) */
        t.*,
        CASE 
            WHEN fb.max_fan = fb.min_fan THEN 1
            ELSE CEIL(
                (t.fans_num_no_unit - fb.min_fan) * 4.0 / (fb.max_fan - fb.min_fan + 1)
            )
        END AS fan_level
    FROM tmp_item_full_info_with_first_img_comment_30 t
    LEFT JOIN excluded_items e ON t.item_id = e.item_id,
    fan_bounds fb
    WHERE 
        e.item_id IS NULL
        AND t.fans_num_no_unit IS NOT NULL 
        AND t.fans_num_no_unit >= 0
        AND t.item_id IS NOT NULL
        AND t.item_shop_id IS NOT NULL
),

-- Step 3: 在每个 fan_level 中随机抽取最多 250 个 shop（去重）
selected_shops AS (
    SELECT 
        fan_level,
        item_shop_id AS shop_id
    FROM (
        SELECT 
            fan_level,
            item_shop_id,
            ROW_NUMBER() OVER (
                PARTITION BY fan_level 
                ORDER BY RAND()
            ) AS rn
        FROM items_with_level
        GROUP BY fan_level, item_shop_id
    ) t
    WHERE rn <= 250
),

-- Step 4: 对每个选中的 shop，在其 fan_level 内随机抽取最多 10 个 item
final_sample AS (
    SELECT *
    FROM (
        SELECT 
            i.*,
            ROW_NUMBER() OVER (
                PARTITION BY i.fan_level, i.item_shop_id 
                ORDER BY RAND()
            ) AS rn_item
        FROM items_with_level i
        INNER JOIN selected_shops s 
            ON i.fan_level = s.fan_level 
           AND i.item_shop_id = s.shop_id
    ) t
    WHERE rn_item <= 10
)

-- Step 5: 输出最终抽样结果（无 LIMIT，保留全部 ～10,000 条）
SELECT * FROM final_sample;

-- SELECT COUNT(*) as c from tmp_sampled_items_10k_30;

-- SELECT * FROM final_sample;