-- ********************************************************************--
-- author:算法组外部专家-xinyucao
-- create time:2026-02-09 23:47:28
-- ********************************************************************--
-- select * from tmp_item_full_info_with_first_img_comment_7 limit 1000;

-- SELECT 
--     *,
--     ROW_NUMBER() OVER (PARTITION BY item_id) AS rn
-- FROM tmp_item_full_info_with_first_img_comment
-- ORDER BY item_id, rn
-- LIMIT 3000;

-- SELECT *
-- FROM (
--     SELECT 
--         *,
--         COUNT(*) OVER (PARTITION BY item_id) AS group_size
--     FROM tmp_item_full_info_with_first_img_comment
-- ) t
-- WHERE group_size > 3
-- ORDER BY item_id  -- 可按需调整组内排序字段
-- LIMIT 1000;  -- ODPS 要求 ORDER BY 必须带 LIMIT

-- select * from data_infra_datahub.item_daily_filter_v4 where item_id = '10015671353' and pt >= '20210101' and pt <= '20261231';

-- 分组统计不同rank_type下店铺数量
-- SELECT 
--     rank_type,
--     COUNT(DISTINCT item_shop_id) AS shop_count
-- FROM tmp_item_full_info_with_first_img_comment_3
-- GROUP BY rank_type
-- ORDER BY shop_count DESC;

-- select * from tmp_sampled_items_10k LIMIT 1000;

-- select COUNT(DISTINCT item_id) from tmp_sampled_items_10k;

-- select * from tmp_daily_all_records_for_sampled_items where item_id = '946187398430';




-- SELECT
--     MIN(first_img_comment_time) AS earliest_first_img_comment_time,
--     MAX(first_img_comment_time) AS latest_first_img_comment_time
-- FROM (
--     SELECT
--         item_id,
--         MIN(comment_time) AS first_img_comment_time
--     FROM data_infra_datahub.ods_taobao_comment_content_info
--     WHERE comment_images IS NOT NULL 
--       AND comment_images != ''
--       AND comment_images != '[]'
--     GROUP BY item_id
-- ) t;

-- SELECT 
--     item_id,
--     seller_id,
--     comment_id,
--     comment_content,
--     comment_seller_name,
--     comment_images,
--     comment_sku,
--     comment_time,
--     `version`
-- FROM data_infra_datahub.ods_taobao_comment_content_info
-- WHERE comment_time = '2017-02-25 00:42:41'
--   AND comment_images IS NOT NULL 
--   AND comment_images != '';

-- select * from data_infra_datahub.item_info_combine_v2 where item_id='526143441090';

-- 统计时间范围
-- SELECT 
--     MIN(start_sell_date) AS min_start_sell_date,
--     MAX(start_sell_date) AS max_start_sell_date
-- FROM tmp_item_full_info_with_first_img_comment_14
-- WHERE start_sell_date IS NOT NULL;

-- 统计comment表中最早一个带图评论出现的时间范围
-- SELECT
--     MIN(first_img_comment_time) AS earliest_first_img_comment_time,
--     MAX(first_img_comment_time) AS latest_first_img_comment_time
-- FROM (
--     SELECT
--         c.item_id,
--         MIN(c.comment_time) AS first_img_comment_time
--     FROM data_infra_datahub.ods_taobao_comment_content_info c
--     INNER JOIN (
--         SELECT DISTINCT item_id
--         FROM tmp_item_full_info_with_first_img_comment_14
--         WHERE item_id IS NOT NULL
--     ) t
--     ON CAST(c.item_id AS STRING) = t.item_id  -- 或者 CAST(t.item_id AS BIGINT) = c.item_id
--     WHERE c.comment_images IS NOT NULL 
--       AND c.comment_images != ''
--       AND c.comment_images != '[]'
--     GROUP BY c.item_id
-- ) final;


-- 统计每种 rank_type 的总粉丝数及其占全量粉丝的百分比
-- SELECT 
--     rank_type,
--     total_fans,
--     all_fans,
--     ROUND(total_fans * 100.0 / all_fans, 4) AS percentage
-- FROM (
--     SELECT 
--         rank_type,
--         SUM(fans_num_no_unit) AS total_fans,
--         SUM(SUM(fans_num_no_unit)) OVER () AS all_fans  -- 全局总粉丝数
--     FROM tmp_item_full_info_with_first_img_comment_7
--     WHERE 
--         rank_type IN ('1', '2', '3', '4', '\\N')
--         AND fans_num_no_unit IS NOT NULL
--         AND fans_num_no_unit > 0
--         AND item_shop_id IS NOT NULL
--     GROUP BY rank_type
-- ) t
-- ORDER BY 
--     CASE WHEN rank_type = '\\N' THEN 999 ELSE CAST(rank_type AS BIGINT) END;

-- 检查 rank_type = '\N' 的记录分布
-- SELECT 
--     COUNT(*) AS total_rows,
--     COUNT(item_shop_id) AS non_null_shop_id,
--     COUNT(fans_num_no_unit) AS non_null_fans,
--     SUM(CASE WHEN fans_num_no_unit > 0 THEN 1 ELSE 0 END) AS positive_fans_count,
--     SUM(fans_num_no_unit) AS sum_fans
-- FROM tmp_item_full_info_with_first_img_comment_7
-- WHERE rank_type = '\\N';


-- 粉丝数量统计
-- 整体粉丝数分布概览
-- SELECT 
--     COUNT(*) AS total_shops,                          -- 店铺总数
--     SUM(fans_num_no_unit) AS total_fans,             -- 总粉丝量
--     AVG(fans_num_no_unit) AS avg_fans,               -- 平均粉丝数
--     MEDIAN(fans_num_no_unit) AS median_fans,         -- 中位数粉丝（更稳健）
--     MAX(fans_num_no_unit) AS max_fans,               -- 最大粉丝数
--     MIN(fans_num_no_unit) AS min_fans,               -- 最小粉丝数
--     STDDEV_POP(fans_num_no_unit) AS std_fans         -- 标准差（衡量离散度）
-- FROM tmp_item_full_info_with_first_img_comment_14
-- WHERE 
--     fans_num_no_unit IS NOT NULL 
--     AND fans_num_no_unit >= 0;  -- 排除负数（如有）


-- 分位数：了解不同层级的粉丝门槛
-- SELECT 
--     PERCENTILE(fans_num_no_unit, 0.5) AS p50_median,   -- 50% 店铺低于此值
--     PERCENTILE(fans_num_no_unit, 0.75) AS p75,         -- 75% 分位
--     PERCENTILE(fans_num_no_unit, 0.90) AS p90,         -- 头部10%门槛
--     PERCENTILE(fans_num_no_unit, 0.95) AS p95,         -- 头部5%门槛
--     PERCENTILE(fans_num_no_unit, 0.99) AS p99           -- 头部1%门槛
-- FROM tmp_item_full_info_with_first_img_comment_7
-- WHERE fans_num_no_unit IS NOT NULL AND fans_num_no_unit >= 0;

-- 统计粉丝数在 [0,100], (100,200], (200,300], (300,400] 区间内的商品数量
SELECT 
    fan_range,
    COUNT(item_id) AS item_count
FROM (
    SELECT 
        item_id,
        CASE 
            WHEN fans_num_no_unit >= 0   AND fans_num_no_unit <= 28435563 THEN '[0,28435563]'
            WHEN fans_num_no_unit > 28435563  AND fans_num_no_unit <= 56871126 THEN '(28435563,56871126]'
            WHEN fans_num_no_unit > 56871126  AND fans_num_no_unit <= 85306689 THEN '(56871126,85306689]'
            WHEN fans_num_no_unit > 85306689  AND fans_num_no_unit <= 113742253 THEN '(85306689,113742253]'
            ELSE NULL
        END AS fan_range
    FROM tmp_item_full_info_with_first_img_comment_14
    WHERE 
        fans_num_no_unit IS NOT NULL
        AND fans_num_no_unit >= 0
        AND item_id IS NOT NULL
) t
WHERE fan_range IS NOT NULL
GROUP BY fan_range
ORDER BY 
    CASE fan_range
        WHEN '[0,28435563]'    THEN 1
        WHEN '(28435563,56871126]'  THEN 2
        WHEN '(56871126,85306689]'  THEN 3
        WHEN '(85306689,113742253]'  THEN 4
    END;