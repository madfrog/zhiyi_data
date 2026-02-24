-- ********************************************************************--
-- author:算法组外部专家-xinyucao
-- create time:2026-02-12 09:41:24
-- ********************************************************************--
-- 创建目标临时表（结构同 ddl_temp.sql）
CREATE TABLE IF NOT EXISTS tmp_item_no_early_img_comment 
AS
SELECT 
    i.item_id,
    i.title,
    i.pic_url,
    i.pic_url_list,
    i.category_id,
    i.category_name,
    i.shop_id AS item_shop_id,
    i.sale_time,
    i.cprice AS item_cprice,
    i.collect,
    i.comment_count,
    i.first_day_sales_volume,
    i.first_day_sale,
    i.first_week_sales_volume,
    i.first_week_sale,
    i.first_month_sales_volume,
    i.first_month_sale,
    i.first_day_collect,
    i.sale_day,
    i.sale_7day,
    i.sale_30day,
    i.total_sale_volume,
    i.total_sale_amount,
    i.first_insert_time,
    i.video_url,
    i.bad_rate,
    i.properties,
    i.bad_comment_count,
    i.start_sell_volume,
    i.start_sell_date,
    i.start_sell_amount,
    -- 评论图片统计（全量，非仅窗口期内）
    COALESCE(c_all.img_comment_cnt, 0) AS img_comment_cnt,
    COALESCE(c_all.no_img_comment_cnt, 0) AS no_img_comment_cnt,
    -- 最早带图评论时间（用于筛选）
    c_first.first_img_comment_time,
    -- 店铺信息（直接关联）
    s.seller_id AS shop_seller_id,
    s.rank,
    s.rank_type,
    s.main_industry,
    s.good_comment_rate,
    s.status AS shop_status,
    s.label_industry,
    s.label_type,
    s.label_style,
    s.total_sales_volume AS shop_total_sales_volume,
    s.total_sale AS shop_total_sale,
    s.total_item_amount,
    s.fans_num_no_unit,
    -- 衍生字段
    TO_CHAR(c_first.first_img_comment_time, 'yyyy-mm-dd') AS first_img_comment_date_str,
    CASE 
        WHEN c_first.first_img_comment_time IS NOT NULL 
        THEN DATEDIFF(c_first.first_img_comment_time, TO_DATE(i.start_sell_date, 'yyyymmdd'), 'dd')
    END AS days_diff
FROM data_infra_datahub.item_info_combine_v2 i
-- 关联店铺信息
INNER JOIN data_infra_datahub.dws_shop_info_combine_v8 s 
    ON i.shop_id = s.shop_id
-- 关联最早带图评论时间（用于筛选条件）
LEFT JOIN (
    SELECT 
        item_id,
        MIN(comment_time) AS first_img_comment_time
    FROM data_infra_datahub.ods_taobao_comment_content_info
    WHERE 
        comment_images IS NOT NULL 
        AND TRIM(comment_images) != ''
        AND TRIM(comment_images) != '[]'
        AND TRIM(comment_images) != '\\N'
    GROUP BY item_id
) c_first 
    ON i.item_id = c_first.item_id
-- 关联全量评论图片统计（用于输出字段）
LEFT JOIN (
    SELECT 
        item_id,
        SUM(CASE WHEN comment_images IS NOT NULL 
                   AND TRIM(comment_images) != '' 
                   AND TRIM(comment_images) != '[]' 
                   AND TRIM(comment_images) != '\\N' 
              THEN 1 ELSE 0 END) AS img_comment_cnt,
        SUM(CASE WHEN comment_images IS NULL 
                   OR TRIM(comment_images) = '' 
                   OR TRIM(comment_images) = '[]' 
                   OR TRIM(comment_images) = '\\N' 
              THEN 1 ELSE 0 END) AS no_img_comment_cnt
    FROM data_infra_datahub.ods_taobao_comment_content_info
    GROUP BY item_id
) c_all 
    ON i.item_id = c_all.item_id
-- 主筛选条件
WHERE 
    -- 条件1: 开卖时间 <= 2025-10-31 (即 window_end - 60天)
    i.start_sell_date IS NOT NULL
    AND i.start_sell_date <= '20251031'
    -- 条件2: 首张买家秀在 window_end 之后 或 从未出现
    AND (
        c_first.first_img_comment_time IS NULL 
        OR c_first.first_img_comment_time > TO_DATE('2025-12-31', 'yyyy-mm-dd')
    );

-- select * from tmp_item_no_early_img_comment limit 1000;