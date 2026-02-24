-- 创建临时宽表（不含 item_daily_filter_v4 字段）
CREATE TABLE IF NOT EXISTS tmp_item_full_info_with_first_img_comment_7 (
    -- 来自 item_info_combine_v2
    item_id STRING,
    title STRING,
    pic_url STRING,
    pic_url_list STRING,
    category_id STRING,
    category_name STRING,
    
    item_shop_id STRING,
    sale_time DATETIME,
    item_cprice DOUBLE,
    collect BIGINT,
    comment_count BIGINT,
    first_day_sales_volume BIGINT,
    first_day_sale DOUBLE,
    first_week_sales_volume BIGINT,
    first_week_sale DOUBLE,
    first_month_sales_volume BIGINT,
    first_month_sale DOUBLE,
    first_day_collect BIGINT,
    sale_day BIGINT,
    sale_7day BIGINT,
    sale_30day BIGINT,
    total_sale_volume BIGINT,
    total_sale_amount DOUBLE,
    first_insert_time DATETIME,
    video_url STRING,
    bad_rate DOUBLE,
    properties STRING,
    bad_comment_count BIGINT,
    start_sell_volume BIGINT,
    start_sell_date STRING,
    start_sell_amount DOUBLE,

    -- 新增：评论图片统计字段
    img_comment_cnt BIGINT,        -- 带图评论数
    no_img_comment_cnt BIGINT,     -- 无图评论数

    -- 来自 ods_taobao_comment_content_info（最早一条带图评论）
    first_img_comment_time DATETIME,

    -- 来自 dws_shop_info_combine_v8（直接通过 item.shop_id 关联）
    shop_seller_id STRING,
    rank BIGINT,
    rank_type STRING,
    main_industry STRING,
    good_comment_rate DOUBLE,
    shop_status STRING,
    label_industry STRING,
    label_type STRING,
    label_style STRING,
    shop_total_sales_volume BIGINT,
    shop_total_sale DOUBLE,
    total_item_amount BIGINT,
    fans_num_no_unit BIGINT,

    -- 衍生分析字段
    first_img_comment_date_str STRING,
    days_diff BIGINT
)
COMMENT '商品宽表：首日销量>0 且 首张买家秀在开卖7天后（无daily表，含评论图片统计）';

INSERT OVERWRITE TABLE tmp_item_full_info_with_first_img_comment_7
SELECT 
    -- item_info_combine_v2 字段
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

    -- 评论图片统计
    c.img_comment_cnt,
    c.no_img_comment_cnt,

    -- 最早带图评论时间
    c.first_img_comment_time,

    -- dws_shop_info_combine_v8 字段（直接通过 i.shop_id 关联）
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
    TO_CHAR(c.first_img_comment_time, 'yyyy-mm-dd') AS first_img_comment_date_str,
    DATEDIFF(c.first_img_comment_time, TO_DATE(i.start_sell_date, 'yyyy-mm-dd'), 'dd') AS days_diff

FROM data_infra_datahub.item_info_combine_v2 i

-- 关联评论聚合数据
INNER JOIN (
    SELECT 
        item_id,
        SUM(CASE 
            WHEN comment_images IS NOT NULL
                 AND TRIM(comment_images) != ''
                 AND TRIM(comment_images) != '[]'
                 AND TRIM(comment_images) != '\\N'
            THEN 1 ELSE 0 END) AS img_comment_cnt,
        SUM(CASE 
            WHEN comment_images IS NULL
                 OR TRIM(comment_images) = ''
                 OR TRIM(comment_images) = '[]'
                 OR TRIM(comment_images) = '\\N'
            THEN 1 ELSE 0 END) AS no_img_comment_cnt,
        MIN(CASE 
            WHEN comment_images IS NOT NULL
                 AND TRIM(comment_images) != ''
                 AND TRIM(comment_images) != '[]'
                 AND TRIM(comment_images) != '\\N'
            THEN comment_time END) AS first_img_comment_time
    FROM data_infra_datahub.ods_taobao_comment_content_info
    GROUP BY item_id
) c ON i.item_id = c.item_id

-- 直接关联店铺信息（通过 item_info_combine_v2.shop_id）
INNER JOIN data_infra_datahub.dws_shop_info_combine_v8 s
    ON i.shop_id = s.shop_id  -- 👈 关键修改：不再通过 daily 表中转

-- 主筛选条件
WHERE 
    i.start_sell_volume > 0
    AND i.start_sell_date IS NOT NULL
    AND TRIM(i.start_sell_date) != ''
    AND c.first_img_comment_time IS NOT NULL
    AND c.first_img_comment_time >= TO_DATE(i.start_sell_date, 'yyyy-mm-dd')
    AND DATEDIFF(c.first_img_comment_time, TO_DATE(i.start_sell_date, 'yyyy-mm-dd'), 'dd') >= 3;