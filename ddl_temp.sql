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