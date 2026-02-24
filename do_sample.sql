-- 创建临时表 tmp_sampled_items_10k 并写入分层抽样数据
-- 创建临时表 tmp_sampled_items_10k：按粉丝数区间分层抽样
CREATE TABLE IF NOT EXISTS tmp_sampled_items_10k_7
AS
SELECT *
FROM (
    SELECT 
        *,
        CASE 
            WHEN fans_num_no_unit >= 0 AND fans_num_no_unit <= 28435563 THEN '[0,28435563]'
            WHEN fans_num_no_unit > 28435563 AND fans_num_no_unit <= 56871126 THEN '(28435563,56871126]'
            WHEN fans_num_no_unit > 56871126 AND fans_num_no_unit <= 85306689 THEN '(56871126,85306689]'
            WHEN fans_num_no_unit > 85306689 AND fans_num_no_unit <= 113742253 THEN '(85306689,113742253]'
            ELSE NULL
        END AS fan_range,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN fans_num_no_unit >= 0 AND fans_num_no_unit <= 28435563 THEN 1
                    WHEN fans_num_no_unit > 28435563 AND fans_num_no_unit <= 56871126 THEN 2
                    WHEN fans_num_no_unit > 56871126 AND fans_num_no_unit <= 85306689 THEN 3
                    WHEN fans_num_no_unit > 85306689 AND fans_num_no_unit <= 113742253 THEN 4
                    ELSE 999
                END
            ORDER BY RAND()
        ) AS rn
    FROM tmp_item_full_info_with_first_img_comment_3
    WHERE 
        item_id IS NOT NULL
        AND fans_num_no_unit IS NOT NULL
        AND fans_num_no_unit >= 0
        AND fans_num_no_unit <= 113742253  -- 限定在目标区间内
) t
WHERE 
    (fan_range = '[0,28435563]' AND rn <= 9796)
 OR (fan_range = '(28435563,56871126]' AND rn <= 77)
 OR (fan_range = '(56871126,85306689]' AND rn <= 27)
 OR (fan_range = '(85306689,113742253]' AND rn <= 100);

-- SELECT COUNT(*) as c from tmp_sampled_items_10k_3;


-- 创建临时表：仅包含 item_daily_filter_v4 中被抽样商品的记录
-- 创建临时表：包含抽样商品的所有 daily 记录，并按 item_id 分组、insert_time 倒序
CREATE TABLE IF NOT EXISTS tmp_daily_all_records_for_sampled_items 
AS
SELECT 
    d.item_id,
    d.shop_id,
    d.sale_time,
    d.insert_time,
    d.root_category_id,
    d.category_id,
    d.cprice,
    d.day_30_sales_volume,
    d.day_sales_volume,
    d.add_volume,
    d.add_amount,
    d.pt,
    -- 可选：添加组内倒序行号（最新记录 rn=1）
    ROW_NUMBER() OVER (
        PARTITION BY d.item_id 
        ORDER BY d.insert_time DESC, d.pt DESC
    ) AS rn_desc
FROM data_infra_datahub.item_daily_filter_v4 d
WHERE 
    d.pt >= '20170101' AND d.pt <= '20261231'
    AND CAST(d.item_id AS STRING) IN (
        SELECT DISTINCT item_id
        FROM tmp_sampled_items_10k
        WHERE item_id IS NOT NULL
    )
-- 注意：ODPS 中 CREATE TABLE 无法保证物理顺序，ORDER BY 仅用于 LIMIT 场景
-- 如果不需要行号，可删除 rn_desc 字段和窗口函数
;

-- 创建临时表：包含所有抽样商品的评论记录
CREATE TABLE IF NOT EXISTS tmp_comments_for_sampled_items 
AS
SELECT 
    c.item_id,
    c.seller_id,
    c.comment_id,
    c.comment_content,
    c.comment_seller_name,
    c.comment_images,
    c.comment_sku,
    c.comment_time,
    c.version
FROM ods_taobao_comment_content_info c
WHERE 
    c.item_id IS NOT NULL
    AND CAST(c.item_id AS STRING) IN (
        SELECT DISTINCT CAST(item_id AS STRING)
        FROM tmp_sampled_items_10k
        WHERE item_id IS NOT NULL
    );