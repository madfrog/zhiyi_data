-- ********************************************************************--
-- author:算法组外部专家-信誉草
-- create time:2026-02-07 23:33:45
-- ********************************************************************--
SELECT 
    COUNT(*) AS item_count
FROM (
    SELECT 
        v.item_id,
        v.start_sell_date,  -- 原始字符串，如 '2023-05-11'
        TO_CHAR(f.first_img_comment_time, 'yyyy-mm-dd') AS img_date_str  -- 转为相同格式字符串
    FROM (
        SELECT 
            item_id,
            start_sell_date
        FROM data_infra_datahub.item_info_combine_v2
        WHERE 
            start_sell_volume IS NOT NULL
            AND start_sell_volume > 0
            AND start_sell_date IS NOT NULL
            AND TRIM(start_sell_date) != ''
            AND TRIM(start_sell_date) != '[]'  -- 虽然不太可能，但防御性处理
    ) v
    INNER JOIN (
        SELECT 
            item_id,
            MIN(comment_time) AS first_img_comment_time
        FROM data_infra_datahub.ods_taobao_comment_content_info
        WHERE 
            comment_images IS NOT NULL
            AND TRIM(comment_images) != ''
            AND TRIM(comment_images) != '[]'
        GROUP BY item_id
    ) f
    ON v.item_id = f.item_id
) t
WHERE 
    t.start_sell_date != t.img_date_str;
