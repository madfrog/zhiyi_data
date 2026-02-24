SELECT 
    COUNT(*) AS item_count
FROM (
    SELECT 
        v.item_id,
        TO_DATE(v.start_sell_date, 'yyyy-mm-dd') AS first_actual_sale_date,          -- 转为 DATETIME
        f.first_img_comment_time                                          -- 本身是 DATETIME
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
    t.first_img_comment_time >= t.first_actual_sale_date  -- 买家秀不早于开卖日
    AND DATEDIFF(t.first_img_comment_time, t.first_actual_sale_date, 'dd') >= 7;
