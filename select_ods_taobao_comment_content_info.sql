SELECT  item_id -- 商品id
        ,seller_id -- 卖家id
        ,comment_id -- 评论id
        ,comment_content -- 评论内容
        ,comment_images -- 评论图片
        ,comment_sku -- 评论sku
        ,comment_time -- 评论时间
FROM    data_infra_datahub.ods_taobao_comment_content_info
where comment_images 
;
