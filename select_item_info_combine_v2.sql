SELECT  item_id -- 商品id
        ,title -- 商品名
        ,pic_url -- 主图url
        ,pic_url_list -- 轮播图url
        ,category_id -- 品类id
        ,category_name -- 品类名
        ,shop_id -- 店铺id
        ,sale_time -- 上新时间
        ,cprice -- 促销价
        ,collect -- 商品收藏量
        ,comment_count -- 商品评论数
        ,first_day_sales_volume -- 首日销量
        ,first_day_sale -- 首日销售额
        ,first_week_sales_volume -- 首周销量
        ,first_week_sale -- 首周销售额
        ,first_month_sales_volume -- 首月销量
        ,first_month_sale -- 首月销售额
        ,first_day_collect -- 首日搜藏量
        ,sale_day -- 当日销量（昨日）
        ,sale_7day -- 近7日销量
        ,sale_30day -- 近30天销量
        ,total_sale_volume -- 总销量
        ,total_sale_amount -- 总销售额
        ,first_insert_time -- 商品首次插入时间
        ,video_url -- 视频链接
        ,bad_rate -- 目前差评率
        ,properties -- 属性信息
        ,bad_comment_count -- 差评数
        ,start_sell_volume -- 开卖当天销量
        ,start_sell_date -- 开卖当天日期
        ,start_sell_amount -- 开卖首日销售额
FROM    data_infra_datahub.item_info_combine_v2
;
