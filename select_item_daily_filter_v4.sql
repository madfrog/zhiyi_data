SELECT  item_id -- 商品ID
        ,shop_id -- 店铺ID
        ,sale_time -- 上新时间
        ,insert_time -- 爬取时间
        ,root_category_id -- 根类目id
        ,category_id -- 类目id
        ,cprice -- 促销价
        ,day_30_sales_volume -- 30天累计销量
        ,day_sales_volume -- 今日销量
        ,add_volume -- 累计销量
        ,add_amount -- 累计销售额
        ,pt
FROM    data_infra_datahub.item_daily_filter_v4
WHERE   pt = MAX_PT('data_infra_datahub.item_daily_filter_v4')
LIMIT 100
;