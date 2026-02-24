SELECT  shop_id -- 店铺ID
        ,seller_id -- 卖家ID
        ,rank -- 店铺等级（几个图标，1-6）
        ,rank_type -- 店铺等级类型:4-金冠;3-皇冠;2-钻石;1-红心
        ,main_industry -- 爬虫主营行业，会废弃
        ,good_comment_rate -- 好评率
        ,status -- 店铺状态（0-无效，1-有效）
        ,label_industry -- 人工标注的行业信息
        ,label_type -- 人工标注的店铺类型
        ,label_style -- 人工标注的店铺风格
        ,total_sales_volume -- 店铺总销售量
        ,total_sale -- 店铺总销售额
        ,total_item_amount -- 店铺总商品数
        ,fans_num_no_unit -- 去除单位的店铺粉丝
FROM    data_infra_datahub.dws_shop_info_combine_v8
LIMIT   100
;