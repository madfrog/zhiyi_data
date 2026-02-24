CREATE TABLE IF NOT EXISTS item_daily_filter_v4(
	item_id BIGINT COMMENT '商品ID',
	 shop_id BIGINT COMMENT '店铺ID',
	 sale_time DATETIME COMMENT '上新时间',
	 insert_time DATETIME COMMENT '爬取时间',
	 root_category_id BIGINT COMMENT '根类目id',
	 category_id BIGINT COMMENT '类目id',
	 cprice BIGINT COMMENT '促销价',
	 day_30_sales_volume BIGINT COMMENT '30天累计销量',
	 day_sales_volume BIGINT COMMENT '今日销量',
	 day_sales_amount BIGINT COMMENT '今日销售额',
	 shelves BIGINT COMMENT '商品上架状态，0:商品下架、1:商品正常、2:下架但是可以获取数据',
	 shop_type BIGINT COMMENT '店铺类型',
	 add_volume BIGINT COMMENT '累计销量',
	 add_amount BIGINT COMMENT '累计销售额'
) 
PARTITIONED BY (pt STRING) ROW FORMAT SERDE 'com.aliyun.apsara.serde.AliOrcSerDe' 
TBLPROPERTIES ('comment'='过滤后的商品每日销量');