CREATE TABLE IF NOT EXISTS ods_taobao_comment_content_info(
	item_id BIGINT COMMENT '商品id',
	 seller_id BIGINT COMMENT '卖家id',
	 comment_id BIGINT COMMENT '评论id',
	 comment_content STRING COMMENT '评论内容',
	 comment_seller_name STRING COMMENT '买家匿名名字',
	 comment_images STRING COMMENT '评论图片',
	 comment_sku STRING COMMENT '评论sku',
	 comment_time DATETIME COMMENT '评论时间',
	 `version` STRING COMMENT '版本号') ROW FORMAT SERDE 'com.aliyun.apsara.serde.AliOrcSerDe' 
TBLPROPERTIES ('comment'='淘宝商品评论内容');