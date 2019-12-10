package com.sitech.gmall.common;

public class GmallConstants {
    public static final String KAFKA_TOPIC_STARTUP = "gmall_startup";
    public static final String KAFKA_TOPIC_EVENT = "gmall_event";
    public static final String KAFKA_TOPIC_NEW_ORDER = "gmall_new_order";
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "gmall_order_detail";

    public static final String ES_INDEX_DAU = "gmall_dau";
    public static final String ES_INDEX_NEW_MID = "gmall_new_mid";
    public static final String ES_INDEX_NEW_ORDER = "gmall_new_order";
    public static final String ES_INDEX_SALE_DETAIL = "gmall_sale_detail";

    public static final String REDIS_DAU_HOUR_KEY = "gmall_dau_hour";
    public static final String REDIS_NEW_ORDER_KEY = "gmall_new_order";
    public static final String REDIS_ORDER_DETAIL_KEY = "gmall_order_detail";

}
