//package com.javablog.elasticsearch.query.impl;//package cn.fancylab.monitor.untrans.query.impl;
//
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.common.geo.GeoPoint;
//import org.elasticsearch.common.unit.DistanceUnit;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.aggregations.*;
//import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
//import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
//import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
//import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
//import org.elasticsearch.search.aggregations.bucket.range.Range;
//import org.elasticsearch.search.aggregations.bucket.terms.Terms;
//import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
//import org.elasticsearch.search.aggregations.metrics.avg.Avg;
//import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
//import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
//import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
//import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
//import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.InitializingBean;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//
//import org.elasticsearch.search.aggregations.bucket.filter.Filter;
//
//@Service("aggregationQuery")
//public class AggregationQueryImpl implements AggregationQuery, InitializingBean {
//    private final static Logger log = LoggerFactory.getLogger(SortQueryImpl.class);
//    private RestHighLevelClient restHighLevelClient;
//    //private String hosts = ;
//
//    @Override
//    public void afterPropertiesSet() {
//        restHighLevelClient = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("122.9.66.81", 9200, "http"),
//                        new HttpHost("116.63.104.40", 9200, "http"),
//                        new HttpHost("116.63.39.126", 9200, "http"),
//                        new HttpHost("122.9.66.249", 9200, "http"),
//                        new HttpHost("116.63.49.205", 9200, "http")));
//    }
//
//    @Override
//    public  void AvgAggregations(String indexName, String typeName, String field) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        AvgAggregationBuilder agg1 = AggregationBuilders.avg("agg").field(field);
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        log.info("cource:" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Avg agg = searchResponse.getAggregations().get("agg");
//        log.info(field + " avg ：" + agg.getValue());
//    }
//
//    @Override
//    public void termsAggregation(String indexName, String typeName,long startTime, long endTime) throws IOException {
//        HashMap<String, Integer> apiAvg = new HashMap<String, Integer>();
//        SearchRequest searchRequest = new SearchRequest();
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.rangeQuery("createDate").from(startTime).to(endTime));
//        TermsAggregationBuilder aggregation = AggregationBuilders.terms("province")
//                .field("province");   //text类型不能用于索引或排序，必须转成keyword类型
//        aggregation.subAggregation(AggregationBuilders.avg("avg_request_fee")
//                .field("fee"));  //avg_age 为子聚合名称，名称可随意
//        searchSourceBuilder.aggregation(aggregation);
//        searchRequest.source(searchSourceBuilder);
//        SearchResponse searchResponse = null;
//        log.info("cource:" + searchRequest.source());
//        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        SearchHits hits = searchResponse.getHits();
//        Aggregations aggregations = searchResponse.getAggregations();
//        Terms byCompanyAggregation = aggregations.get("province");
//        List<? extends Terms.Bucket> buckets = byCompanyAggregation.getBuckets();
//        for (int i = 0; i < buckets.size(); i++) {
//            Terms.Bucket elasticBucket = buckets.get(i);
//            Object key = elasticBucket.getKey();
//            Avg averageAge = elasticBucket.getAggregations().get("avg_request_fee");
//            Double avg = averageAge.getValue();
//            int intAvg = avg.intValue();
//            apiAvg.put(key.toString(), intAvg);
//        }
//        log.info(apiAvg.toString());
//    }
//
//    @Override
//    public  void cardinalityAggregations(String indexName, String typeName, String field) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        CardinalityAggregationBuilder agg1 = AggregationBuilders.cardinality("agg").field(field);
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        log.info("cource:" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Cardinality agg = searchResponse.getAggregations().get("agg");
//        double value = agg.getValue();
//        log.info(field + " cardinalityAggregation value ：" + value);
//    }
//
//    @Override
//    public  void dateHistogramAggregation(String indexName, String typeName, String field) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        DateHistogramAggregationBuilder agg1 = AggregationBuilders.dateHistogram("agg").field(field).dateHistogramInterval(DateHistogramInterval.DAY);
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Histogram agg = searchResponse.getAggregations().get("agg");
//        for (Histogram.Bucket entry : agg.getBuckets()) {
//            DateTime key = (DateTime) entry.getKey();    // Key
//            String keyAsString = entry.getKeyAsString(); // Key as String
//            long docCount = entry.getDocCount();         // Doc count
//            log.info("key:" + keyAsString + " date:" + key.getYear() + ", doc_count " + docCount);
//        }
//    }
//
//    @Override
//    public  void dateRangeAggregation(String indexName, String typeName, String field) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        AggregationBuilder agg1 = AggregationBuilders.dateRange("agg").field(field).format("yyyy").
//                addUnboundedTo("2011").
//                addRange("2011","2019")
//                .addUnboundedFrom("2019");
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        log.info("source:" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Range agg = searchResponse.getAggregations().get("agg");
//        for (Range.Bucket entry : agg.getBuckets()) {
//            String key = entry.getKeyAsString();                // Date range as key
//            DateTime fromAsDate = (DateTime) entry.getFrom();   // Date bucket from as a Date
//            DateTime toAsDate = (DateTime) entry.getTo();       // Date bucket to as a Date
//            long docCount = entry.getDocCount();                // Doc count
//
//            log.info("key:"+key+" from:"+fromAsDate+" to:"+toAsDate+" doc_count:" +docCount);
//        }
//    }
//
//    @Override
//    public  void extendedStatsAggregation(String indexName, String typeName, String field) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        ExtendedStatsAggregationBuilder agg1 = AggregationBuilders.extendedStats("agg").field(field);
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        log.info("source :" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        ExtendedStats agg = searchResponse.getAggregations().get("agg");
//        double min = agg.getMin();
//        double max = agg.getMax();
//        double avg = agg.getAvg();
//        double sum = agg.getSum();
//        long count = agg.getCount();
//        double stdDeviation = agg.getStdDeviation();
//        double sumOfSquares = agg.getSumOfSquares();
//        double variance = agg.getVariance();
//        log.info("min ：" + min);
//        log.info("max ：" + max);
//        log.info("avg ：" + avg);
//        log.info("sum ：" + sum);
//        log.info("count ：" + count);
//        log.info("stdDeviation ：" + stdDeviation);
//        log.info("sumOfSquares ：" + sumOfSquares);
//        log.info("variance ：" + variance);
//    }
//
//    @Override
//    public void filterAggregation(String indexName, String typeName, String termField, String termValue) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        AbstractAggregationBuilder agg1 = AggregationBuilders.filter("agg",
//                QueryBuilders.termQuery(termField, termValue)).subAggregation(AggregationBuilders.avg("my_agg").field("replyTotal"));
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Filter agg = searchResponse.getAggregations().get("agg");
//        log.info("searchResponse:"+searchResponse);
//    }
//
//    /**
//     * histogram 统计能够对字段取值按间隔统计建立直方图
//     * @param indexName   索引名称
//     * @param typeName    TYPE名称
//     * @param field       字段名称
//     * @param interval     间段值
//     * @throws IOException
//     */
//    public  void histogramAggregation(String indexName, String typeName, String field,int interval) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        AbstractAggregationBuilder agg1 = AggregationBuilders.histogram("agg").field(field).interval(interval);
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        log.info("source:" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Histogram agg = searchResponse.getAggregations().get("agg");
//        for (Histogram.Bucket entry : agg.getBuckets()) {
//            double key = (double) entry.getKey();       // Key
//            double docCount = entry.getDocCount();    // Doc count
//            log.info("key:" + key + ", doc_count " + docCount);
//        }
//    }
//
//    /**
//     * histogram 统计能够对字段取值按间隔统计建立直方图
//     * @param indexName   索引名称
//     * @param typeName    TYPE名称
//     * @param field       字段名称
//     * @param interval     间段值
//     * @throws IOException
//     */
//    public  void histogramDateAggregation(String indexName, String typeName, String field,int interval) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        AggregationBuilder agg1 = AggregationBuilders.dateHistogram("agg").field(field).dateHistogramInterval(DateHistogramInterval.DAY).interval(interval);
//        sourceBuilder.aggregation(agg1);
//        searchRequest.source(sourceBuilder);
//        log.info("source:" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Histogram agg = searchResponse.getAggregations().get("agg");
//        for (Histogram.Bucket entry : agg.getBuckets()) {
//            DateTime key = (DateTime) entry.getKey();       // Key
//            double docCount = entry.getDocCount();    // Doc count
//            log.info("key:" + key + ", doc_count " + docCount);
//        }
//    }
//
//    public  void geoDistanceAggregation(String indexName, String typeName) throws IOException {
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        GeoDistanceAggregationBuilder geoDistanceAggregationBuilder = AggregationBuilders.geoDistance("agg", new GeoPoint(24.46667, 118.10000))
//                .field("location")
//                .unit(DistanceUnit.KILOMETERS)
//                .addUnboundedTo(100)
//                .addRange(100, 500)
//                .addRange(500, 5000);
//        sourceBuilder.aggregation(geoDistanceAggregationBuilder);
//        searchRequest.source(sourceBuilder);
//        log.info("source:" + searchRequest.source());
//        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        Range agg = searchResponse.getAggregations().get("agg");
//        for (Range.Bucket entry : agg.getBuckets()) {
//            String key = entry.getKeyAsString();                // Date range as key
//            Double from = (Double) entry.getFrom();   // Date bucket from as a Date
//            Double to = (Double) entry.getTo();       // Date bucket to as a Date
//            long docCount = entry.getDocCount();                // Doc count
//            log.info("key:" + key + " from:"+ from +" to:" + to + " doc_count:" +docCount);
//        }
//    }
//}
