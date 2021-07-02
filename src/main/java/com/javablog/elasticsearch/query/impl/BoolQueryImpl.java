package com.javablog.elasticsearch.query.impl;

import com.javablog.elasticsearch.query.BoolQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author hacker
 */
@Service("boolQuery")
@Slf4j
public class BoolQueryImpl implements BoolQuery {

    @Autowired
    RestHighLevelClient restHighLevelClient;
    /**
     * bool组俣查询
     * @param indexName   索引名称
     * @param typeName    TYPE名称
     * @throws IOException
     */
    @Override
    public void boolQuery(String indexName, String typeName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.should(QueryBuilders.termQuery("province","湖北省"));
        queryBuilder.should(QueryBuilders.termQuery("province","北京"));
        queryBuilder.must(QueryBuilders.matchQuery("smsContent","中国"));
        //运营商不是联通的手机号
        queryBuilder.mustNot(QueryBuilders.termQuery("operatorId",2));
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
        }
    }

    /**
     *
     * @param indexName
     * @param typeName
     * @throws IOException
     */
    @Override
    public void boostingQuery(String indexName, String typeName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryPositiveBuilder = QueryBuilders.matchQuery("smsContent", "苹果");
        MatchQueryBuilder matchQueryNegativeBuilder = QueryBuilders.matchQuery("smsContent", "水果 乔木 维生素");//
        BoostingQueryBuilder boostingQueryBuilder = QueryBuilders.boostingQuery(matchQueryPositiveBuilder,
                matchQueryNegativeBuilder).negativeBoost(0.1f);
        searchSourceBuilder.query(boostingQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info(searchRequest.source().toString());
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("score:"+hit.getScore());
            log.info("结果"+hit.getSourceAsMap());
        }
    }
}
