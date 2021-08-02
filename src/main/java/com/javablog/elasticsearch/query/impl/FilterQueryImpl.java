package com.javablog.elasticsearch.query.impl;

import com.javablog.elasticsearch.query.FilterQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
@Service("filterQuery")
@Slf4j
public class FilterQueryImpl implements FilterQuery {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Override
    public void filterInBoolQuery(String indexName, String typeName) throws IOException {
        SearchHits hits = getSearchHits(indexName);
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
        }
    }

    private SearchHits getSearchHits(String indexName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery("province","湖北省"));
        queryBuilder.filter(QueryBuilders.termQuery("operatorId",1));
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        return hits;
    }

    @Override
    public void rangeQuery(String indexName, String typeName, String fieldName, int from,int to) throws IOException {
        SearchHits hits = getSearchHits(indexName, fieldName, from, to);
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
        }
    }

    private SearchHits getSearchHits(String indexName, String fieldName, int from, int to) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery("province","湖北省"));
        queryBuilder.filter(QueryBuilders.rangeQuery(fieldName).from(from).to(to));
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        return hits;
    }

    @Override
    public void existQuery(String indexName, String typeName, String fieldName) throws IOException {
        SearchHits hits = getSearchHits(indexName, fieldName);
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
        }
    }

    private SearchHits getSearchHits(String indexName, String fieldName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.existsQuery(fieldName));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        return hits;
    }

    @Override
    public void typeQuery(String typeName) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.typeQuery(typeName));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
        }
    }
}
