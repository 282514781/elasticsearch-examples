package com.javablog.elasticsearch.query.impl;

import com.javablog.elasticsearch.query.SortQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service("sortQuery")
@Slf4j
public class SortQueryImpl implements SortQuery {

    @Autowired
    RestHighLevelClient restHighLevelClient;
    @Override
    public void queryMatch(String indexName, String typeName, String field,String keyWord) throws IOException {
        SearchHit[] h = getSearchHits(indexName, field, keyWord);
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap() +",score:"+ hit.getScore());
        }
    }

    private SearchHit[] getSearchHits(String indexName, String field, String keyWord) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field,keyWord));
        searchSourceBuilder.sort("replyTotal");
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        return hits.getHits();
    }

    @Override
    public void sortQuery(String indexName, String typeName, String field, String keyWord, String sort, SortOrder sortOrder) throws IOException {
        SearchHits hits = getSearchHits(indexName, field, keyWord, sort, sortOrder);
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap() +",score:"+ hit.getScore());
        }
    }

    private SearchHits getSearchHits(String indexName, String field, String keyWord, String sort, SortOrder sortOrder) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field,keyWord));
        searchSourceBuilder.sort(sort, sortOrder);
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        return hits;
    }

    @Override
    public void multSortQuery(String indexName, String typeName, String field, String keyWord, String sort1, String sort2, SortOrder sortOrder) throws IOException {
        SearchHits hits = getSearchHits(indexName, field, keyWord, sort1, sort2, sortOrder);
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap() +",score:"+ hit.getScore());
        }
    }

    private SearchHits getSearchHits(String indexName, String field, String keyWord, String sort1, String sort2, SortOrder sortOrder) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field,keyWord));
        searchSourceBuilder.sort(sort1, sortOrder);
        searchSourceBuilder.sort(sort2, sortOrder);
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        return hits;
    }
}
