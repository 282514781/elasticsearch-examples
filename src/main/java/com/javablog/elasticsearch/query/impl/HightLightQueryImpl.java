package com.javablog.elasticsearch.query.impl;

import com.javablog.elasticsearch.query.HighLightQuery;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

/**
 * @author hacker
 */
@Service("hightLightQuery")
@Slf4j
public class HightLightQueryImpl implements HighLightQuery{

   @Autowired
   RestHighLevelClient restHighLevelClient;
    @Override
    public void hightLightQuery(String indexName, String type,String field,String keyword) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //条件
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(field,keyword);
        // 高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field(field).
                preTags("<b><font color=red>").postTags("</font></b>");
        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.query( queryBuilder);
        searchRequest.source(searchSourceBuilder);
        log.info("source string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            //得到高亮显示的集合
            Map<String, HighlightField> map = hit.getHighlightFields();
            HighlightField highlightField =  map.get(field);
            // log.info("高"+map);
            if (highlightField!=null){
//                log.info(highlightField.getName());
                Text[] texts =  highlightField.getFragments();
                log.info(texts[0]+"<p>");
            }
//            log.info("普通字段结果"+hit.getSourceAsMap());
        }
    }

    @Override
    public void hightLightQueryByFragment(String indexName, String type, int fragmentSize) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //条件
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder("smsContent","企业");
        // 高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("smsContent").
                preTags("<b><em style='color:red;'>").postTags("</em></b>");
        highlightBuilder.fragmentSize(fragmentSize);
        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.query( queryBuilder);
        searchRequest.source(searchSourceBuilder);
        log.info("source string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            //得到高亮显示的集合
            Map<String, HighlightField> map = hit.getHighlightFields();
            HighlightField highlightField =  map.get("smsContent");
            if (highlightField!=null){
                Text[] fragments3 = highlightField.getFragments();
                for (Text text : fragments3) {
                    log.info("result:"+text);
                }
            }
        }
    }

    @Override
    public void hightLightQueryByNumOfFragments(String indexName, String type, int fragmentSize,int numOfFragments) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //条件
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder("smsContent","企业");
        // 高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("smsContent").
                preTags("<b><em style='color:red;'>").postTags("</em></b>");
        highlightBuilder.fragmentSize(fragmentSize);
        highlightBuilder.numOfFragments(numOfFragments);
        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.query( queryBuilder);
        searchRequest.source(searchSourceBuilder);
        log.info("source string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            //得到高亮显示的集合
            Map<String, HighlightField> map = hit.getHighlightFields();
            HighlightField highlightField =  map.get("smsContent");
            if (highlightField!=null){
                Text[] fragments3 = highlightField.getFragments();
                for (Text text : fragments3) {
                    log.info("result:"+text);
                }
            }
        }
    }

    @Override
    public void hightLightNoMatchSize(String indexName, String type, int fragmentSize,int numOfFragments,int noMatchSize) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //条件
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        MatchQueryBuilder querySmsContentBuilder = new MatchQueryBuilder("smsContent","企业");
        MatchQueryBuilder queryCorpNameBuilder = new MatchQueryBuilder("corpName","企业");
        builder.should(querySmsContentBuilder);
        builder.should(queryCorpNameBuilder);
        // 高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("smsContent").field("corpName").
                preTags("<b><em style='color:red;'>").postTags("</em></b>");
        highlightBuilder.fragmentSize(fragmentSize);
        highlightBuilder.numOfFragments(numOfFragments);
        highlightBuilder.noMatchSize(noMatchSize);
        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.query(builder);
        searchRequest.source(searchSourceBuilder);
        log.info("source string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            //得到高亮显示的集合
            Map<String, HighlightField> map = hit.getHighlightFields();
            HighlightField highlightField1 = map.get("corpName");
            if(highlightField1!=null) {
                Text[] fragments1 = highlightField1.getFragments();
                for (Text text : fragments1) {
                    log.info("1:" + text);
                }
            }

            HighlightField highlightField2 =  map.get("smsContent");
            if (highlightField2!=null){
                Text[] fragments3 = highlightField2.getFragments();
                for (Text text : fragments3) {
                    log.info("2:"+text);
                }
            }
        }
    }
}
