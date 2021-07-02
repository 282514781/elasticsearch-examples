package com.javablog.elasticsearch.query.impl;

import com.google.common.collect.Lists;
import com.javablog.elasticsearch.query.BaseQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * @author hacker
 */
@Service("baseQuery")
@Slf4j
public class BaseQueryImpl implements BaseQuery {

    @Autowired
    RestHighLevelClient restHighLevelClient;
    /**
     * 查询某个字段里含有某个关键词的文档
     * @param indexName   索引名
     * @param fieldName   字段名称
     * @param fieldValue  字段值
     * @return 返回结果列表
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> termQuery(String indexName, String fieldName, String fieldValue, Integer form, Integer size) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery(fieldName, fieldValue));
        //分页
        sourceBuilder.from(form);
        sourceBuilder.size(size);
        searchRequest.source(sourceBuilder);
        log.info("source:" + searchRequest.toString());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 查询某个字段里含有多个关键词的文档
     * @param indexName   索引名
     * @param typeName    TYPE
     * @param fieldName   字段名称
     * @param fieldValues  字段值
     * @return 返回结果列表
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> termsQuery(String indexName, String typeName, String fieldName, String... fieldValues) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termsQuery(fieldName,fieldValues));
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        searchRequest.source(sourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 查询所有文档
     * @param indexName   索引名称
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> queryAll(String indexName, Integer form, Integer size) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQuery());
        searchSourceBuilder.from(form);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("{}",hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("{}",hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * match 搜索
     * @param indexName 索引名称
     * @param typeName  TYPE名称
     * @param field     字段
     * @param keyWord   搜索关键词
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> queryMatch(String indexName, String typeName, String field,String keyWord) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
//        searchRequest.routing();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field,keyWord));
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }
    /**
     * 获取所有index
     */

    @Override
    public List<String> listIndexes() throws Exception{

        try {
            // 构建请求,注意*号的写法
            GetIndexRequest getIndexRequest = new GetIndexRequest("*accesslog*");

            // 构建获取所有索引的请求：org.elasticsearch.client.indices.GetIndexRequest
            GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);

            // 获取所有的索引
            String[] indices = getIndexResponse.getIndices();

            // 转化为list形式
            List<String> asList = Arrays.asList(indices);

            // 复制一下，不然不能追加
            return new ArrayList<>(asList);
        } catch (Exception e) {

            log.error("获取所有索引失败：{}", e);
            throw new Exception(e);
        }
    }
    /**
     * 布尔match查询
     * @param indexName    索引名称
     * @param field        字段名称
     * @param keyWord      关键词
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> queryMatchWithOperate(String indexName, List<String> field,List<String> keyWord) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder orQuery = QueryBuilders.boolQuery();
        if(CollectionUtils.isNotEmpty(field) && CollectionUtils.isNotEmpty(keyWord)&&   field.size() == keyWord.size()) {
            for (int i = 0; i < field.size(); i++) {

                QueryBuilder qb1 = QueryBuilders.matchPhraseQuery(field.get(i), keyWord.get(i));
                orQuery.should(qb1);
            }
        }
        searchSourceBuilder.query(orQuery);
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     *该查询通过字段fields参数作用在多个字段上。
     * @param indexName  索引名称
     * @param typeName   TYPE名称
     * @param keyWord    关键字
     * @param fieldNames  字段
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> queryMulitMatch(String indexName, String typeName,String keyWord,String ...fieldNames) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        List<Map<String,Object>> response = Lists.newArrayList();
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(keyWord,fieldNames));
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 对查询词语分析后构建一个短语查询
     * @param indexName    索引名称
     * @param typeName     TYPE名称
     * @param fieldName    字段名称
     * @param keyWord      关键字
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> queryMatchPhrase(String indexName, String typeName,String fieldName,String keyWord) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchPhraseQuery(fieldName,keyWord));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    @Override
    public List<Map<String,Object>> queryMatchPrefixQuery(String indexName, String typeName,String fieldName,String keyWord) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        List<Map<String,Object>> response = Lists.newArrayList();
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchPhrasePrefixQuery(fieldName,keyWord).maxExpansions(1));
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 查出指定_id的文档
     * @param indexName   索引名称
     * @param typeName    TYPE名称
     * @param ids         _id值
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> idsQuery(String indexName, String typeName,String ... ids) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds(ids));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 查找某字段以某个前缀开头的文档
     * @param indexName 索引名称
     * @param typeName  TYPE名称
     * @param field     字段
     * @param prefix    前缀
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> prefixQuery(String indexName, String typeName, String field, String prefix) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.prefixQuery(field,prefix));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }
    /**
     * 布尔match查询
     * @param indexName    索引名称
     * @param field        字段名称
     * @param keyWord      关键词
     * @param op           该参数取值为or 或 and
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> queryMatchWithOperate(String indexName, String field, String keyWord, Operator op) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field,keyWord).operator(op));
        searchRequest.source(searchSourceBuilder);
        log.info("source:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }
    /**
     * 查找某字段以某个前缀开头的文档
     * @param indexName 索引名称
     * @param typeName  TYPE名称
     * @param field     字段
     * @param value     查询关键字
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> fuzzyQuery(String indexName, String typeName,String field,String value) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.fuzzyQuery(field,value).prefixLength(2));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 以通配符来查询
     * @param indexName     索引名称
     * @param fieldName     字段名称
     * @param wildcard      通配符
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> wildCardQuery(String indexName, String fieldName, String wildcard) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
//        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.wildcardQuery(fieldName, wildcard));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     * 范围查询
     * @param indexName     索引名称
     * @param typeName      TYPE名称
     * @param fieldName     字段名称
     * @param from
     * @param to
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> rangeQuery(String indexName, String typeName, String fieldName, int from,int to) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.rangeQuery(fieldName).from(from).to(to));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    /**
     *正则表达示查询
     * @param indexName     索引名称
     * @param typeName      TYPE名称
     * @param fieldName     字段名称
     * @param regexp        正则表达示
     * @throws IOException
     */
    @Override
    public List<Map<String,Object>> regexpQuery(String indexName, String typeName, String fieldName, String regexp) throws IOException {
        List<Map<String,Object>> response = Lists.newArrayList();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.regexpQuery(fieldName,regexp));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    @Override
    public List<Map<String,Object>> moreLikeThisQuery(String indexName, String typeName, String[] fieldNames, String[] likeTexts) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        List<Map<String,Object>> response = Lists.newArrayList();
        searchRequest.types(typeName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.moreLikeThisQuery(likeTexts).minTermFreq(1));
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse =  restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("count:"+hits.getTotalHits());
        SearchHit[] h =  hits.getHits();
        for (SearchHit hit : h) {
            log.info("结果"+hit.getSourceAsMap());
            response.add(hit.getSourceAsMap());
        }
        return response;
    }

    @Override
    public Boolean scrollQuery(String indexName, String typeName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        //初始化scroll
        //值不需要足够长来处理所有数据—它只需要足够长来处理前一批结果。每个滚动请求(带有滚动参数)设置一个新的过期时间。
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L)); //设定滚动时间间隔
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQuery());
        searchSourceBuilder.size(5); //设定每次返回多少条数据
        searchRequest.source(searchSourceBuilder);
        log.info("string:" + searchRequest.source());
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        log.info("-----首页-----");
        for (SearchHit searchHit : searchHits) {
            log.info(searchHit.getSourceAsString());
        }
        //遍历搜索命中的数据，直到没有数据
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            log.info("string:" + scrollRequest.toString());
            try {
                searchResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                log.info("-----下一页-----");
                for (SearchHit searchHit : searchHits) {
                    log.info(searchHit.getSourceAsString());
                }
            }

        }
        //清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);//也可以选择setScrollIds()将多个scrollId一起使用
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean succeeded = clearScrollResponse.isSucceeded();
        log.info("succeeded:" + succeeded);
        return succeeded;
    }
}
