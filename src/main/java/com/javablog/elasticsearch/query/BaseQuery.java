package com.javablog.elasticsearch.query;


import org.elasticsearch.index.query.Operator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface BaseQuery {
    public List<Map<String, Object>> termQuery(String indexName, String fieldName, String fieldValue, Integer form, Integer size) throws IOException;
    public List<Map<String,Object>> termsQuery(String indexName, String typeName, String fieldName, String... fieldValues) throws IOException;
    public List<Map<String,Object>> queryAll(String indexName, Integer form, Integer size) throws IOException ;
    public List<Map<String,Object>> queryMatch(String indexName, String typeName, String field, String keyWord) throws IOException ;

    List<String> listIndexes() throws Exception;

    public List<Map<String,Object>> queryMatchWithOperate(String indexName, List<String> field, List<String> keyWord) throws IOException;
    public List<Map<String,Object>> queryMulitMatch(String indexName, String typeName, String keyWord, String... fieldNames) throws IOException;
    public List<Map<String,Object>> queryMatchPhrase(String indexName, String typeName, String fieldName, String keyWord) throws IOException ;
    public List<Map<String,Object>> queryMatchPrefixQuery(String indexName, String typeName, String fieldName, String keyWord) throws IOException;
    public List<Map<String,Object>> idsQuery(String indexName, String typeName, String... ids) throws IOException ;
    public List<Map<String,Object>> prefixQuery(String indexName, String typeName, String field, String prefix) throws IOException ;

    List<Map<String,Object>> queryMatchWithOperate(String indexName, String field, String keyWord, Operator op) throws IOException;

    public List<Map<String,Object>> fuzzyQuery(String indexName, String typeName, String field, String value) throws IOException ;
    public List<Map<String,Object>> wildCardQuery(String indexName, String fieldName, String wildcard) throws IOException;
    public List<Map<String,Object>> rangeQuery(String indexName, String typeName, String fieldName, int from, int to) throws IOException ;
    public List<Map<String,Object>> regexpQuery(String indexName, String typeName, String fieldName, String regexp) throws IOException;
    public List<Map<String,Object>> moreLikeThisQuery(String indexName, String typeName, String[] fieldNames, String[] likeTexts) throws IOException;
    public Boolean scrollQuery(String indexName, String typeName) throws IOException ;
}
