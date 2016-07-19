package org.apache.solr.handler;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ExtendedDismaxQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class RedbubbleMoreLikeThisHandler extends RequestHandlerBase {

  private String DOC_ID_PARAM = "id";
  
  private String START_PARAM = "start";
  
  private String ROWS_PARAM = "rows";
  
  private String INTERESTING_TERMS_PARAM = "it";
  
  private int START_PARAM_DEFAULT = 0;
  
  private int ROWS_PARAM_DEFAULT = 10;
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    SolrParams params = req.getParams();
    Integer originalDocId = params.getInt(DOC_ID_PARAM);
    String[] similarityFields =  params.getParams(MoreLikeThisParams.SIMILARITY_FIELDS);
        
    if( similarityFields == null || similarityFields.length < 1 ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, 
          "MoreLikeThis requires at least one similarity field: "+MoreLikeThisParams.SIMILARITY_FIELDS );
    }
    
    if(originalDocId == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "id parameter missing!");
    }
    
    SolrIndexSearcher searcher = req.getSearcher();
    MoreLikeThis mlt = new MoreLikeThis(searcher.getIndexReader());
    setMLTparams(params, similarityFields, mlt);
    
    Query originalDocQuery = createOriginalDocQuery(originalDocId, req);
    int matchLuceneDocId = getOriginalDocLuceneDocId(originalDocQuery, searcher);
    
    //TODO: cache interesting terms per doc?
    String[] interestingTerms = mlt.retrieveInterestingTerms(matchLuceneDocId);
    
    Query edismaxQuery = createEdismaxQuery(interestingTerms, params, req);
    
    DocList results = executeEdismaxQuery(edismaxQuery, params, searcher);
    
    boolean returnInterestingTerms = params.getBool(INTERESTING_TERMS_PARAM, false);
    
    if (returnInterestingTerms) {
      rsp.add("interestingTerms", interestingTerms);
    }
    
    rsp.add("response", results);
  }

  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public String getSource() {
    return null;
  }
  
  private void setMLTparams(SolrParams params, String[] similarityFields, MoreLikeThis mlt) {
    mlt.setMinTermFreq(       params.getInt(MoreLikeThisParams.MIN_TERM_FREQ,         MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
    mlt.setMinDocFreq(        params.getInt(MoreLikeThisParams.MIN_DOC_FREQ,          MoreLikeThis.DEFAULT_MIN_DOC_FREQ));
    mlt.setMaxDocFreq(        params.getInt(MoreLikeThisParams.MAX_DOC_FREQ,          MoreLikeThis.DEFAULT_MAX_DOC_FREQ));
    mlt.setMinWordLen(        params.getInt(MoreLikeThisParams.MIN_WORD_LEN,          MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
    mlt.setMaxWordLen(        params.getInt(MoreLikeThisParams.MAX_WORD_LEN,          MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
    mlt.setMaxQueryTerms(     params.getInt(MoreLikeThisParams.MAX_QUERY_TERMS,       MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
    mlt.setMaxNumTokensParsed(params.getInt(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED, MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
    mlt.setBoost(            params.getBool(MoreLikeThisParams.BOOST, false ) );
    mlt.setFieldNames(similarityFields);
  }
  
  private Query createOriginalDocQuery(int matchId, SolrQueryRequest req) {
    QParser parser;
    try {
      parser = QParser.getParser("id:" + matchId, QParserPlugin.DEFAULT_QTYPE, req);
      return parser.getQuery();
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error while building match query", e);
    }
  }
  
  private int getOriginalDocLuceneDocId(Query originalDocQuery, SolrIndexSearcher searcher) {
    try {
      DocList match = searcher.getDocList(originalDocQuery, null, null, 0, 1, 0);
      DocIterator iterator = match.iterator();
      if (iterator.hasNext()) {
        return iterator.nextDoc();
      } else {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Original doc query found no documents.");
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error while running original doc query", e);
    }
  }
  
  private Query createEdismaxQuery(String[] interestingTerms, SolrParams params, SolrQueryRequest req) {
    ExtendedDismaxQParser edismaxParser = new ExtendedDismaxQParser(StringUtils.join(interestingTerms, " OR "), params, null, req);
    try {
      return edismaxParser.parse();
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error while creating edismax query", e);
    }
  }
  
  private DocList executeEdismaxQuery(Query query, SolrParams params, SolrIndexSearcher searcher) {
    int start = params.getInt(START_PARAM, START_PARAM_DEFAULT);
    int rows = params.getInt(ROWS_PARAM, ROWS_PARAM_DEFAULT);
    
    try {
      return searcher.getDocList(query, new ArrayList<Query>(), null, start, rows, 0);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error while executing edismax query", e);
    }
  }
}
