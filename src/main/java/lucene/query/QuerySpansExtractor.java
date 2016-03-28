package lucene.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author Mark
 */
public class QuerySpansExtractor
{
    public static Spans[] getSpans(Query query, IndexReader reader) throws IOException
    {
        ArrayList spans=new ArrayList();
        getSpans(query,reader,spans);
        return (Spans[]) spans.toArray(new Spans[spans.size()]);
    }
    
    private static final void getSpans(Query query, IndexReader reader,
            ArrayList spans) throws IOException
    {
        if (query instanceof BooleanQuery)
            getSpansFromBooleanQuery((BooleanQuery) query, reader,spans);
        else if (query instanceof PhraseQuery)
            getSpansFromPhraseQuery((PhraseQuery) query, reader,spans);
        else if (query instanceof TermQuery)
            getSpansFromTermQuery((TermQuery) query, reader,spans);
        else if (query instanceof SpanQuery)
            getSpansFromSpanQuery((SpanQuery) query, reader,spans);
    }

    private static final void getSpansFromBooleanQuery(BooleanQuery query,IndexReader reader,
            ArrayList spans) throws IOException
    {
        BooleanClause[] queryClauses = query.getClauses();
        int i;

        for (i = 0; i < queryClauses.length; i++)
        {
            if (!queryClauses[i].isProhibited())
                getSpans(queryClauses[i].getQuery(), reader,spans);
        }
    }

    private static final void getSpansFromPhraseQuery(PhraseQuery query,IndexReader reader,
            ArrayList spans) throws IOException
    {
    	//System.out.println("Phrase query");
        Term[] queryTerms = query.getTerms();
        int i;
        SpanQuery []clauses=new SpanQuery[queryTerms.length];

        for (i = 0; i < queryTerms.length; i++)
        {
            clauses[i]=new SpanTermQuery(queryTerms[i]);
        }
        SpanNearQuery sp = new SpanNearQuery(clauses,query.getSlop(),false);
        sp.setBoost(query.getBoost());        
        spans.add(sp.getSpans(reader));
    }

    private static final void getSpansFromTermQuery(TermQuery query,IndexReader reader,
            ArrayList spans) throws IOException
    {
        SpanTermQuery stq = new SpanTermQuery(query.getTerm());
        stq.setBoost(query.getBoost());        
        spans.add(stq.getSpans(reader));
    }

    private static final void getSpansFromSpanQuery(SpanQuery query,IndexReader reader,
            ArrayList spans) throws IOException
    {
        spans.add(query.getSpans(reader));
    }}
