package capisco.lucene_extractor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class LuceneExtractor
{
	//final static String index = "/home/mjc62/masc_500k/masc_500k_index_capisco/";
	//final static String output = "/home/mjc62/masc_500k/capiscoOutput/";
	//final static String namefield = "path"; //path for Capisco
	//final static String valuefield = "topics"; //value for Capisco
	
	final static String index = "/home/mjc62/masc_500k/masc_500k_index_solr/";
	final static String output = "/home/mjc62/masc_500k/solrOutput/";
	final static String namefield = "resourcename"; //resourcename for Solr
	final static String valuefield = "_text_"; //value for Solr
	
	public static void main(String args[]) throws Exception 
	{
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(index).toPath()));
		IndexSearcher searcher = new IndexSearcher(reader);
		searching(searcher);
	}   
    
	public static void searching(IndexSearcher searcher) throws ParseException, IOException
	{	
		ArrayList<String> docNames = new ArrayList<String>();
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(index).toPath()));
		for (int i=0; i<reader.maxDoc(); i++) {
		    Document doc = reader.document(i);
		    docNames.add(doc.get(namefield));
		}
		
		for(String docName : docNames){	
			Term term = new Term(namefield, docName);
			TermQuery tq = new TermQuery(term);		
			TopDocs results = searcher.search(tq, 1000);
			ScoreDoc[] hits = results.scoreDocs;
			String docTitle = docName.substring(docName.lastIndexOf("/")+1);
			System.out.println(docTitle);
			
			File docDir = new File(output);
			if (!docDir.exists()){
				docDir.mkdir();
			}
			
			File writename = new File(output + docTitle); 
        	
        	BufferedWriter out = new BufferedWriter(new FileWriter(writename,true));
			for (int i = 0; i < hits.length; ++i) 
			{
				Document doc = searcher.doc(hits[i].doc);
				String[] resultConcepts = doc.getValues(valuefield);				 	
				for(String concept : resultConcepts){					 
            	out.write(concept + "\r\n");
				}							
			}
			out.close();
		}
	}
}
