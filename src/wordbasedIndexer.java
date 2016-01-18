/**This version is used to generate a plain lucene index of text file (no connection to capisco), 
 * then, use progress in capisco.reverse to export txt version. Actually, only this java file is necessary for vanilla lucene index generation
*/


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.BreakIterator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

//import capisco.executors.TaskExecutor;

public class wordbasedIndexer
{
	File root;
	//TaskExecutor<IndexTask> executor;
	IndexWriter writer;
	//String path = "/home/yg115/wordbased1";
	
	public wordbasedIndexer(File root, File index) throws Exception
	{
		//executor = new TaskExecutor<IndexTask>(new IndexWorkerFactory(server, port, database), workers, workers);
		this.root = root;
		writer = createIndex(index);
	}

	public static String fromStream(InputStream in) throws IOException
	{
	    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	    StringBuilder out = new StringBuilder();
	    String newLine = System.getProperty("line.separator");
	    String line;
	    while ((line = reader.readLine()) != null) {
	        out.append(line);
	        out.append(newLine);
	    }
	    return out.toString();
	}
	
	public void run()
	{
		try
		{
			//PrintWriter nw = new PrintWriter("names.txt", "UTF-8");		
			for (File sub : FileUtils.listFiles(root, null, true))
			{
				if(sub.getName().endsWith(".zip")) //zipfile handler
				{
					ZipFile zf = new ZipFile(sub);
					String name = null;
					//Vector<InputStream> inputStreams = new Vector<InputStream>();
					for (Enumeration<? extends ZipEntry> e = zf.entries(); e.hasMoreElements();) {//zip entries havent been closed, the same as the stream. hard to decide where to close them 
						ZipEntry ze = e.nextElement();
						long size = ze.getSize();

						if (size > 0) {
							//name = sub.getName() + "." + path.getUnescapedName(); //the name has some problems, which leads unable to get text name, but the upper path instead
							name = ze.getName();
							String tit = name.substring(0,name.lastIndexOf("/"));
							String pageNum = name.substring(name.lastIndexOf("/") + 1, name.length()-4);
							
							String wholeContent = fromStream(zf.getInputStream(ze));
							BreakIterator wordIterator = BreakIterator.getWordInstance(); 
							wordIterator.setText(wholeContent);									
							printEachForward(wordIterator, wholeContent, tit, pageNum);
											
						}
					}
			
				}							
			}
																	      //into buffer. Then, transfer the buffer to Capisco	
			
			System.out.println("All files queued for processing, awaiting termination");
			//executor.shutdown();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void printEachForward(BreakIterator boundary, String source, String path, String pageNum)throws IOException {
	     
		HashMap<String,Integer> wordsAndcounts = new HashMap<String,Integer>();      	
    	//BufferedWriter out = new BufferedWriter(new FileWriter(writename,true));
    		
		
		int start = boundary.first();
	     for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
	    	 if(!wordsAndcounts.containsKey(source.substring(start,end)))
	    		 wordsAndcounts.put(source.substring(start,end), new Integer(1));
	    	 else{
	    		 Integer newCountNum = new Integer(wordsAndcounts.get(source.substring(start,end)).intValue() + 1);
	    		 wordsAndcounts.remove(source.substring(start,end));
	    		 wordsAndcounts.put(source.substring(start,end), newCountNum);
	    	 }
	    	  //
	     }
	     this.save(wordsAndcounts, path, pageNum);
//	     out.close();
	 }
	
	public void close() throws IOException
	{
		System.out.println("Closing lucene index writer");
		writer.commit();
		System.out.println("writer is closed");
	}
	
	protected void save(HashMap<String,Integer> wordsAndcounts, String path, String pageNum)   //create lucene index here
	{		
		Document doc = new Document();	
		Iterator it = wordsAndcounts.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        doc.add(new StringField("path", path, Field.Store.YES));
	        doc.add(new StringField("pageNum", pageNum, Field.Store.YES));
	        doc.add(new StringField("topics", (String)pair.getKey(), Field.Store.YES));
	        doc.add(new StringField("count", pair.getValue().toString(), Field.Store.YES));
	        it.remove(); // avoids a ConcurrentModificationException
	    }	
		try
		{
			writer.addDocument(doc);
		}
		catch (IOException e)
		{
			String message = "Failed to add " + path + " to index";
			throw new RuntimeException(message, e);
		}
	}
	
	static IndexWriter createIndex(File location) throws IOException
	{
		try
		{
			FSDirectory dir = FSDirectory.open(location);
			Analyzer analyzer = new EnglishAnalyzer();
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_1, analyzer);
			config.setOpenMode(IndexWriterConfig.OpenMode.valueOf("CREATE_OR_APPEND"));
			return new IndexWriter(dir, config);
		}
		catch (IOException e)
		{
			System.err.format("Could not create index writer (%1$s)", e.getMessage());
			throw e;
		}
	}
	
	public static void main(String[] args)
	{
		// arg[0] = input documents directory path
		// arg[1] = where to create/append to the index

		
		if (args.length < 2)
		{
			System.out.println("Use: WMindexer <input documents path> <index path> ");
			return;
		}
		
		File documents = new File(args[0]);
		File index = new File(args[1]);
		
		try
		{
			wordbasedIndexer indexer = new wordbasedIndexer(documents, index);
			indexer.run();
			indexer.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
