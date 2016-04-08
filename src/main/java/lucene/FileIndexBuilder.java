/** Build a file-based Lucene inverted index.
 * 
 * @author Scott Sanner
 */

package lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import lucene.query.PorterStemAnalyzer;
import util.ConfigRead;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileIndexBuilder {

	public Analyzer  _analyzer; 
	public String    _indexPath;
	
	public FileIndexBuilder(String index_path) {
		
	    // Specify the analyzer for tokenizing text.
	    // The same analyzer should be used for indexing and searching
		_analyzer = new PorterStemAnalyzer(new String[0] /* stopword list */,
										   false /* do lowercase */, 
										   false /* do stem */);
	
	    // Store the index path
	    _indexPath = index_path;
	}
		
	/** Main procedure for adding files to the index
	 * 
	 * @param files
	 * @param clear_old_index set to true to create a new index, or
	 *                        false to add to a currently existing index
	 * @return
	 */
	public boolean addFiles(List<File> files, boolean clear_old_index) {
	
		try {
		    // The boolean arg in the IndexWriter ctor means to
		    // create a new index, overwriting any existing index
			//
			// NOTES: Set create=false to add to an index (even while
			//        searchers and readers are accessing it... additional
			//        content goes into separate segments).
			//
			//        To merge can use:
			//        IndexWriter.addIndexes(IndexReader[]) and 
			//        IndexWriter.addIndexes(Directory[])
			//
			//        Index is optimized on optimize() or close()
		    IndexWriter w = new IndexWriter(_indexPath, _analyzer, clear_old_index,
		        IndexWriter.MaxFieldLength.UNLIMITED);
		    
		    // Add all files
		    for (File f : files) {
		    	System.out.println("Adding: " + f.getPath());
		    	DocAdder.AddDoc(w, f);
		    }
		    
		    // Close index writer
	        w.optimize();
		    w.close();
		    
		} catch (IOException e) {
			System.err.println(e);
			return false;
		}
		
		return true;
	}

	/** Main procedure for adding files to the index
	 *
	 * @param files
	 * @param clear_old_index set to true to create a new index, or
	 *                        false to add to a currently existing index
	 * @return
	 */
	public boolean addTweets(List<File> files, boolean clear_old_index) {

		try {
			// The boolean arg in the IndexWriter ctor means to
			// create a new index, overwriting any existing index
			//
			// NOTES: Set create=false to add to an index (even while
			//        searchers and readers are accessing it... additional
			//        content goes into separate segments).
			//
			//        To merge can use:
			//        IndexWriter.addIndexes(IndexReader[]) and
			//        IndexWriter.addIndexes(Directory[])
			//
			//        Index is optimized on optimize() or close()
			IndexWriter w = new IndexWriter(_indexPath, _analyzer, clear_old_index, IndexWriter.MaxFieldLength.UNLIMITED);

			// Add all files
			for (File f : files) {
				System.out.println("Adding tweets from file: " + f.getPath());
				DocAdder.AddTweet(w, f);
			}

			// Close index writer
			w.optimize();
			w.close();

		} catch (IOException e) {
			System.err.println(e);
			return false;
		}

		return true;
	}

	public static void main(String[] args) throws Exception {
		ConfigRead configRead = new ConfigRead();
		String index_path = configRead.getIndexPath();
		FileIndexBuilder b = new FileIndexBuilder(index_path);
		b.buildIndex("");
	}
	
	public void buildIndex(String filename) throws Exception {
		List<File> files;
		if(filename.equals(""))
			files = FileFinder.GetAllFiles("doc", ".csv", true);
		else {
			files = new ArrayList<>();
			files.add(new File(filename));
		}
		addTweets(files, true);
		
//		IndexDisplay.Display(index_path, System.out);
	}

}
