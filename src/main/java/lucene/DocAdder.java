/** Simple helper class to read a file and construct fields for indexing
 *  with an IndexWriter (can be memory of file-based).  Called by
 *  MemoryIndexBuilder and FileIndexBuilder.
 *
 * @author Scott Sanner
 */

package lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DocAdder {

	public static void AddDoc(IndexWriter w, File f) {
		try {
			Document doc = new Document();
			BufferedReader br = new BufferedReader(new FileReader(f));
			StringBuilder content = new StringBuilder();
			String line = null;
			String first_line = null;
			int count = 0;
			while ((line = br.readLine()) != null) {
				count++;
				if (first_line == null)
					first_line = line;
				content.append(line + "\n");

				doc.add(new Field("PATH", f.getPath(),
						Field.Store.YES, Field.Index.NO));

				doc.add(new Field("FIRST_LINE", first_line,
						Field.Store.YES, Field.Index.ANALYZED,
						Field.TermVector.WITH_POSITIONS_OFFSETS));

				doc.add(new Field("CONTENT", content.toString(),
						Field.Store.YES, Field.Index.ANALYZED,
						Field.TermVector.WITH_POSITIONS_OFFSETS));

				w.addDocument(doc);
			}
		} catch (IOException e) {
			System.err.println("Could not add file '" + f + "': " + e);
			e.printStackTrace(System.err);
		}

	}

	public static void AddTweet(IndexWriter w, File f) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
//			StringBuilder content = new StringBuilder();
			String line = null;
			String first_line = null;
			int count = 0;
			while ((line = br.readLine()) != null) {
				Document doc = new Document();
				count++;
				System.out.println(count);
//				content.append(line + "\n");
				String[] splits = line.split(" ");
				for(String field : splits){
					if(field.split("hashtag:").length > 1)
						doc.add(new Field("hashtag", field.split("hashtag:")[1], Field.Store.YES, Field.Index.ANALYZED,
								Field.TermVector.WITH_POSITIONS_OFFSETS));
					if(field.split("mention:").length > 1)
						doc.add(new Field("mention", field.split("mention:")[1], Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
					if(field.split("term:").length > 1)
						doc.add(new Field("term", field.split("term:")[1], Field.Store.YES, Field.Index.ANALYZED,
								Field.TermVector.WITH_POSITIONS_OFFSETS));
					if(field.split("location:").length > 1)
						doc.add(new Field("location", field.split("location:")[1], Field.Store.YES, Field.Index.ANALYZED,
								Field.TermVector.WITH_POSITIONS_OFFSETS));
					if(field.split("from:").length > 1)
						doc.add(new Field("from", field.split("from:")[1], Field.Store.YES, Field.Index.ANALYZED,
								Field.TermVector.WITH_POSITIONS_OFFSETS));
				}
				doc.add(new Field("PATH", line,
						Field.Store.YES, Field.Index.NO));

				doc.add(new Field("CONTENT", line,
						Field.Store.YES, Field.Index.ANALYZED,
						Field.TermVector.WITH_POSITIONS_OFFSETS));

				w.addDocument(doc);
			}

		} catch (IOException e) {
			System.err.println("Could not add file '" + f + "': " + e);
			e.printStackTrace(System.err);
		}

	}

}
