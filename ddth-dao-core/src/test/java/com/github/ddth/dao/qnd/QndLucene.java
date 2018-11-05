package com.github.ddth.dao.qnd;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.github.ddth.lucext.directory.IndexManager;

public class QndLucene {
    public final static void main(String[] args) throws IOException {
        File dir = new File("./temp");
        FileUtils.deleteQuietly(dir);
        dir.mkdirs();

        try (Directory DIR = FSDirectory.open(dir.toPath())) {
            try (IndexManager indexManager = new IndexManager(DIR)) {
                indexManager.init();
                {
                    Document doc = new Document();
                    doc.add(new StringField("id", "1", Store.YES));
                    doc.add(new StoredField("name", "Nguyễn Bá Thành"));
                    doc.add(new StoredField("yob", 1981));
                    StoredField field = new StoredField("d", 1.0);
                    field.setLongValue(1);
                    indexManager.getIndexWriter().addDocument(doc);
                }
                {
                    IndexSearcher indexSearcher = indexManager.getIndexSearcher();
                    TopDocs result = indexSearcher.search(new TermQuery(new Term("id", "1")), 1);
                    Document doc = indexSearcher.doc(result.scoreDocs[0].doc);
                    doc.getFields().forEach(f -> {
                        System.out.println(f.name() + " - " + f.fieldType() + " - "
                                + f.getClass().getSimpleName() + ": " + f);
                        System.out.println(f.numericValue());
                        System.out.println(f.stringValue());
                        System.out.println(f.readerValue());
                        System.out.println(f.binaryValue());
                        System.out.println();
                    });
                }
            }
        }
    }

}
