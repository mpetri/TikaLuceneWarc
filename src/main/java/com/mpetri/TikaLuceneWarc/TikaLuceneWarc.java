package com.mpetri.TikaLuceneWarc;

import static java.nio.file.FileVisitOption.*;
import static java.nio.file.FileVisitResult.*;

import java.io.*;
import java.io.FileInputStream;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.ArrayDeque;
import java.util.concurrent.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.math3.util.Precision;
import org.apache.logging.log4j.*;
import org.apache.logging.log4j.core.config.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.en.*;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.*;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.xml.sax.SAXException;

public final class TikaLuceneWarc {
  public static final String FIELD_BODY = "contents";
  public static final String FIELD_URL = "url";
  public static final String FIELD_REALURL = "rurl";
  public static final String FIELD_DATE = "date";
  public static final String FIELD_ID = "id";

  public static class Posting implements Comparable<Posting> {
    public int id;
    public int freq;
    public ArrayList<Integer> pos;

    public Posting(int i, int s, ArrayList<Integer> p) {
      this.id = i;
      this.freq = s;
      this.pos = new ArrayList(p);
    }

    public Posting(int i, int s) {
      this.id = i;
      this.freq = s;
    }

    @Override
    public int compareTo(Posting anotherInstance) {
      return this.id - anotherInstance.id;
    }
  }

  public static class indexFile implements Callable {
    private Path warcFile;
    private IndexWriter writer;
    private HtmlParser parser;
    private boolean positions;
    private String languageFilter;

    public indexFile(Path warcFile, IndexWriter writer, boolean storePos, String languageF) {
      this.warcFile = warcFile;
      this.writer = writer;
      this.parser = new HtmlParser();
      this.positions = storePos;
      languageFilter = languageF;
    }

    public Integer call() {
      final Logger LOG = LogManager.getLogger(TikaLuceneWarc.class);
      int docCount = 0;
      // (1) open warc file
      FileInputStream is;
      ArchiveReader ar;
      try {
        is = new FileInputStream(warcFile.toString());
        // The file name identifies the ArchiveReader and indicates if it should be decompressed
        ar = WARCReaderFactory.get(warcFile.toString(), is, true);

        // Once we have an ArchiveReader, we can work through each of the records it contains
        int rec = 0;
        for (ArchiveRecord r : ar) {
          rec++;
          // The header file contains information such as the type of record, size, creation time, and URL
          ArchiveRecordHeader header = r.getHeader();
          String url = header.getUrl();
          String realUrl = header.getUrl();

          String mimetype = header.getMimetype();
          String date = header.getDate();
          if (!mimetype.contains("application/http")) {
            LOG.error(rec + " SKIP url = " + url + " date = " + date + " mime = " + mimetype);
            continue;
          }

          // (1) tika parse content
          try {
            BodyContentHandler handler = new BodyContentHandler(-1);
            Metadata metadata = new Metadata();
            parser.parse(r, handler, metadata);
            String parsedContent = handler.toString();

            boolean skipDocument = true;
            String lang = "all";
            if (languageFilter == "all") {
              skipDocument = false;
            } else {
              LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
              lang = identifier.getLanguage();
              if (lang.equals(languageFilter)) {
                skipDocument = false;
              }
            }

            if (skipDocument) {
              LOG.info(rec + " SKIP url = " + url + " date = " + date + " lang = '" + lang + "'");
            } else {
              LOG.info(
                  rec
                      + " INDEX url = "
                      + url
                      + " date = "
                      + date
                      + " size = "
                      + parsedContent.length()
                      + " lang = '"
                      + lang
                      + "'");

              // (3) make a new, empty document
              Document document = new Document();

              // (4) add document metadata
              if (url != null) {
                url = url.replaceFirst("^https://www\\.", "");
                url = url.replaceFirst("^https://", "");
                url = url.replaceFirst("^http://www\\.", "");
                url = url.replaceFirst("^http://", "");

                // change url from abc.domain.com to com.domain.abc
                String[] urlparts = url.split("/");
                String firstPart = urlparts[0];
                String[] mainurlParts = firstPart.split("\\.");
                for (int i = 0; i < mainurlParts.length / 2; i++) {
                  String temp = mainurlParts[i];
                  mainurlParts[i] = mainurlParts[mainurlParts.length - i - 1];
                  mainurlParts[mainurlParts.length - i - 1] = temp;
                }
                String newUrl = "";
                for (int i = 0; i < mainurlParts.length - 1; i++) {
                  newUrl += mainurlParts[i] + ".";
                }
                newUrl += mainurlParts[mainurlParts.length - 1] + "/";
                for (int i = 1; i < urlparts.length; i++) {
                  newUrl += urlparts[i] + "/";
                }
                url = newUrl;
              }
              document.add(new StringField(FIELD_URL, url, Field.Store.YES));
              document.add(new StringField(FIELD_REALURL, realUrl, Field.Store.YES));
              Long urlHash = (long) url.hashCode();

              String warcId = header.getReaderIdentifier();
              document.add(new StringField(FIELD_ID, warcId, Field.Store.YES));

              Instant d = Instant.parse(date);
              Long dateid = ((d.toEpochMilli() / 1000) << 30) + urlHash;
              document.add(new LongField(FIELD_DATE, dateid, Field.Store.YES));

              // (5) add document content
              FieldType fieldType = new FieldType();
              if (positions) {
                fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
              } else {
                fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
              }
              fieldType.setStoreTermVectors(true);
              document.add(new Field(FIELD_BODY, parsedContent, fieldType));

              // (6) index document content
              writer.addDocument(document);
              docCount++;
            }
          } catch (TikaException e) {
            System.err.println("TIKA " + url);
          } catch (IOException e) {
            System.err.println("TIKA " + url);
          } catch (SAXException e) {
            System.err.println("TIKA " + url);
          } catch (Exception e) {
            System.err.println("TIKA " + url);
          }
        }
      } catch (FileNotFoundException e) {
        System.err.println("Warc Parser: " + e.toString());
      } catch (IOException e) {
        System.err.println("Warc Parser: " + e.toString());
      }
      return docCount;
    }
  }

  public static void writeInt(DataOutputStream out, int value, boolean plain) throws IOException {
    if (plain) {
      String str = Integer.toString(value) + "\n";
      byte[] data = str.getBytes("UTF-8");
      out.write(data);
    } else {
      out.writeByte(value & 0xFF);
      out.writeByte((value >> 8) & 0xFF);
      out.writeByte((value >> 16) & 0xFF);
      out.writeByte((value >> 24) & 0xFF);
    }
  }

  public static void main(String[] args) throws Exception {
    final Logger LOG = LogManager.getLogger(TikaLuceneWarc.class);

    // (1) parse command line options
    Options options = new Options();
    options.addOption("help", false, "print this help output");
    options.addOption("plain", false, "write plaintext instead of 32-bit little endian");
    options.addOption("pos", false, "write positional information in .pos file");
    options.addOption("url", false, "perform url reordering");
    options.addOption("input", true, "input directory with warc files");
    options.addOption("startDate", true, "start date of the collection. format: 'YYYYMMDD'");
    options.addOption("stopDate", true, "stop date of the collection. format: 'YYYYMMDD'");
    options.addOption("lang", true, "language filter. default = 'en'. use 'all' for all languages");
    options.addOption("tmp", true, "temporary directory to store lucene index");
    options.addOption("output", true, "output directory to store d2si filse");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      // parse the command line arguments
      cmd = parser.parse(options, args);
    } catch (Exception exp) {
      // oops, something went wrong
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("TikaLuceneWarc", options);
      System.err.println("Parsing failed.  Reason: " + exp.getMessage());
      return;
    }
    String inputDir = "";
    String outputDir = "";
    String tmpDir = "./";
    String languageFilter = "en";
    boolean urlSorting = false;
    boolean storePositions = false;
    boolean writePlain = false;

    // define start and stop date
    Calendar cal = GregorianCalendar.getInstance();
    cal.set(2016, 8 - 1, 26); // start of the CC-NEWS crawl 20160826
    Date startDate = cal.getTime();

    Calendar ccal = Calendar.getInstance();
    Date stopDate = ccal.getTime(); // today
    final DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    if (cmd.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("TikaLuceneWarc", options);
      return;
    }
    if (cmd.hasOption("input")) {
      inputDir = cmd.getOptionValue("input");
    }
    if (cmd.hasOption("tmp")) {
      tmpDir = cmd.getOptionValue("tmp");
    }
    if (cmd.hasOption("lang")) {
      languageFilter = cmd.getOptionValue("lang");
    }
    if (cmd.hasOption("startDate")) {
      String sDateStr = cmd.getOptionValue("startDate");

      try {
        startDate = sdf.parse(sDateStr);
      } catch (ParseException pe) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TikaLuceneWarc", options);
        System.err.println("invalid start date.");
        return;
      }
      if (startDate == null) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TikaLuceneWarc", options);
        System.err.println("invalid start date.");
        return;
      }
    }
    if (cmd.hasOption("stopDate")) {
      String stopDateStr = cmd.getOptionValue("stopDate");
      try {
        stopDate = sdf.parse(stopDateStr);
      } catch (ParseException pe) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TikaLuceneWarc", options);
        System.err.println("invalid stop date.");
        return;
      }
      if (stopDate == null) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TikaLuceneWarc", options);
        System.err.println("invalid stop date.");
        return;
      }
    }

    if (cmd.hasOption("output")) {
      outputDir = cmd.getOptionValue("output");
    }
    if (cmd.hasOption("pos")) {
      storePositions = true;
    }
    if (cmd.hasOption("plain")) {
      writePlain = true;
    }
    if (cmd.hasOption("url")) {
      urlSorting = true;
    }
    if (inputDir == "" || outputDir == "") {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("TikaLuceneWarc", options);
      System.err.println("Command line parsing failed. missing input/base/output.");
      return;
    }
    String baseName =
        "CC-NEWS-" + sdf.format(startDate) + "-" + sdf.format(stopDate) + "-" + languageFilter;

    LOG.info("input dir = " + inputDir);
    LOG.info("tmp dir = " + tmpDir);
    LOG.info("output dir = " + outputDir);
    LOG.info("input dir = " + inputDir);
    LOG.info("base name = " + baseName);
    LOG.info("url sorted docids = " + urlSorting);
    LOG.info("start date = " + sdf.format(startDate));
    LOG.info("stop date = " + sdf.format(stopDate));

    // (1) determine input files
    final Date fstartDate = startDate;
    final Date fstopDate = stopDate;
    final ArrayDeque<Path> inputFileStack = new ArrayDeque<>();
    final PathMatcher matcher =
        FileSystems.getDefault().getPathMatcher("glob:" + inputDir + "**/*.warc.gz");
    Files.walkFileTree(
        Paths.get(inputDir),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (matcher.matches(file)) {
              // check if file is in date range
              String fileName = file.getFileName().toString();
              fileName = fileName.replaceFirst("^CC-NEWS-", "");
              String fdateStr = fileName.substring(0, 8);
              try {
                Date fileDate = sdf.parse(fdateStr);
                if (fileDate.after(fstartDate) || fileDate.equals(fstartDate)) {
                  if (fileDate.before(fstopDate) || fileDate.equals(fstopDate)) {
                    LOG.info("Found " + fileName);
                    inputFileStack.add(file);
                  }
                }
              } catch (ParseException pe) {
                System.err.println("Invalid file name. Could not parse date '" + fdateStr + "'");
              }
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
    LOG.info("Found " + inputFileStack.size() + " warc files.");

    // (2) configure lucene index writer
    {
      final StandardAnalyzer sa = new StandardAnalyzer(CharArraySet.EMPTY_SET);
      final IndexWriterConfig iwc = new IndexWriterConfig(sa);
      iwc.setSimilarity(new BM25Similarity());
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      iwc.setRAMBufferSizeMB(4 * 1024);
      iwc.setUseCompoundFile(false);
      iwc.setMergeScheduler(new ConcurrentMergeScheduler());
      final Path indexPath = Paths.get(tmpDir);
      final Directory indexDir = FSDirectory.open(indexPath);
      final IndexWriter writer = new IndexWriter(indexDir, iwc);

      // (3) iterate over all files and index
      final long start = System.nanoTime();
      int processors = Runtime.getRuntime().availableProcessors();
      ExecutorService es = Executors.newFixedThreadPool(processors);
      int numFiles = inputFileStack.size();
      LOG.info("threads = " + processors);
      LOG.info("files = " + numFiles);
      final ArrayDeque<Future> outputFutureStack = new ArrayDeque<>();
      for (Path warcFile : inputFileStack) {
        Future<Integer> future =
            es.submit(new indexFile(warcFile, writer, storePositions, languageFilter));
        outputFutureStack.add(future);
      }

      int numProcessed = 0;
      for (Future<Integer> future : outputFutureStack) {
        try {
          Integer indexed_docs = future.get();
          numProcessed++;
          double percent = (double) numProcessed / (double) numFiles * 100;
          LOG.info(
              "processed "
                  + numProcessed
                  + "/"
                  + numFiles
                  + " ("
                  + Precision.round(percent, 2)
                  + "%) - "
                  + indexed_docs);
        } catch (ExecutionException ex) {
          ex.getCause().printStackTrace();
        }
      }

      es.shutdown();
      try {
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        LOG.error("indexing files interrupted!");
        return;
      }

      // (4) merge into one index
      int numIndexed = writer.maxDoc();
      LOG.info("writing and merging indexes!");
      try {
        writer.commit();
        writer.forceMerge(1);
      } finally {
        writer.close();
      }
      final long durationMillis =
          TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      LOG.info(
          "Total "
              + numIndexed
              + " documents indexed in "
              + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
    }

    {
      LOG.info("Parsing Lucene index from disk.");
      Path indexPath = Paths.get(tmpDir);
      IndexReader reader;

      if (!Files.exists(indexPath)
          || !Files.isDirectory(indexPath)
          || !Files.isReadable(indexPath)) {
        throw new IllegalArgumentException(tmpDir + " does not exist or is not a directory.");
      }

      LOG.info("Reading index at " + tmpDir);
      reader = DirectoryReader.open(FSDirectory.open(indexPath));

      int nDocs = reader.numDocs();
      LOG.info("Number of documents " + nDocs);
      HashMap<String, Integer> docMap = new HashMap();
      LOG.info("Create document mapping.");
      String[] docWarcIDs = new String[nDocs];
      String[] docUrls = new String[nDocs];
      for (int i = 0; i < nDocs; i++) {
        Document doc = reader.document(i);
        String docURL = doc.get(FIELD_URL);
        String docRealURL = doc.get(FIELD_REALURL);
        String docDate = doc.get(FIELD_DATE);
        String docWarcId = doc.get(FIELD_ID);
        docWarcIDs[i] = docWarcId;
        docUrls[i] = docRealURL;
        if (urlSorting) {
          docMap.put(docURL + "-" + docDate, i);
        } else {
          if (docMap.containsKey(docDate)) {
            int exist_id = docMap.get(docDate);
            Document exist_doc = reader.document(exist_id);
            String edocURL = exist_doc.get(FIELD_URL);
            LOG.error(
                "Docmap already contains key "
                    + docDate
                    + " with value "
                    + exist_id
                    + " instead of "
                    + i);
            LOG.error(
                "Docmap already contains key "
                    + docDate
                    + " with url "
                    + edocURL
                    + " instead of "
                    + docURL);
          }
          docMap.put(docDate, i);
        }
      }
      LOG.info("Found " + docMap.size() + " docids in index");
      LOG.info("Create correct docId mapping.");
      HashMap<Integer, Integer> idMap = new HashMap();
      String[] sortedDocWarcIDs = new String[nDocs];
      String[] sortedUrls = new String[nDocs];
      if (urlSorting) {
        Object[] urls = docMap.keySet().toArray();
        Arrays.sort(urls);
        for (int i = 0; i < urls.length; i++) {
          String curl = String.valueOf(urls[i]);
          Integer old_id = docMap.get(curl);
          idMap.put(old_id, i);
          sortedDocWarcIDs[i] = docWarcIDs[old_id];
          sortedUrls[i] = docUrls[old_id];
        }
      } else {
        Object[] dates = docMap.keySet().toArray();
        List<Long> datelist = new ArrayList<Long>();
        for (int i = 0; i < dates.length; i++) {
          String cdate = String.valueOf(dates[i]);
          Long d = Long.parseLong(cdate);
          datelist.add(d);
        }
        Collections.sort(datelist);
        for (int i = 0; i < datelist.size(); i++) {
          String cdate = Long.toString(datelist.get(i));
          Integer old_id = docMap.get(cdate);
          idMap.put(old_id, i);
          sortedDocWarcIDs[i] = docWarcIDs[old_id];
          sortedUrls[i] = docUrls[old_id];
        }
      }

      int realnDocs = idMap.size();
      {
        LOG.info("Mapping postings lists.");
        Terms terms = SlowCompositeReaderWrapper.wrap(reader).terms(FIELD_BODY);

        String docsFile = outputDir + "/" + baseName + ".docs";
        FileOutputStream docsfos = new FileOutputStream(docsFile);
        BufferedOutputStream bdocsfos = new BufferedOutputStream(docsfos, 128 * 1024 * 1024);
        DataOutputStream docsdos = new DataOutputStream(bdocsfos);
        writeInt(docsdos, 1, writePlain);
        writeInt(docsdos, realnDocs, writePlain);

        String freqsFile = outputDir + "/" + baseName + ".freqs";
        FileOutputStream freqsfos = new FileOutputStream(freqsFile);
        BufferedOutputStream bfreqsfos = new BufferedOutputStream(freqsfos, 128 * 1024 * 1024);
        DataOutputStream freqsdos = new DataOutputStream(bfreqsfos);

        int numTerms = 0;
        long numPostings = 0;
        int[] docSizes = new int[realnDocs];

        if (storePositions) {
          String possFile = outputDir + "/" + baseName + ".pos";
          FileOutputStream possfos = new FileOutputStream(possFile);
          BufferedOutputStream bpossfos = new BufferedOutputStream(possfos, 128 * 1024 * 1024);
          DataOutputStream possdos = new DataOutputStream(bpossfos);

          TermsEnum termIter = terms.iterator();
          for (int i = 0; i < (docSizes.length); i++) {
            docSizes[i] = 0;
          }
          while (termIter.next() != null) {
            // for each posting
            int ft = termIter.docFreq();
            writeInt(docsdos, ft, writePlain);
            writeInt(freqsdos, ft, writePlain);
            ArrayList<Posting> postings = new ArrayList();
            DocsAndPositionsEnum docPosEnum = termIter.docsAndPositions(null, null);
            int ret = docPosEnum.nextDoc();
            while (ret != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              int dId = docPosEnum.docID();
              Integer mapped_id = idMap.get(dId);
              if (mapped_id == null) {
                LOG.error("cannot find mapping for docid = " + dId);
              } else {
                int fdt = docPosEnum.freq();
                docSizes[mapped_id] += fdt;
                ArrayList<Integer> positions = new ArrayList();

                for (int i = 0; i < fdt; ++i) {
                  final int position = docPosEnum.nextPosition();
                  positions.add(position);
                }
                postings.add(new Posting(mapped_id, fdt, positions));
              }
              ret = docPosEnum.nextDoc();
            }

            Collections.sort(postings);
            for (Posting posting : postings) {
              writeInt(docsdos, posting.id, writePlain);
              writeInt(freqsdos, posting.freq, writePlain);

              writeInt(possdos, posting.freq, writePlain);
              for (int i = 0; i < posting.freq; i++) {
                writeInt(possdos, posting.pos.get(i), writePlain);
              }
            }

            numPostings += ft;
            numTerms++;
          }
          possdos.close();
        } else {
          TermsEnum termIter = terms.iterator();
          for (int i = 0; i < (docSizes.length); i++) {
            docSizes[i] = 0;
          }
          while (termIter.next() != null) {
            // for each posting
            int ft = termIter.docFreq();
            writeInt(docsdos, ft, writePlain);
            writeInt(freqsdos, ft, writePlain);
            ArrayList<Posting> postings = new ArrayList();
            DocsEnum docEnum = termIter.docs(null, null, DocsEnum.FLAG_FREQS);
            int ret = docEnum.nextDoc();
            while (ret != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              int dId = docEnum.docID();
              Integer mapped_id = idMap.get(dId);
              if (mapped_id == null) {
                LOG.error("cannot find mapping for docid = " + dId);
              } else {
                int fdt = docEnum.freq();
                docSizes[mapped_id] += fdt;
                postings.add(new Posting(mapped_id, fdt));
              }
              ret = docEnum.nextDoc();
            }
            Collections.sort(postings);
            for (Posting posting : postings) {
              writeInt(docsdos, posting.id, writePlain);
              writeInt(freqsdos, posting.freq, writePlain);
            }
            if (ft != postings.size()) {
              LOG.error("postings.size() = " + postings.size() + " ft = " + ft);
            }
            numPostings += ft;
            numTerms++;
          }
        }

        LOG.info("Number of terms " + numTerms);
        LOG.info("Number of postings " + numPostings);
        freqsdos.close();
        docsdos.close();

        // write the document sizes in word tokens
        String sizeFile = outputDir + "/" + baseName + ".sizes";
        LOG.info("Creating document size file " + sizeFile);
        FileOutputStream sizefos = new FileOutputStream(sizeFile);
        BufferedOutputStream bsizefos = new BufferedOutputStream(sizefos, 64 * 1024 * 1024);
        DataOutputStream sizedos = new DataOutputStream(bsizefos);
        writeInt(sizedos, realnDocs, writePlain);
        LOG.info("writing " + realnDocs + " document sizes");
        for (int i = 0; i < (docSizes.length); i++) {
          writeInt(sizedos, docSizes[i], writePlain);
        }
        sizedos.close();

        // write the document warcIds 
        String warcIdFile = outputDir + "/" + baseName + ".warcids";
        LOG.info("Creating warcid file " + warcIdFile);
        FileOutputStream widfos = new FileOutputStream(warcIdFile);
        BufferedOutputStream bwidfos = new BufferedOutputStream(widfos, 64 * 1024 * 1024);
        PrintStream widps = new PrintStream(bwidfos);
        for (int i = 0; i < (realnDocs); i++) {
          widps.print(sortedDocWarcIDs[i]);
        }
        widps.close();

        // write the document urls 
        String urlFile = outputDir + "/" + baseName + ".urls";
        LOG.info("Creating url file " + urlFile);
        FileOutputStream urlfos = new FileOutputStream(urlFile);
        BufferedOutputStream burlfos = new BufferedOutputStream(urlfos, 64 * 1024 * 1024);
        PrintStream urlps = new PrintStream(burlfos);
        for (int i = 0; i < (realnDocs); i++) {
          urlps.print(sortedUrls[i]);
        }
        urlps.close();
      }
    }
  }
}
