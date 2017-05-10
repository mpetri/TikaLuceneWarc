# A Tike-Lucene WARC parser for the CC-NEWS crawl

# Installation

```
git clone https://github.com/mpetri/TikaLuceneWarc.git
mvn clean package appassembler:assemble
```

# Getting the collection from AWS

This requires at least 1TB per year (crawl started 26/08/16)

```
apt-get install awscli
aws s3 cp --no-sign-request --recursive s3://commoncrawl/crawl-data/CC-NEWS/ .
```

Retrieve only specific years:

```
aws s3 cp --no-sign-request --recursive s3://commoncrawl/crawl-data/CC-NEWS/2017/ .
```
# D2SI Output format 

See description [here](https://github.com/ot/ds2i)

A binary sequence is a sequence of integers prefixed by its length, where both the sequence integers and the length are written as 32-bit little-endian unsigned integers.

A collection consists of 3 files, `<basename>.docs`, `<basename>.freqs`, `<basename>.sizes`.

`<basename>.docs` starts with a singleton binary sequence where its only integer is the number of documents in the collection. It is then followed by one binary sequence for each posting list, in order of term-ids. Each posting list contains the sequence of document-ids containing the term.

`<basename>.freqs` is composed of a one binary sequence per posting list, where each sequence contains the occurrence counts of the postings, aligned with the previous file (note however that this file does not have an additional singleton list at its beginning).

`<basename>.sizes` is composed of a single binary sequence whose length is the same as the number of documents in the collection, and the i-th element of the sequence is the size (number of terms) of the i-th document.

In addition the following files are created

`<basename>.urls` the urls of the documents one per line.

`<basename>.warcids` the unique warcids of the documents one per line.

## Optional

`<basename>.pos` the positional information of each ocurrence in the following format: In the same order as `<basename>.freqs` for each element `f` in `<basename>.freqs` a list of positions in the document is written as a binary sequence whose length is `f`.



# Creating a collection

Create CCNEWS-20160901-20170228-EN

```
cd TikaLuceneWarc
mvn clean package appassembler:assemble
./target/appassembler/bin/TikaLuceneWarc -input /path/to/CC-NEWS -output /d2si/output/path/ -tmp /tmp/lucene/index/dir/ -url -startDate 20160901 -stopDate 20170228 -lang en
```

Create CCNEWS-20160901-20170228-DE

```
cd TikaLuceneWarc
mvn clean package appassembler:assemble
./target/appassembler/bin/TikaLuceneWarc -input /path/to/CC-NEWS -output /d2si/output/path/ -tmp /tmp/lucene/index/dir/ -url -startDate 20160901 -stopDate 20170228 -lang de
```

Create CCNEWS-20170101-20171231-ALL

```
cd TikaLuceneWarc
mvn clean package appassembler:assemble
./target/appassembler/bin/TikaLuceneWarc -input /path/to/CC-NEWS -output /d2si/output/path/ -tmp /tmp/lucene/index/dir/ -url -startDate 20160901 -stopDate 20171231 -lang all
```

Create CCNEWS-20170101-20171231-ALL with positional information

```
cd TikaLuceneWarc
mvn clean package appassembler:assemble
./target/appassembler/bin/TikaLuceneWarc -input /path/to/CC-NEWS -output /d2si/output/path/ -tmp /tmp/lucene/index/dir/ -url -startDate 20160901 -stopDate 20171231 -lang all -pos
```


Create CCNEWS-20170101-20171231-ALL with date ordering

```
cd TikaLuceneWarc
mvn clean package appassembler:assemble
./target/appassembler/bin/TikaLuceneWarc -input /path/to/CC-NEWS -output /d2si/output/path/ -tmp /tmp/lucene/index/dir/ -startDate 20160901 -stopDate 20171231 -lang en
```






