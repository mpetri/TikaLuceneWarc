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






