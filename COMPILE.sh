#!/bin/bash
javac -classpath libs/lucene-core-4.10.1-SNAPSHOT.jar:libs/commons-io-2.4.jar:libs/lucene-analyzers-common-4.10.1.jar:libs/lucene-queryparser-4.10.1.jar:libs/pairtree-1.0.0.jar:src -d bin  src/wordbasedIndexer.java
