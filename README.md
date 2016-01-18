This ./RUN.sh is to generate solr lucene index. The lucene index includes page number information, which means the related extractor is designed to get the page number information. I did not write volume based one because solr things are just for comparision, the setAnalyser is able to get the total counting from pagebased solr index.

Example of the command: ./RUN.sh <input document path> <output path> 
