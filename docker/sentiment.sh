docker exec -it sentiment sh -c "wget --post-data 'The quick brown fox jumped over the lazy dog.' 'localhost:9000/?properties={\"annotators\":\"tokenize,ssplit,pos\",\"outputFormat\":\"json\"}' -O -"