curl -X POST "http://localhost:9200/tweets/_search" -H 'Content-Type: application/json' -d\
'{"query":{"match_all":{}}}'