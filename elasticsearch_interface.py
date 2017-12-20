from elasticsearch import Elasticsearch
from datetime import datetime
import time

class ElasticsearchInterface():

    def __init__(self):
        self.es = Elasticsearch()
       
        if not self.es.indices.exists(index="initial"):
            self.es.indices.create(index = "initial", body={"mappings":          \
                                                           {"na":                \
                                                           {"properties":        \
                                                           {"symbol":            \
                                                           {"type": "keyword" }, \
                                                            "is_vowel":          \
                                                           {"type": "keyword"},  \
                                                            "date":              \
                                                           {"type": "double"}}}}})

        if not self.es.indices.exists(index="followup"):
            self.es.indices.create(index = "followup", body={"mappings":         \
                                                            {"na":               \
                                                            {"properties":       \
                                                            {"initid":           \
                                                            {"type": "keyword"}, \
                                                             "date":             \
                                                            {"type": "double"}}}}})

    def log_initial(self, uuid, symbol, is_vowel):

        entry = {"symbol": symbol,     \
                 "is_vowel": is_vowel, \
                 "date": 1000000*float(datetime.utcnow().strftime('%s.%f'))}

        self.es.index("initial", doc_type='na', id=uuid, body=entry)

    def log_followup(self, uuid):

        entry = {"initid": uuid, \
                 "date": 1000000*float(datetime.utcnow().strftime('%s.%f'))}

        self.es.index("followup", doc_type='na', body=entry)

    def symbol_aggregates(self, symbol):

        followups = 0
        total_track = 0

        symb_all = self.es.search(index="initial", body={"query":            \
                                                        {"match":            \
                                                        {"symbol":           \
                                                        {"query": symbol}}}, \
                                                         "sort": "date",     \
                                                         "aggs":             \
                                                        {"earliest":         \
                                                        {"min":              \
                                                        {"field": "date"}},  \
                                                         "latest":           \
                                                        {"max":              \
                                                        {"field": "date"}}}}, size=1000)

        count = symb_all['hits']['total']  
        latest = datetime.utcfromtimestamp(symb_all['aggregations']['latest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
        earliest = datetime.utcfromtimestamp(symb_all['aggregations']['earliest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')

        while True:
        
            uuids = [x['_id'] for x in symb_all['hits']['hits']]
            follow_count = self.es.search(index="followup", body={"query": \
                                                                 {"terms": \
                                                                 {"initid": uuids}}}, size=0)
           
            followups += follow_count['hits']['total']
            total_track += len(uuids)

            if count <= total_track:
                break

            z = x['_source']['date']
            symb_all = self.es.search(index="initial", body={"query":            \
                                                            {"match":            \
                                                            {"symbol":           \
                                                            {"query": symbol}}}, \
                                                             "sort": "date",     \
                                                             "search_after": [z]}, size=1000)
            
        stats = {"symbol"   : symbol,   \
                 "count"    : count,    \
                 "latest"   : latest,   \
                 "earliest" : earliest, \
                 "followups": followups}

        return stats
        
    def range_aggregates(self, lower, upper):
        
        l_millidiv = lower.split('.')
        l_nomilli = time.mktime(time.strptime(l_millidiv[0], "%Y-%m-%d %H:%M:%S"))
        xlower = 1000000*float(l_nomilli) + float(l_millidiv[1])

        u_millidiv = upper.split('.')
        u_nomilli = time.mktime(time.strptime(u_millidiv[0], "%Y-%m-%d %H:%M:%S"))
        xupper = 1000000*float(u_nomilli) + float(u_millidiv[1])

        symbol_aggs = []

        
        for symbol in 'abcdefghijklmnopqrstuvwxyz':

            followups = 0
            total_track = 0

            
            symb_all = self.es.search(index="initial", body={"query":               \
                                                            {"bool":                \
                                                            {"must":                \
                                                           [{"match":               \
                                                            {"symbol":              \
                                                            {"query": symbol}}},    \
                                                            {"range":               \
                                                            {"date":                \
                                                            {"gte": xlower,         \
                                                             "lte": xupper}}} ] }}, \
                                                             "sort": "date",        \
                                                             "aggs":                \
                                                            {"earliest":            \
                                                            {"min":                 \
                                                            {"field": "date"}},     \
                                                             "latest":              \
                                                            {"max":                 \
                                                            {"field": "date"}}}}, size=1000)

            count = symb_all['hits']['total']

            if count:
            
                latest = datetime.utcfromtimestamp(symb_all['aggregations']['latest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                earliest = datetime.utcfromtimestamp(symb_all['aggregations']['earliest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')

                while True:
  
                    uuids = [x['_id'] for x in symb_all['hits']['hits']]
                    follow_count = self.es.search(index="followup", body={"query":           \
                                                                         {"bool":            \
                                                                         {"must":            \
                                                                        [{"terms":           \
                                                                         {"initid": uuids}}, \
                                                                         {"range":           \
                                                                         {"date":            \
                                                                         {"gte": xlower,     \
                                                                          "lte": xupper}}} ] }}}, size=0)

                    followups += follow_count['hits']['total']
                    total_track += len(uuids)

                    if count <= total_track:
                        break

                    z = x['_source']['date']
                    symb_all = self.es.search(index="initial", body={"query":               \
                                                                    {"bool":                \
                                                                    {"must":                \
                                                                   [{"match":               \
                                                                    {"symbol":              \
                                                                    {"query": symbol}}},    \
                                                                    {"range":               \
                                                                    {"date":                \
                                                                    {"gte": xlower,         \
                                                                     "lte": xupper}}} ] }}, \
                                                                     "sort": "date",        \
                                                                     "search_after": [z]}, size=1000)

                stats = {"symbol"   : symbol,   \
                         "count"    : count,    \
                         "latest"   : latest,   \
                         "earliest" : earliest, \
                         "followups": followups}
            
                symbol_aggs.append(stats)
        
        return symbol_aggs
