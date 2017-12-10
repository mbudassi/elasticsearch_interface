from elasticsearch import Elasticsearch
from datetime import datetime
import time

class ElasticsearchInterface():

    def __init__(self):
        #The elasticsearch python connector
        self.es = Elasticsearch()

        #Mapping "initial" index i.e. creating elasticsearch schema for initial documents. Most important: date is of type double, so that it can be sorted later. We are not using Elasticsearch's native date datatype because it does not save 6 subsecond decimal places. Instead, we are using epoch time. (Level of precision should be identical to sqllite implementation). Doc_type is being phased out in next version of elasticsearch, so we'll set it arbitrarily to "na" here, and never make use of it.
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

        #Mapping "followup" index i.e. creating elasticsearch schema for followup documents. In addition to date double format, initid type must be set to keyword for term matching later.
        if not self.es.indices.exists(index="followup"):
            self.es.indices.create(index = "followup", body={"mappings":         \
                                                            {"na":               \
                                                            {"properties":       \
                                                            {"initid":           \
                                                            {"type": "keyword"}, \
                                                             "date":             \
                                                            {"type": "double"}}}}})

    #Different indices for different fields: Initial document contains "symbol","is_vowel", and "date". uuid is stored in native "id" field
    def log_initial(self, uuid, symbol, is_vowel):

        entry = {"symbol": symbol,     \
                 "is_vowel": is_vowel, \
                 "date": 1000000*float(datetime.utcnow().strftime('%s.%f'))}

        self.es.index("initial", doc_type='na', id=uuid, body=entry)

    #Different indices for different fields: Followup document contains uuid (we save it as initid to remind ourselves that it is the uuid of the initial entry), and date. We don't care what the id of the followup is though, so we let it be automatically assigned.
    def log_followup(self, uuid):

        entry = {"initid": uuid, \
                 "date": 1000000*float(datetime.utcnow().strftime('%s.%f'))}

        self.es.index("followup", doc_type='na', body=entry)

    def symbol_aggregates(self, symbol):

        followups = 0
        total_track = 0

        #Query for the symbol, and use aggs to aggregate results and determine the min and max fielddates.
        #Limit number of returned documents to 1000 to limit memory usage. Sort them to pick up where we left off, if there are more than 1000.
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

        #count is the actual number of matches, and may be much bigger than 1000. Latest and earliest are converted back from epoch time to human readable.
        count = symb_all['hits']['total']  
        latest = datetime.utcfromtimestamp(symb_all['aggregations']['latest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
        earliest = datetime.utcfromtimestamp(symb_all['aggregations']['earliest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')

        #However, there may be more than 1000 documents, so we enter a loop:
        while True:
        
            #Get the id for every document in the above symbol-query, store it in a list, and see if it matches any followup "initid" (Note, can't search more than 1024 terms)
            #Set returned documents to 0 because we don't care about the content of the documents 
            uuids = [x['_id'] for x in symb_all['hits']['hits']]
            follow_count = self.es.search(index="followup", body={"query": \
                                                                 {"terms": \
                                                                 {"initid": uuids}}}, size=0)
           
            #If it does match followup document "initid"s, find out how many
            followups += follow_count['hits']['total']
            total_track += len(uuids)

            #If there are more matches to the symbol query than the number of documents we retrieved from the symbol query, then get the documents for the remaining matches. Else, stop here i.e. break.
            if count <= total_track:
                break

            #Get the date of the last retrieved document, and "search after" for the rest of them, 1000 at a time, picking up from this date.
            z = x['_source']['date']
            symb_all = self.es.search(index="initial", body={"query":            \
                                                            {"match":            \
                                                            {"symbol":           \
                                                            {"query": symbol}}}, \
                                                             "sort": "date",     \
                                                             "search_after": [z]}, size=1000)
            
        #Assemble the results, and return it
        stats = {"symbol"   : symbol,   \
                 "count"    : count,    \
                 "latest"   : latest,   \
                 "earliest" : earliest, \
                 "followups": followups}

        return stats
        
    def range_aggregates(self, lower, upper):
        
        #sqllite implimentation dictates the way we specify the upper and lower bounds. So, split the string to remove the subsecond portion, convert the remainder to epoch time, and put the subsecond potion back in.
        l_millidiv = lower.split('.')
        l_nomilli = time.mktime(time.strptime(l_millidiv[0], "%Y-%m-%d %H:%M:%S"))
        xlower = 1000000*float(l_nomilli) + float(l_millidiv[1])

        u_millidiv = upper.split('.')
        u_nomilli = time.mktime(time.strptime(u_millidiv[0], "%Y-%m-%d %H:%M:%S"))
        xupper = 1000000*float(u_nomilli) + float(u_millidiv[1])

        symbol_aggs = []

        #Same idea as sqllite implementation: Go through every letter:
        for symbol in 'abcdefghijklmnopqrstuvwxyz':

            followups = 0
            total_track = 0

            #Identical to symbol aggregates function, except we specify the range:
            #"bool" sets up the conditional statement: must match symbol and be within range, greater than or equal to (gte) lower bound and less than or equal to (lte) the upper one.
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

            #Proceed if there are any hits
            if count:
            
                latest = datetime.utcfromtimestamp(symb_all['aggregations']['latest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                earliest = datetime.utcfromtimestamp(symb_all['aggregations']['earliest']['value']/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')

                while True:

                    #Range must also be indicated for the followup documents that match the initial ids
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
            
                #Rather than returning the collected values, we collect it in an aggregate
                symbol_aggs.append(stats)

        #Then, after going through all the symbols, we return the aggregate
        return symbol_aggs
