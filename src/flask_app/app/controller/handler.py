from os import environ
from bson import ObjectId
from flask_restful import Resource
from json import JSONEncoder
from traceback import print_exc
from flask import request, make_response
from .utils.ProcessQueryMongoNew import process_query


from logging import getLogger
logger = getLogger()



class JsonEncoder(JSONEncoder):
    def default(self, object):
        if isinstance(object, ObjectId):
            return str(object)
        return JSONEncoder.default(self, object)



class SearchAPI(Resource):
    def post(self):
        payload = request.get_json(force=True)
        type = payload["type"].lower() 
        query = payload["query"]
        num_results = payload["num_results"]
        logger.info("[INFO] Executing query")
        
        global col_len, collection, inv_index, ps, STwords, col_nolyrics, terms_index, bm_avg
        try:
            logger.info("[INFO] Query executed")
            if type =="boolean" or type=="ranked" or type=='ranked_bm':
                
                response = process_query(query=query, qtype=type, col_len=col_len, 
                            collection=col_nolyrics, inv_index=inv_index, ps=ps, STwords=STwords)
                
                return make_response(response, 200)
            else:
                response={"body": "query not supported"}
                return make_response(response,500)  
            
            
        except Exception as e:
            print_exc()
            response = {'error':str(e)}
            logger.error(f"Query could not be executed: {str(e)}")
            return make_response(response, 400)
        
        
# class LyricsAPI(Resource):
    
#     def post(self):
#         payload = request.get_json(force=True)
#         logger.info("[INFO] Executing query")
#         # Camelcase frontend convention mapping
#         artist = payload["Artist"]
#         title = payload["Title"]    
#         try:
#             logger.info("[INFO] Query executed")
#             if type =="boolean" or type=="ranked":
#                 response = process_query(query=query, qtype=type)
#                 return make_response(response, 200)
#             else:
#                 response={"body": "query not supported"}
#                 return make_response(response,500)  
#         except Exception as e:
#             print_exc()
#             response = {'error':str(e)}
#             logger.error(f"Query could not be executed: {str(e)}")
#             return make_response(response, 400)
