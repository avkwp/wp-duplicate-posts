from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import database
from api import api
import os
import sys
sys.path.append('../')
import records

PORT = 9090
HOST = '127.0.0.1'

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/XmlRpcService')

server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler, allow_none=True)
server.register_introspection_functions()

print(os.path.join('sqlite_test', 'application.db'))

db = database.LocalDatabase(db_file=os.path.join('XMLRPC', 'sqlite_test', 'application.db'))
server.register_instance(api.Api(database=db))

print("The server is running at Port: " + PORT.__str__() + 
", Host: " + HOST.__str__())

server.serve_forever()