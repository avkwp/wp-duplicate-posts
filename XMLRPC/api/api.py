from database import HostDatabase, Writer
from datetime import datetime
from rpc_helpers import Fault
from rpc_helpers import get_code
import base64
import random
from base.session import Session
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import records

MODE_FULL = 0
MODE_OUTCOME = 1
MODE_FILE = 2
MODE_FEED = 3

class Request(object):

  def share_outcome_attrs(self):
    pass

  def share_outcome_existing(self):
    pass

  def share_outcome_nonexisting(self):
    pass

  def resolve_duplicates(self):
    pass

class Api(Session, Request):

    def __init__(self, database):
        self.database = database
        self.db = self.database.get_conn()

    def set_host(self, dbname):
        self.host_db = HostDatabase(dbname)
    
    def register_request(self, mode, dbname=None, sql=None, filename=None, num_cols_start=4):
      if((sql is not None) and (dbname is not None)):
          self.set_host(dbname)
          writer = Writer(self.host_db)
          writer.write_to_csv(sql)
      elif filename is not None:
          pass
      if(mode == MODE_FULL):
          resolve = records.set_original_and_score(num_cols_start, filename, writer.to_file)
          resolve().resolve_duplicates(writer=writer, contingency_write=True)
      elif(mode == MODE_FEED):
          session_id = self.obtainSession(True, "reshoringuk", "feed", None, "")
          resolve = records.set_feed_and_score(num_cols_start, filename, writer.to_file)
          resolve().resolve_duplicates(session_id, writer=writer, contingency_write=True)
          return session_id
      else:
          pass