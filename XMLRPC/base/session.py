from rpc_helpers import Fault
from database import LocalDatabase
import base64
import random

def build_short_code(project_code, unit_code):
    return project_code + "__" + unit_code

class Session():

    def __init__(self, database):
        self.database = database
        self.db = self.database.get_conn()
        
    def createProjectShortCode(self, project_code, unit_code, password):
        try:
            return self.database.create_project(project_code, unit_code, password)
        except Exception as e:
            raise e
        return True
    
    def expireSession(self, session_id):
        self.database.expire_session(session_id)
        return True

    def obtainSession(self, refresh, project_code, unit_code, expiry_timestamp, session_id):
        cursor = self.db.cursor()
        if not refresh:
            try:
                cursor.execute("""
                SELECT * FROM session WHERE project_code = '{project_code}' AND
                unit_code = '{unit_code}'
                """.format(project_code=project_code, unit_code=unit_code))
                session = cursor.fetchone()
                # do not check for expiry and do not raise a fault
                if session and len(session) > 0 and (session[4] > expiry_timestamp) and (session[2] == session_id):
                    return session[2]
                else:
                    self.database.delete_session(project_code, unit_code)
                    session_id = base64.b64encode(
                        (random.randint(1, 100).__str__() + build_short_code(project_code, unit_code)).encode()).decode()
                    session_id = self.database.issue_session(project_code, unit_code, session_id)
                if session_id:
                    return session_id
                else:
                    raise Fault("session_error", "Session not issued")
            except Exception as e:
                if len(e.args) > 1:
                    raise Fault(e.args[0], e.args[1])
                else:
                    raise Fault(e.args[0], "An error occurred in obtaining a session")
        else:
            session_id = base64.b64encode((random.randint(1,100).__str__() + build_short_code(project_code, unit_code)).encode()).decode()
            self.database.delete_session(project_code, unit_code)
            self.database.expire_session(session_id)
            try:
                # makes sure it removes the duplicates
                session_id = self.database.issue_session(project_code, unit_code, session_id)
                if session_id:
                    return session_id
                else:
                    raise Fault("session_error", "Session not issued")
            except Exception as e:
                if len(e.args) > 1:
                    raise Fault(e.args[0], e.args[1])
                else:
                    raise Fault(e.args[0], "An error occurred in obtaining a session")

    def verifyProjectShortCode(self, project_code, unit_code, password, session_id):
        try:
            return self.database.verify_project(project_code, unit_code, password, session_id)
        except Exception as e:
            raise e
        return None