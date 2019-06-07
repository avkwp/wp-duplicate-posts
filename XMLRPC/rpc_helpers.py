class Fault(Exception):

    def __init__(self, faultCode, faultMessage=None):
        self.faultCode = faultCode
        self.faultMessage = faultMessage
        pass

    def __str__(self):
        return "FaultCode: " + self.faultCode + ", FaultMessage: " + self.faultMessage


def get_code(short_code):
    return short_code.split("__")
