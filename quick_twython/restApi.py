# -*- coding: utf-8 -*-
import settings
import time
from twython import Twython, TwythonError, TwythonRateLimitError

class TwythonWrapper(Twython):
    """
    Wrapper for twitter REST APIs calls
    """
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret,filedir):

        filename=filedir + "_" + time.strftime("%Y-%m-%dT%H_%M_%S", time.gmtime()) +".jsons"
        self.filedir = filedir
        self.outputfile =  open(filename,"a+",encoding='utf-8')
        self.filebreak = filebreak
        TwythonWrapper.__init__(self, app_key, app_secret, oauth_token, oauth_token_secret)
