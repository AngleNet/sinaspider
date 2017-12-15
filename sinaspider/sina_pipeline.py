
import urllib.parse
import aenum

from sinaspider.pipeline import Pipeline, PipelineNode

class SinaResponseType(aenum.Enum):
    TRENDING_WEIBO = 0          # d.weibo.com
    REPOST_LIST = aenum.auto()  # weibo.com/aj/v6/mblog/info/big
    USER_INFO= aenum.auto()     # weibo.com/p/1005052840177141/info 
    USER_WEIBO = aenum.auto()   # weibo.com/p/aj/v6/mblog/mbloglist

    UNDEFINED = aenum.auto()    

class SinaResponse(object):
    def __init__(self, res_type, response):
        self.type = res_type
        self.response = response
    
    def __repr__(self):
        return '<%s, %s>' % (self.type, self.response)

class TrendingWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        if response.type != SinaResponseType.TRENDING_WEIBO:
            return None
        return (response, )

class RepostListProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        if response.type != SinaResponseType.REPOST_LIST:
            return None
        return (response, )

class UserInfoProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_INFO:
            return None
        return (response, )
class UserWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_WEIBO:
            return None
        return (response, )

class Router(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        url = urllib.parse.urlsplit(response.url)
        response = SinaResponse(SinaResponseType.UNDEFINED, response)
        if url.netloc == 'd.weibo.com':
            response.type = SinaResponseType.TRENDING_WEIBO
        elif url.netloc == 'weibo.com':
            if 'mblog/info/big' in url.path:
                response.type = SinaResponseType.REPOST_LIST
            elif 'info' in url.path:
                response.type = SinaResponseType.USER_INFO
            elif 'mblog/mbloglist' in url.path:
                response.type = SinaResponseType.USER_WEIBO
        return (response,)
        
        


class SinaPipeline(Pipeline):
    def __init__(self, queue):
        router = Router()
        Pipeline.__init__(self, self.__class__.__name__,
                          router, queue)
