
import aenum
from bs4 import BeautifulSoup
import json
import logging
import pickle
import re
import urllib.parse

from sinaspider.pipeline import Pipeline, PipelineNode

### Data Structures

class Serializable(object):
    def serialize(self):
        """
        Serialize the instance to bytes. To deserialize:

            object = pickle.loads(b'...')
        """
        return pickle.dumps(self)

class SinaUser(Serializable):
    def __init__(self):
        self.uid = 0 
        self.nick_name = ''
        self.gender = '' 
        self.location = ''
        self.brief = ''
        self.birth = ''
        self.num_tweets = 0 
        self.num_followees = 0 # Been followed by the user.
        self.num_fans = 0
        self.sentiment = '' 
        self.vip_level = 0 # 0 indicates not a VIP
        self.is_certified = False
        self.homepage = ''
        self.labels = ''
    
    def __repr__(self):
        L = ['%s=%r' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

class SinaTweet(Serializable):
    def __init__(self):
        self.tid = 0 # Tweet ID
        self.uid = 0 
        self.content = ''
        self.time = ''
        self.coordinates = ''
        self.platform = '' # Phone type or PC?
        self.num_comments = 0
        self.num_loves = 0
        self.num_reposts = 0

    def __repr__(self):
        L = ['%s=%r' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))


class SinaRelation(Serializable):
    def __init__(self):
        """
        Relation a->b.
        """
        self.a = ''
        self.b = ''
    def __repr__(self):
        L = '%s->%r' % (self.a, self.b)
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))


### Pipeline definitions
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
        response = response.response
        links = []
        logger = logging.getLogger(self.name)
        try:
            content = decode_response_text(response)
            assert type(content) == type(dict())
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
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
        response = response.response
        links = []
        logger = logging.getLogger(self.name)
        try:
            content = decode_response_text(response)
            user = SinaUser()
            user_info_html_parser(content, user)
        except Exception:
            logger.exception('')
        return (user, )


class UserWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_WEIBO:
            return None
        response = response.response
        links = []
        logger = logging.getLogger(self.name)
        try:
            content = decode_response_text(response)
        except Exception:
            logger.exception('')
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


### Utility functions 

# requests.Response utility
def decode_response_text(response): 
    """
    Return the string of text decoded via response's content-type and charset.

    """
    type_entry = response.headers.get('content-type', None)
    if type_entry is None:
        return None
    content_type, char_set = type_entry.split(';')
    char_set = char_set.split('=')[-1]
    response.encoding = char_set
    ret = response.text
    if content_type == 'application/json':
        ret = json.loads(ret)
        # Default is 'text/html'
    ret = re.sub(r'(\\r)|(\\n)|(\r)|(\\t)', '', ret)
    ret = re.sub(r'\\/', '/', ret)
    ret = re.sub(r'\\"', '"', ret)
    ret = re.sub('&nbsp;', '', ret)
    return ret

def debug_str_response(response):
    """
    Return a string of useful information of the response
    
    Input:
    - response: A requests.Response.
    """
    if response is None:
        return 'None'
    history = list()
    if response.history:
        history = [res.url for res in response.history]
    ret = {
        'url': response.url,
        'headers': response.headers,
        'link': response.url,
        'history': history
    }
    return str(ret)

# User parser utility
def user_info_html_parser(html, user):
    config_box = ''
    number_box = ''
    info_box = ''
    for script in BeautifulSoup(html, 'lxml').find_all('script'):
        if "$CONFIG['page_id']" in str(script):
            config_box = script.contents[0]
        elif 'Pl_Core_T8CustomTriColumn__' in str(script):
            number_box = str(script)
        elif 'Pl_Core_UserInfo__' in str(script):
            info_box = str(script)
    if config_box:
        for value in config_box.split(';'):
            if "['oid']" in value:
                user.id = value.split("'")[-2]
    if number_box:
        number_box = extract_html_from_script(number_box)
        script = BeautifulSoup(number_box, 'lxml')
        for box in script.find_all('td', class_='S_line1'):
            name = box.span.contents[0].strip()
            number = box.strong.contents[0].strip()
            if name == '关注':
                user.num_followees = int(number)
            elif name == '粉丝':
                user.num_fans = int(number)
            elif name == '微博':
                user.num_tweets = int(number)
    if info_box:
        info_box = extract_html_from_script(info_box)
        script = BeautifulSoup(script, 'lxml')
        certified_box = script.find('p', class_='verify')
        if certified_box:
            user.is_certified = True
        for box in script.find_all('li', class_='S_line2'):
            item = box.find('span', class_='item_text')
            if 'Lv' in str(item):
                user.vip_level = int(item.span.contents[0].strip().split('.')[-1])
            elif '标签' in str(item):
                for label_box in item.find_all('a'):
                    user.label += label_box.contents[0] + ';'

# Trending weibo
def trending_weibo_html_parser(response):
    """
    Return a list of SinaTweets 
    """
    return list()

# Repost list 
def repost_list_html_parser(response):
    """
    Return a list of SinaTweets
    """
    return list()

# User weibo
def user_weibo_html_parser(response):
    """
    Return a list of SinaTweets
    """

# Others
def extract_html_from_script(script):
    if '<html>' in script or 'html' not in script:
        return ''
    return script[script.find('div') - 1:-21]
