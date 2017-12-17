
import aenum
from bs4 import BeautifulSoup
import json
import logging
from os.path import dirname, join, abspath
import pickle
import plyvel
import re
import urllib.parse

from sinaspider.pipeline import Pipeline, PipelineNode

### Links
_USER_TWEETS_LINKS = {
    'top_page': 'https://www.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=%s'
                '&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=%s&'
                'id=%s&feed_type=0&domain_op=%s',
    'mid_page': 'https://www.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=%s'
                '&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=%s'
                'pre_page=%s&pagebar=%s&id=%s&domain_op=%s',
    'bot_page': 'https://www.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=%s'
                '&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=%s'
                '&pre_page=%s&pagebar=%s&id=%s&domain_op=%s'
}
_USER_INFO_LINK = 'http://www.weibo.com/p/%s/info'
_TRENDING_TWEETS_LINK = 'https://d.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6'
                        '&domain=102803_ctg1_1760_-_ctg1_1760&pagebar=0&tab=home'
                        '&current_page=1&pre_page=1&page=1&pl_name=Pl_Core_NewMixFeed__3'
                        '&id=102803_ctg1_1760_-_ctg1_1760&script_uri=/&feed_type=1'
                        '&domain_op=102803_ctg1_1760_-_ctg1_1760'    # Scheduler seed.
_RETWEET_LINKS = 'https://www.weibo.com/aj/v6/mblog/info/big?ajwvr=6&id=%s&page=%s&ouid=%s'
_USER_HOME_LINK = {
    'id': 'http://www.weibo.com/u/%s',
    'nick': 'http://www.weibo.com/n/%s'
}


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
        self.page_id = ''
    
    def __repr__(self):
        L = ['%s=%r' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

class SinaTweet(Serializable):
    def __init__(self):
        self.tid = 0 # Tweet ID
        self.uid = 0 
        self.otid = 0 # Origin
        self.ouid = 0
        self.content = ''
        self.time = 0
        self.coordinates = ''
        self.platform = '' # Phone type or PC?
        self.num_comments = 0
        self.num_loves = 0
        self.num_reposts = 0

    def __repr__(self):
        L = ['%s=%r' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))


class SinaFlow(Serializable):
    def __init__(self):
        """
        A flow from a to b.
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

class SinaPipeline(Pipeline):
    """
    The Pipeline seems like:
             +-----+      ---->TrendingWeiboProcessor--->LevelDBWriter
             |  R  |     / 
     ------->|  O  +----/  --->UserWeiboProcessor--->LevelDBWriter
     Response|  U  +------/
     ------->|  T  +---------->UserInfoprocessor--->LevelDBWriter
             |  E  +----+
             |  R  |     \---->RepostListProcessor--->LevelDBWriter
             +-----+
    """
    def __init__(self, queue):
        router = Router()
        Pipeline.__init__(self, self.__class__.__name__,
                          router, queue)
        wtlevdb = LevelDBWriter()
        ptrweibo = TrendingWeiboProcessor()
        ptrweibo.forward(wtlevdb)
        prelist = RepostListProcessor()
        prelist.forward(wtlevdb)
        puinfo = UserInfoProcessor()
        puinfo.forward(wtlevdb)
        puweibo = UserWeiboProcessor()
        puweibo.forward(wtlevdb)
        router.forward(ptrweibo)
        router.forward(prelist)
        router.forward(puinfo)
        router.forward(puweibo)

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
        

class TrendingWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        if response.type != SinaResponseType.TRENDING_WEIBO:
            return None
        response = response.response
        links = set()
        logger = logging.getLogger(self.name)
        try:
            content_json = decode_response_text(response)
            assert type(content_json) == type(dict())
            if content_json['code'] != '100000':
                logger.debug('%s failed.' % response.url)
                return # Failed, need retry.
            content = strip_text_wight_blank(content_json['data'])
            tweets, flows = tweet_page_parser(content)
            for tweet in tweets:
                if tweet.num_reposts > 0:
                    link = _RETWEET_LINKS % (tweet.tid, 1, tweet.uid)
                    links.add(link)
                    link = _USER_HOME_LINK['id'] % tweet.uid
            for flow in flows:
                if type(flow.a) is int:
                    link = _USER_HOME_LINK['id'] % flow.a
                elif type(flow.a) is str:
                    link = _USER_HOME_LINK['nick'] % flow.a
                link = urllib.parse.quote(link)
                links.add(link)
                if type(flow.b) is int:
                    link = _USER_HOME_LINK['id'] % flow.b
                elif type(flow.b) is str:
                    link = _USER_HOME_LINK['nick'] % flow.b
                link = urllib.parse.quote(link)
                links.add(link)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
        return (tweets, flows, links)

class RepostListProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        if response.type != SinaResponseType.REPOST_LIST:
            return None
        response = response.response
        links = set()
        logger = logging.getLogger(self.name)
        try:
            content_json = decode_response_text(response)
            assert type(content_json) == type(dict())
            if content_json['code'] != '100000':
                logger.debug('%s failed.' % response.url)
                return # Failed, need retry.
            content = strip_text_wight_blank(content_json['data'])
            tweets, flows = retweet_list_page_parser(content)
            for flow in flows:
                if type(flow.a) is int:
                    link = _USER_HOME_LINK['id'] % flow.a
                elif type(flow.a) is str:
                    link = _USER_HOME_LINK['nick'] % flow.a
                link = urllib.parse.quote(link)
                links.add(link)
                if type(flow.b) is int:
                    link = _USER_HOME_LINK['id'] % flow.b
                elif type(flow.b) is str:
                    link = _USER_HOME_LINK['nick'] % flow.b
                link = urllib.parse.quote(link)
                links.add(link)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
        return (tweets, flows, links)


        return (response, )

class UserInfoProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_INFO:
            return None
        response = response.response
        links = []
        logger = logging.getLogger(self.name)
        try:
            content = decode_response_text(response)
            user = SinaUser()
            user_info_html_parser(content, user)
            user.homepage = response.url
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
        return (user, )


class UserWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_WEIBO:
            return None
        response = response.response
        links = set()
        logger = logging.getLogger(self.name)
        try:
            content_json = decode_response_text(response)
            assert type(content_json) == type(dict())
            if content_json['code'] != '100000':
                logger.debug('%s failed.' % response.url)
                return # Failed, need retry.
            content = strip_text_wight_blank(content_json['data'])
            tweets, flows = tweet_page_parser(content)
            for tweet in tweets:
                if tweet.num_reposts > 0:
                    link = _RETWEET_LINKS % (tweet.tid, 1)
                    links.add(link)
            for flow in flows:
                if type(flow.a) is int:
                    link = _USER_HOME_LINK['id'] % flow.a
                elif type(flow.a) is str:
                    link = _USER_HOME_LINK['nick'] % flow.a
                link = urllib.parse.quote(link)
                links.add(link)
                if type(flow.b) is int:
                    link = _USER_HOME_LINK['id'] % flow.b
                elif type(flow.b) is str:
                    link = _USER_HOME_LINK['nick'] % flow.b
                link = urllib.parse.quote(link)
                links.add(link)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
        return (tweets, flows, links)

class LevelDBWriter(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
        self.db_dir = join(dirname(dirname(abspath(__file__))), 'data')
        self.db_name_map = {
            'SinaTweet': 'tweets.db',
            'SinaUser': 'users.db',
            'SinaFlow': 'flows.db'
        }
    
    def run(self, client, *kws):
        if not kws:
            return
        logger = logging.getLogger(self.name)
        for entries in *kws:
            if not entries:
                continue
            entry = entries.pop()
            db_name = self.db_name_map.get(entry.__class__.__name__, 'error.db')
            db = plyvel.DB(join(self.db_dir, db_name), create_if_missing=True) 
            wb = db.write_batch()
            if type(entry) is SinaFlow:
                wb.put(pickle.dumps(entry.a), pickle.dumps(entry.b))
                for entry in entries:
                    wb.put(pickle.dumps(entry.a), pickle.dumps(entry.b))
            elif type(entry) is SinaUser:
                wb.put(pickle.dumps(entry.uid), entry.serialize())
                for entry in entries:
                    wb.put(pickle.dumps(entry.uid), entry.serialize())
            elif type(entry) is SinaTweet:
                wb.put(pickle.dumps(entry.tid), entry.serialize())
                for entry in entries:
                    wb.put(pickle.dumps(entry.tid), entry.serialize())
            wb.write()
            db.close()

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
    elif content_type == 'text/html':
        ret = strip_text_wight_blank(ret)
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

def strip_text_wight_blank(text):
    text = re.sub(r'(\\r)|(\\n)|(\r)|(\\t)', '', text)
    text = re.sub(r'\\/', '/', text)
    text = re.sub(r'\\"', '"', text)
    text = re.sub('&nbsp;', '', text)
    return text
 

# Html parser

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
            elif "['page_id']" in value:
                user.page_id = value.split("'")[-2]
            elif "['sex']" in value:
                user.gender = value.split("'")[-2]
            elif "['onick']" in value:
                user.nick_name = value.split("'")[-2]
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

def tweet_page_parser(html):
    """
    Returns a list of tweets along with their path. The returned tweet only contains
    its own content.
    """
    tweets = []
    flows = []
    ouidp = re.compile(r'(ouid=([0-9]*))')
    rouidp = re.compile(r'(rouid=([0-9]*))')
    box = BeautifulSoup(html, 'lxml')
    for wrap_box in box.find_all('div', 'WB_cardwrap'):
        if 'mid' not in wrap_box.attrs or wrap_box.find('div', 'WB_cardtitle_b'):
            #Bypass mysterious box
            continue
        tweet_box = wrap_box.find('div', 'WB_detail')
        if tweet_box.find('a', ignore='ignore'):
            continue
        is_forward = wrap_box.attrs.get('isForward', False)
        tweet = SinaTweet()
        tweet.tid = wrap_box.attrs.get('mid', 0)
        tbinfo = wrap_box.attrs.get('tbinfo', '')
        if tbinfo:
            uid = ouidp.match(tbinfo)
            tweet.uid = uid.groups()[1]
        flow = tweet_box_parser(tweet_box, tweet)
        hanle_box = wrap_box.find('div', 'WB_handle')
        tweet_handle_box_parser(hanle_box, tweet)
        if is_forward:
            otweet = SinaTweet()
            otweet.tid = wrap_box.attrs.get('omid', 0)
            if tbinfo:
                ouid = rouidp.match(tbinfo)
                otweet.uid = ouid.groups()[1]
            otweet_box = tweet_box.find('div', 'WB_expand')
            tweet_box_parser(otweet_box, otweet)
            tweet.otid = otweet.tid
            tweet.ouid = otweet.uid
            f = SinaFlow()
            f.a = otweet.tid
            if flow:
                f.b = flow[0].a
            else:
                f.b = tweet.uid
            flow.append(f)
            handle_box = otweet_box.find('div', 'WB_handle')
            tweet_handle_box_parser(handle_box, otweet)
        tweets.append(tweet)
        tweets.append(otweet)
        flows.extend(flow)
    return (tweets, flows)

def tweet_box_parser(box, tweet):
    """
    Parse the input tweet box and fill the field of the input tweet. 

    Return a list Relations of the retweeting path.
    """
    path = []
    from_box = box.find('div', 'WB_from')
    tweet_from_box_parser(from_box, tweet)
    text_box = box.find('div', 'WB_text')
    bypass = False
    for inner in text_box.contents:
        if inner.name == 'img':
            continue
        elif inner.name == 'a':
            bypass = True
            path.append(inner.get_text()[1:])
        elif inner.name is None and not bypass:
            tweet.content += inner
    flows = []
    if path:
        flow = SinaFlow()
        flow.a = path.pop()
        for _ in range(path):
            e = path.pop()
            flow.b = e
            flows.append(flow)
            flow = SinaFlow()
            flow.a = e
        flow.b = tweet.uid
        flows.append(flow)
    return flows

def tweet_from_box_parser(box, tweet):
    for inner in box.find_all('a'):
        _date = inner.attrs.get('date', '')
        if _date:
            tweet.time = int(inner.attrs.get('date')[:11])
            break
        action_type = inner.attrs.get('action_type', '')
        if action_type == 'app_source':
            tweet.platform = inner.get_text()
 

def tweet_handle_box_parser(box, tweet):
    """
    Parse number of retweets, comments, loves.
    """
    p = re.compile('[0-9]+')
    for inner in box.find_all('a'):
        action_type = inner.attrs('action_type', '')
        if action_type == 'fl_forward':
            em = inner.find('em', text=p)
            tweet.num_reposts = int(em.get_text())
        elif action_type == 'fl_comment':
            em = inner.find('em', text=p)
            tweet.num_comments = int(em.get_text())
        elif action_type == 'fl_like':
            em = inner.find('em', text=p)
            tweet.num_loves = int(em.get_text())

def retweet_list_page_parser(html, otid, ouid):
    """
    Return a list tweets along with retweeting relations.
    """
    tweets = []
    flows = []
    uidp = re.compile(r'(id=([0-9]+))')
    box = BeautifulSoup(html, 'lxml')
    for tweet_box in box.find_all(attrs={'action-type':'feed_list_item'}):
        tweet = SinaTweet()
        tweet.tid = int(tweet_box.attrs.get('mid', 0))
        tweet.otid = otid
        tweet.ouid = ouid
        face_box = tweet_box.find('div', 'WB_face')
        uid = uidp.match(face_box.a.attrs.get('usercard', '')
        tweet.uid = int(uid.groups()[1])
        from_box = tweet_box.find('div', 'WB_from')
        tweet_from_box_parser(from_box, tweet)
        text_box = tweet_box.find(attrs={'node-type':'text'})
        bypass = False
        path = []
        for inner in text_box.contents:
            if inner.name == 'img':
                continue
            elif inner.name == 'a':
                bypass = True
                path.append(inner.get_text()[1:])
            elif inner.name is None and not bypass:
                tweet.content += inner
        if path:
            flow = SinaFlow()
            flow.a = ouid
            for _ in range(path):
                e = path.pop()
                flow.b = e
                flows.append(flow)
                flow = SinaFlow()
                flow.a = e
            flow.b = tweet.uid
            flows.append(flow)
        else:
            flow = SinaFlow()
            flow.a = ouid
            flow.b = tweet.uid
        tweets.append(tweet)
    return (tweets, flows)

# Others
def extract_html_from_script(script):
    if '<html>' in script or 'html' not in script:
        return ''
    return script[script.find('div') - 1:-21]
