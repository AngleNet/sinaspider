
import aenum
from bs4 import BeautifulSoup
import json
import logging
import os
from os.path import dirname, join, abspath, isdir
import pickle
import plyvel
import re
import urllib.parse
import time

from sinaspider.pipeline import Pipeline, PipelineNode
from sinaspider.config import  PIPELINE_CONFIG
from sinaspider.downloader import DownloaderType

### Links
_USER_TWEETS_LINKS = {
    'top_page': 'https://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=%s'
                '&is_all=1&page=%s&id=%s&feed_type=0&domain_op=%s&stat_date=%s',
    'mid_page': 'https://www.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=%s'
                '&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=%s'
                '&pre_page=%s&pagebar=%s&id=%s&domain_op=%s&stat_date=%s',
    'bot_page': 'https://www.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=%s'
                '&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=%s'
                '&pre_page=%s&pagebar=%s&id=%s&domain_op=%s&stat_date=%s'
}
_USER_INFO_LINK = 'https://www.weibo.com/p/%s/info?home=%s'
_TRENDING_TWEETS_LINK = 'https://d.weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6'\
                        '&domain=102803_ctg1_1760_-_ctg1_1760&pagebar=0&tab=home'\
                        '&current_page=1&pre_page=1&page=1&pl_name=Pl_Core_NewMixFeed__3'\
                        '&id=102803_ctg1_1760_-_ctg1_1760&script_uri=/&feed_type=1'\
                        '&domain_op=102803_ctg1_1760_-_ctg1_1760'    # Scheduler seed.
_RETWEET_LINKS = 'https://www.weibo.com/aj/v6/mblog/info/big?ajwvr=6&id=%s&page=%s&ouid=%s'
_USER_HOME_LINK = {
    'id': 'https://www.weibo.com/u/%s',
    'nick': 'https://www.weibo.com/n/%s'
}
_TWEET_LONGTEXT_LINK = 'https://weibo.com/p/aj/mblog/getlongtext?ajwvr=6&mid=%s&tweet=%s'
_TOPIC_PAGE_LINK =  'https://d.weibo.com/100803?pids=Pl_Discover_Pt6Rank__5&cfs=920'\
                    '&Pl_Discover_Pt6Rank__5_filter=hothtlist_type=1&Pl_Discover_Pt6Rank__5_page=%s'
_TOPIC_LINK = 'https://weibo.com/p/%s?from=faxian_huati'


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
        self.labels = ''
        self.homepage = ''
        self.num_tweets = 0 
        self.num_followees = 0 # Been followed by the user.
        self.num_fans = 0
        self.vip_level = 0 # 0 indicates not a VIP
        self.page_id = ''
        self.others = dict()
    
    def __repr__(self):
        L = ['%s=%s' % (key, value)
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
        self.num_topics = 0
        self.num_atnames = 0 # Include retweet users.
        self.num_links = 0 # Only external links
        self.num_videos = 0
        self.num_images = 0

    def __repr__(self):
        L = ['%s=%s' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def json(self):
        return json.dumps(self.__dict__)

class SinaTopic(Serializable):
    def __init__(self):
        self.tid = ''
        self.brief = ''
        self.num_reads = 0
        self.num_disscuss = 0
        self.num_fans = 0
        self.name = ''
        self.labels = ''
        self.location = ''

    def __repr__(self):
        L = ['%s=%s' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

class SinaTopicReads(Serializable):
    def __init__(self):
        self.tid = ''
        self.timestamp = 0
        self.num_reads = 0
    
    def __repr__(self):
        L = ['%s=%s' % (key, value)
            for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

class SinaFlow(Serializable):
    def __init__(self):
        """
        A flow from a to b.
        """
        self.tag = ''
        self.a = ''
        self.b = ''
    def __repr__(self):
        L = '[%s]%s-->%s' % (self.tag, self.a, self.b)
        return '%s(%s)' % (self.__class__.__name__, L)


### Pipeline definitions
class SinaResponseType(aenum.Enum):
    TRENDING_WEIBO = 0          # d.weibo.com
    LONG_TEXT_WEIBO = aenum.auto() # weibo.com/p/aj/mblog/getlongtext
    REPOST_LIST = aenum.auto()  # weibo.com/aj/v6/mblog/info/big
    USER_INFO= aenum.auto()     # weibo.com/p/1005052840177141/info 
    USER_WEIBO = aenum.auto()   # weibo.com/p/aj/v6/mblog/mbloglist
    USER_HOME = aenum.auto()    # user homepage. Get page_id
    TRENDING_TOPIC_PAGE = aenum.auto()
    TRENDING_TOPIC = aenum.auto()

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

             +-----+     +---->LongTextWeiboProcessor--->LevelDBWriter
             |     |    /
             |     |   /  +--->TrendingWeiboProcessor--->LevelDBWriter
             |     +--/  / 
             |  R  +----/  +-->UserWeiboProcessor--->LevelDBWriter
    -------->|  O  +------/
     Response|  U  +---------->UserInfoprocessor--->LevelDBWriter
    -------->|  T  +------+
             |  E  +----+   \---->RepostListProcessor--->LevelDBWriter
             |  R  +--+  \
             |     ++  \  \----->UserHomePageProcessor
             |     | \  \
             |     |  \  \------>TrendingTopicPageProcessor--->LevelDBWriter
             +-----+   \
                        \-------->TrendingTopicPageProcessor--->LevelDBWriter

    """
    def __init__(self, queue):
        router = Router()
        Pipeline.__init__(self, self.__class__.__name__,
                          router, queue)
        wtlevdb = LevelDBWriter()
        pltextweibo = LongTextWeiboProcessor()
        pltextweibo.forward(wtlevdb)
        ptrweibo = TrendingWeiboProcessor()
        ptrweibo.forward(wtlevdb)
        prelist = RepostListProcessor()
        prelist.forward(wtlevdb)
        puinfo = UserInfoProcessor()
        puinfo.forward(wtlevdb)
        puweibo = UserWeiboProcessor()
        puweibo.forward(wtlevdb)
        puhome = UserHomePageProcessor()
        ptopic = TrendingTopicProcessor()
        ptopic.forward(wtlevdb)
        pptopic = TrendingTopicPageProcessor()
        pptopic.forward(wtlevdb)
        router.forward(pltextweibo)
        router.forward(ptrweibo)
        router.forward(prelist)
        router.forward(puinfo)
        router.forward(puweibo)
        router.forward(puhome)
        router.forward(ptopic)
        router.forward(pptopic)


class Router(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        url = urllib.parse.urlsplit(response.url)
        response = SinaResponse(SinaResponseType.UNDEFINED, response)
        if url.netloc == 'd.weibo.com':
            if 'mblog/mbloglist' in url.path:
                response.type = SinaResponseType.TRENDING_WEIBO
            elif '100803' in url.path:
                response.type = SinaResponseType.TRENDING_TOPIC_PAGE
        elif 'weibo.com' in url.netloc:
            if 'mblog/getlongtext' in url.path:
                response.type = SinaResponseType.LONG_TEXT_WEIBO
            elif 'mblog/info/big' in url.path:
                response.type = SinaResponseType.REPOST_LIST
            elif '/info' in url.path:
                response.type = SinaResponseType.USER_INFO
            elif '/p/aj/v6/mblog/mbloglist' == url.path:
                response.type = SinaResponseType.USER_WEIBO
            elif 'p/100808' in url.path:
                response.type = SinaResponseType.TRENDING_TOPIC
            elif '/sorry' in url.path: 
                pass # Bypass
            else:
                response.type = SinaResponseType.USER_HOME
        logger.debug('Route %s for %s' % (response.type, response.response.url))
        return (response,)

class UndefinedProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.UNDEFINED:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.warn('Get response: %s' % debug_str_response(response))
        logger.warn('Content: %s' % response.text)
        

class TrendingWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        if response.type != SinaResponseType.TRENDING_WEIBO:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
 
        links = set()
        tweets = []
        flows = []
        try:
            content_json = decode_response_text(response)
            assert type(content_json) == type(dict())
            if content_json['code'] != '100000':
                logger.debug('%s failed.' % response.url)
                client.submit_links(response.url)
                return # Failed, need retry.
            content = strip_text_wight_blank(content_json['data'])
            tweets, ltext_tweets, flows, pages = tweet_page_parser(content)
            for flow in flows:
                if type(flow.a) is int:
                    link = _USER_HOME_LINK['id'] % flow.a
                elif type(flow.a) is str:
                    link = _USER_HOME_LINK['nick'] % urllib.parse.quote(flow.a)
                links.add(link)
                if type(flow.b) is int:
                    link = _USER_HOME_LINK['id'] % flow.b
                elif type(flow.b) is str:
                    link = _USER_HOME_LINK['nick'] % urllib.parse.quote(flow.b)
                links.add(link)
            _links = generate_user_links(tweets)
            for tweet in ltext_tweets:
                tweet.content = ''
                link = _TWEET_LONGTEXT_LINK % (tweet.tid, urllib.parse.quote(tweet.json()))
                links.add(link)
            client.submit_links(links.union(_links))
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
        return (tweets, flows)

class RepostListProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        if response.type != SinaResponseType.REPOST_LIST:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        res_parse = urllib.parse.urlparse(response.url)
        dic_query = urllib.parse.parse_qs(res_parse.query)
        otid = int(dic_query['id'][0])
        ouid = int(dic_query['ouid'][0])
        cnt_page = int(dic_query['page'][0])
        links = set()
        tweets = []
        flows = []
        try:
            content_json = decode_response_text(response)
            assert type(content_json) == type(dict())
            if content_json['code'] != '100000':
                client.submit_links(response.url)
                logger.debug('%s failed.' % response.url)
                return # Failed, need retry.
            total_pages = content_json['data']['page']['totalpage']
            content = strip_text_wight_blank(content_json['data']['html'])
            tweets, flows = retweet_list_page_parser(content, otid, ouid)
            for flow in flows:
                if type(flow.a) is int:
                    link = _USER_HOME_LINK['id'] % flow.a
                elif type(flow.a) is str:
                    link = _USER_HOME_LINK['nick'] % urllib.parse.quote(flow.a)
                links.add(link)
                if type(flow.b) is int:
                    link = _USER_HOME_LINK['id'] % flow.b
                elif type(flow.b) is str:
                    link = _USER_HOME_LINK['nick'] % urllib.parse.quote(flow.b)
                links.add(link)
            for idx in range(2, total_pages+1):
                link = _RETWEET_LINKS % (otid, idx, ouid)
                link.add(link)
            _links = generate_user_links(tweets)
            client.submit_links(links.union(_links))
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
        return (tweets, flows)

class UserHomePageProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_HOME:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        url_home = response.url.split('?')[0]
        try:
            links = set()
            content = decode_response_text(response)
            content = strip_text_wight_blank(content)
            page_id, uid = user_home_config_parser(content)
            if page_id:
                link = _USER_INFO_LINK % (page_id, url_home)
                links.add(link)
                domain = page_id[:6]
                link = _USER_TWEETS_LINKS['top_page'] % (domain, 1, page_id, domain,
                        PIPELINE_CONFIG['user_tweets_date'])
                links.add(link)
                link = _USER_TWEETS_LINKS['mid_page'] % (domain, 1, 1, 0, page_id,
                        domain, PIPELINE_CONFIG['user_tweets_date'])
                links.add(link)
                link = _USER_TWEETS_LINKS['bot_page'] % (domain, 1, 1, 1, page_id,
                        domain, PIPELINE_CONFIG['user_tweets_date'])
                links.add(link)
                client.submit_links(links)
            else:
                logger.warn('%s does not have a page_id field.' % url_home)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))



class UserInfoProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_INFO:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        logger.debug('%s' % response.url)
        res_parse = urllib.parse.urlparse(response.url)
        dic_query = urllib.parse.parse_qs(res_parse.query)
        url_home = dic_query['home'][0]
        users = []
        try:
            content = decode_response_text(response)
            content = strip_text_wight_blank(content)
            user = SinaUser()
            user.homepage = url_home
            user_info_html_parser(content, user)
            users.append(user)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
        return (users, )


class UserWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.USER_WEIBO:
            return None
        response = response.response
        res_parse = urllib.parse.urlparse(response.url)
        dic_query = urllib.parse.parse_qs(res_parse.query)
        domain = int(dic_query['domain'][0])
        page_id= int(dic_query['id'][0])
        cnt_page = int(dic_query['page'][0])
        domain_op = int(dic_query['domain_op'][0])
        page_bar = int(dic_query.get('pagebar', ['-1'])[0])
        tries = int(dic_query.get('_tries', ['1'])[0])

        tweets = []
        flows = []
        logger = logging.getLogger(self.name)
        try:
            links = set()
            content_json = decode_response_text(response)
            assert type(content_json) == type(dict())
            if content_json['code'] != '100000' or content_json['data'].strip() == '':
                logger.debug('%s failed.' % response.url)
                if tries < PIPELINE_CONFIG['link_max_retries']:
                    tries += 1
                    link = '%s&_tries=%s' % (response.url, tries)
                    client.submit_links([link])
                return 
            content = strip_text_wight_blank(content_json['data'])
            paging_info = False
            if cnt_page == 1 and page_bar == 1:
                paging_info = True
            tweets, ltext_tweets, flows, pages = tweet_page_parser(content, paging_info)
            for flow in flows:
                if type(flow.a) is int:
                    link = _USER_HOME_LINK['id'] % flow.a
                elif type(flow.a) is str:
                    link = _USER_HOME_LINK['nick'] % urllib.parse.quote(flow.a)
                links.add(link)
                if type(flow.b) is int:
                    link = _USER_HOME_LINK['id'] % flow.b
                elif type(flow.b) is str:
                    link = _USER_HOME_LINK['nick'] % urllib.parse.quote(flow.b)
                links.add(link)
            for idx in range(2, pages+1):
                link = _USER_TWEETS_LINKS['top_page'] % (domain, idx, page_id, domain,
                        PIPELINE_CONFIG['user_tweets_date'])
                links.add(link)
                link = _USER_TWEETS_LINKS['mid_page'] % (domain, idx, idx, 0, page_id,
                        domain, PIPELINE_CONFIG['user_tweets_date'])
                links.add(link)
                link = _USER_TWEETS_LINKS['bot_page'] % (domain, idx, idx, 1, page_id,
                        domain, PIPELINE_CONFIG['user_tweets_date'])
                links.add(link)
            _links = generate_user_links(tweets)
            for tweet in ltext_tweets:
                link = _TWEET_LONGTEXT_LINK % (tweet.tid, urllib.parse.quote(tweet.json()))
                links.add(link)
            client.submit_links(links.union(_links))
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
            response = None # Exiting
        return (tweets, flows)

class LongTextWeiboProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.LONG_TEXT_WEIBO:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        res_parse = urllib.parse.urlparse(response.url)
        dict_query = urllib.parse.parse_qs(res_parse.query)
        tweet = dict_query.get('tweet', [''])[0]
        tries = int(dict_query.get('_tries', ['1'])[0])
        if tweet:
            tweet = json.loads(urllib.parse.unquote(tweet))
            _tweet = SinaTweet()
            for key, value in tweet.items():
                if key in _tweet.__dict__:
                    _tweet.__dict__[key] = value
            try:
                content_json = decode_response_text(response)
                assert type(content_json) == dict
                if content_json['code'] != '100000' or content_json['data']['html'].strip() == '':
                    logger.debug('%s failed.' % response.url)
                    if tries < PIPELINE_CONFIG['link_max_retries']:
                        tries += 1
                        link = '%s&_tries=%s' % (response.url, tries)
                        client.submit_links([link])
                    return # Failed, need retry.
                content = strip_text_wight_blank(content_json['data']['html'])
                box = BeautifulSoup(content, 'lxml')
                _tweet.num_topics = 0
                _tweet.num_videos = 0
                _tweet.num_links = 0
                _tweet.num_atnames = 0
                for inner in box.contents:
                    if inner.name == 'img':
                        continue # Emoij
                    elif inner.name == 'a':
                        _type = inner.attrs.get('extra-data', '')
                        __type = inner.attrs.get('action-type', '')
                        if 'topic' in _type:
                            _tweet.num_topics += 1
                            _tweet.content += inner.get_text()
                        elif 'atname' in _type:
                            tweet.num_atnames += 1
                        elif 'feed_list_url' in __type:
                            if '视频' in inner.get_text():
                                _tweet.num_videos += 1
                            else:
                                _tweet.num_links += 1 
                            _tweet.content += inner.get_text()
                        else:
                            logger = logging.getLogger()
                            logger.warn('Missed tweet text: %s' % inner)
                    elif inner.name is None:
                        _tweet.content += inner
                return ([_tweet],)
            except Exception:
                logger.exception('Exception while handling %s' % debug_str_response(response))
        return (list(),)

class TrendingTopicPageProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.TRENDING_TOPIC_PAGE:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        topics = list()
        try:
            content = decode_response_text(response)
            content = strip_text_wight_blank(content)
            topics = topic_page_parser(content)
            links = set()
            for topic in topics:
                link = _TOPIC_LINK % topic.tid
                links.add(link)
            client.submit_links(links, DownloaderType.TOPIC_DOWNLOADER)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
        return (topics, )

class TrendingTopicProcessor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
    
    def run(self, client, response):
        if response.type != SinaResponseType.TRENDING_TOPIC:
            return None
        response = response.response
        logger = logging.getLogger(self.name)
        logger.debug('Get response: %s' % debug_str_response(response))
        topics = list()
        try:
            content = decode_response_text(response)
            content = strip_text_wight_blank(content)
            topic = SinaTopic()
            topic_parser(content, topic)
            topics.append(topic)
        except Exception:
            logger.exception('Exception while handling %s' % debug_str_response(response))
        return (topics, )


class LevelDBWriter(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)
        self.db_dir = join(dirname(dirname(abspath(__file__))), 'database')
        if not isdir(self.db_dir):
            os.makedirs(self.db_dir)
        self.db_name_map = {
            'SinaTweet': 'tweets.db',
            'SinaUser': 'users.db',
            'SinaFlow': 'flows.db',
            'SinaTopic': 'topics.db',
            'SinaTopicReads': 'topics.db'
        }
    
    def run(self, client, *kws):
        if not kws:
            return
        logger = logging.getLogger(self.name)
        db = None
        for entries in kws:
            if entries is None or len(entries) == 0:
                continue
            for _ in range(PIPELINE_CONFIG['leveldb_max_retries']):
                try:
                    logger.debug('Writing: %s' % entries)
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
                    elif type(entry) is SinaTopic:
                        wb.put(pickle.dumps(('topic'+entry.tid)), entry.serialize())
                        for entry in entries:
                            wb.put(pickle.dumps(('topic'+entry.tid)), entry.serialize())
                    elif type(entry) is SinaTopicReads:
                        wb.put(pickle.dumps('reads%s%s' % (entry.timestamp, entry.tid)), entry.serialize())
                        for entry in entries:
                            wb.put(pickle.dumps('reads%s%s' % (entry.timestamp, entry.tid)), entry.serialize())
                    wb.write()
                    db.close()
                    break
                except plyvel._plyvel.IOError:
                    logger.error('Error during writing %s, retry later.' % entries)
                    time.sleep(PIPELINE_CONFIG['leveldb_retry_delay'])
                except Exception:
                    logger.exception('Error while writing %s to LevelDB' % entries)
                    break
                finally:
                    if db and not db.closed:
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

def user_home_config_parser(html):
    """
    Return page_id, uid
    """
    page_id = ''
    uid = ''
    config_box = ''
    for script in BeautifulSoup(html, 'lxml').find_all('script'):
        if "$CONFIG['page_id']" in str(script):
            config_box = script.contents[0]
            break
    if config_box:
        for value in config_box.split(';'):
            if "['page_id']" in value:
                page_id = value.split("'")[-2]
            elif "['oid']" in value:
                uid = value.split("'")[-2]
    return (page_id, uid)
 
def user_info_html_parser(html, user):
    config_box = ''
    number_box = ''
    info_box = ''
    level_box = ''
    for script in BeautifulSoup(html, 'lxml').find_all('script'):
        if "$CONFIG['page_id']" in str(script):
            config_box = script.contents[0]
        elif '"domid":"Pl_Core_T8CustomTriColumn__' in str(script):
            number_box = str(script)
        elif '"domid":"Pl_Core_UserInfo__' in str(script) or \
            '"domid":"Pl_Official_PersonalInfo__' in str(script):
            info_box = str(script)
        elif '"domid":"Pl_Official_RightGrowNew__' in str(script):
            level_box = str(script)
    if config_box:
        for value in config_box.split(';'):
            if "['oid']" in value:
                user.uid = int(value.split("'")[-2])
            elif "['page_id']" in value:
                user.page_id = value.split("'")[-2]
            elif "['onick']" in value:
                user.nick_name = value.split("'")[-2]
    if number_box:
        number_box = extract_html_from_script(number_box)
        number_box = BeautifulSoup(number_box, 'lxml')
        for box in number_box.find_all('td', 'S_line1'):
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
        info_box = BeautifulSoup(info_box, 'lxml')
        for box in info_box.find_all('li', 'li_1'):
            title_box = box.find('span', 'pt_title')
            detail_box = box.find('span', 'pt_detail')
            if not title_box or not detail_box:
                continue
            title = title_box.get_text(strip=True)
            detail = detail_box.get_text('|', strip=True)
            if '昵称' in title:
                user.nick_name = detail
            elif '所在地' in title:
                user.location = detail
            elif '性别' in title:
                user.gender = detail
            elif '简介' in title:
                user.brief = detail
            elif '个性域名' in title:
                user.homepage = detail
            elif '标签' in title:
                user.labels = detail
            else:
                user.others[title] = detail
    if level_box:
        level_box = extract_html_from_script(level_box)
        level_box = BeautifulSoup(level_box, 'lxml')
        level_box = level_box.find('div', 'level_box')
        if level_box:
            title = level_box.find('a')
            if title:
                user.vip_level = int(title.get_text()[3:])
                

def tweet_page_parser(html, paging_info=False):
    """
    Returns a list of tweets along with their path. The returned tweet only contains
    its own content.
    """
    tweets = []
    ltext_tweets = []
    flows = []
    pages = 0
    ouidp = re.compile(r'(ouid=([0-9]*))')
    rouidp = re.compile(r'.*(rouid=([0-9]*))')
    box = BeautifulSoup(html, 'lxml')
    for wrap_box in box.find_all('div', 'WB_cardwrap'):
        if paging_info:
            node_type = wrap_box.attrs.get('node-type', '')
            if node_type == 'feed_list_page':
                pages = paging_info_parser(wrap_box)
        if 'mid' not in wrap_box.attrs or wrap_box.find('div', 'WB_cardtitle_b'):
            #Bypass mysterious box
            continue
        tweet_box = wrap_box.find('div', 'WB_detail')
        if tweet_box.find('a', ignore='ignore'):
            continue
        is_forward = wrap_box.attrs.get('isforward', '')
        if is_forward == '':
            is_forward = wrap_box.attrs.get('isForward', '')
        tweet = SinaTweet()
        tweet.tid = int(wrap_box.attrs.get('mid', 0))
        tbinfo = wrap_box.attrs.get('tbinfo', '')
        if tbinfo:
            uid = ouidp.match(tbinfo)
            tweet.uid = int(uid.groups()[1])
        flow, is_ltext_tweet = tweet_box_parser(tweet_box, tweet)
        handle_box = wrap_box.find('div', 'WB_feed_handle')
        handle_box = handle_box.find('div', 'WB_handle')
        tweet_handle_box_parser(handle_box, tweet)
        if is_forward == '1':
            otweet = SinaTweet()
            otweet.tid = int(wrap_box.attrs.get('omid', 0))
            if tbinfo:
                ouid = rouidp.match(tbinfo)
                if ouid is None:
                    continue# Weibo already deleted.
                otweet.uid = int(ouid.groups()[1])
            otweet_box = tweet_box.find('div', 'WB_expand')
            _, is_ltext_otweet = tweet_box_parser(otweet_box, otweet)
            tweet.otid = otweet.tid
            tweet.ouid = otweet.uid
            f = SinaFlow()
            f.a = otweet.uid
            if flow:
                f.b = flow[0].a
            else:
                f.b = tweet.uid
            flow.append(f)
            for f in flow:
                f.tag = otweet.tid
            handle_box = otweet_box.find('div', 'WB_handle')
            tweet_handle_box_parser(handle_box, otweet)
            if is_ltext_otweet:
                ltext_tweets.append(otweet)
            else:
                tweets.append(otweet)
        if is_ltext_tweet:
            ltext_tweets.append(tweet)
        else:
            tweets.append(tweet)
        flows.extend(flow)
    return (tweets, ltext_tweets, flows, pages)

def tweet_box_parser(box, tweet):
    """
    Parse the input tweet box and fill the field of the input tweet. 

    Return a list Relations of the retweeting path.
    """
    path = []
    from_box = box.find('div', 'WB_from')
    tweet_from_box_parser(from_box, tweet)
    media_boxes = box.find_all('div', 'WB_media_wrap')
    if media_boxes:
        for media_box in media_boxes:
            for inner in media_box.find_all('li', 'WB_pic'):
                tweet.num_images += 1
    text_box = box.find('div', 'WB_text')
    bypass = False
    is_long_text = False
    for inner in text_box.contents:
        if inner.name == 'img':
            continue # Emoij
        elif inner.name == 'a':
            _type = inner.attrs.get('extra-data', '')
            __type = inner.attrs.get('action-type', '')
            if 'topic' in _type and not bypass:
                tweet.num_topics += 1
                tweet.content += inner.get_text()
            elif 'atname' in _type:
                tweet.num_atnames += 1
                if len(tweet.content) >= 2 and tweet.content[-2:] == '//':
                    bypass = True
                    path.append(inner.get_text()[1:])
                    continue
            elif 'feed_list_url' in __type and not bypass:
                if '视频' in inner.get_text():
                    tweet.num_videos += 1
                else:
                    tweet.num_links += 1 
                tweet.content += inner.get_text()
            elif 'fl_unfold' in __type and not bypass:
                is_long_text = True
            else:
                logger = logging.getLogger()
                logger.warn('Missed tweet text: %s' % inner)
        elif inner.name is None and not bypass:
            tweet.content += inner
    tweet.content = tweet.content.strip()
    flows = []
    if path:
        flow = SinaFlow()
        flow.a = path.pop()
        for _ in range(len(path)):
            e = path.pop()
            flow.b = e
            flows.append(flow)
            flow = SinaFlow()
            flow.a = e
        flow.b = tweet.uid
        flows.append(flow)
    return (flows, is_long_text)

def tweet_from_box_parser(box, tweet):
    for inner in box.find_all('a'):
        _date = inner.attrs.get('date', '')
        if _date:
            tweet.time = int(inner.attrs.get('date')[:11])
        action_type = inner.attrs.get('action-type', '')
        if action_type == 'app_source':
            tweet.platform = inner.get_text()
 
def tweet_handle_box_parser(box, tweet):
    """
    Parse number of retweets, comments, loves.
    """
    p = re.compile('[0-9]+')
    for inner in box.find_all('a'):
        action_type = inner.attrs.get('action-type', '')
        _action_suda = inner.attrs.get('suda-uatrack', '')
        em = inner.find('em', text=p)
        if action_type == 'fl_like' and em:
            tweet.num_loves = int(em.get_text())
        elif (action_type == 'fl_forward' or 'transfer' in _action_suda) and em:
            tweet.num_reposts = int(em.get_text())
        elif (action_type == 'fl_comment' or 'comment' in _action_suda) and em:
            tweet.num_comments = int(em.get_text())

def paging_info_parser(box):
    pages = 0
    for page_box in box.find_all(attrs={'bpfilter': 'page'}):
        link = page_box.get('href', '')
        if link == '':
            continue
        url_parse = urllib.parse.urlparse(link)
        url_query = urllib.parse.parse_qs(url_parse.query)
        page = int(url_query.get('page', ['0'])[0])
        pages = max(pages, page)
    return pages

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
        uid = uidp.match(face_box.a.attrs.get('usercard', ''))
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

# Trending topics
def topic_page_parser(html):
    """
    Return a list of links of topics.
    """
    pidx = re.compile('.*/(.*)\?.*')
    timestamp = round(time.time())
    topics = list()
    topics_box = ''
    for script in BeautifulSoup(html, 'lxml').find_all('script'):
        if '"domid":"Pl_Discover_Pt6Rank__5"' in str(script):
            topics_box = str(script)
            break
    topics_box = extract_html_from_script(topics_box)
    topics_box = BeautifulSoup(topics_box, 'lxml')
    for topic_box in topics_box.find_all('div', 'pic_txt'):
        topic = SinaTopicReads()
        topic.timestamp = timestamp
        link_box = topic_box.find('div', 'pic_box')
        if link_box:
            m = pidx.match(link_box.a.attrs['href'])
            if m:
                topic.tid = m.groups()[0]
        if topic.tid == '':
            break
        for box in topic_box.find_all('div', 'sub_box'):
            if '阅读数' in box.get_text():
                topic.num_reads = wordsToNum(box.span.span.get_text())
        topics.append(topic)
    return topics

def topic_parser(html, topic):
    soup = BeautifulSoup(html, 'lxml')
    desc_box = soup.find('meta', attrs={'name': 'description'})
    if desc_box:
        topic.brief = desc_box.attrs['content']
    config_box = ''
    number_box = ''
    lable_box = ''
    for script in soup.find_all('script'):
        if "$CONFIG['page_id']" in str(script):
            config_box = script.contents[0]
        elif '"domid":"Pl_Core_T8CustomTriColumn' in str(script):
            number_box = str(script)
        elif '"domid":"Pl_Core_T5MultiText' in str(script):
            lable_box = str(script)
    if config_box:
        for value in config_box.split(';'):
            if "['page_id']" in value:
                topic.tid = value.split("'")[-2]
            elif "['onick']" in value:
                topic.name = value.split("'")[-2]
    if number_box:
        number_box = extract_html_from_script(number_box)
        number_box = BeautifulSoup(number_box, 'lxml')
        for box in number_box.find_all('td', 'S_line1'):
            name = box.span.contents[0].strip()
            number = box.strong.contents[0].strip()
            if name == '阅读':
                topic.num_reads = wordsToNum(number)
            elif name == '讨论':
                topic.num_disscuss = wordsToNum(number)
            elif name == '粉丝':
                topic.num_fans = wordsToNum(number)
    if lable_box:
        lable_box = extract_html_from_script(lable_box)
        lable_box = BeautifulSoup(lable_box, 'lxml')
        for box in lable_box.find_all('li', 'li_1'):
            title_box = box.find('span', 'pt_title')
            detail_box = box.find('span', 'pt_detail')
            if not title_box or not detail_box:
                continue
            title = title_box.get_text(strip=True)
            detail = detail_box.get_text('|', strip=True)
            if '分类' in title:
                topic.labels = detail
            elif '地区' in title:
                topic.location = detail
 
# Others
def extract_html_from_script(script):
    if '<html>' in script or 'html' not in script:
        return ''
    return script[script.find('div') - 1:-21]

def generate_user_links(tweets):
    links = set()
    for tweet in tweets:
        link = _USER_HOME_LINK['id'] % tweet.uid
        links.add(link)
    return links

def wordsToNum(s):
    if s == '':
        return 0 
    s = s.strip()
    if s[-1] == '亿':
        return round(float(s[0:-1]) * 100000000)
    elif s[-1] == '万':
        return round(float(s[0:-1]) * 10000)
    else:
        return round(float(s))
