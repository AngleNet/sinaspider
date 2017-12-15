
from os.path import join, abspath, dirname, isdir
import os
import requests

from sinaspider.sina_pipeline import *
import sinaspider.sina_login
import sinaspider.services.ttypes

def _get_result_path():
    dir_path = dirname(dirname(abspath(__file__)))
    dir_path = join(dir_path, 'data')
    if not isdir(dir_path):
        os.mkdir(dir_path)
    name = join(dir_path, 'pipeline')
    return name

class FileWriter(PipelineNode):
    def __init__(self, stage):
        PipelineNode.__init__(self, self.__class__.__name__)
        path = _get_result_path() + '.' + str(stage)
        self.file = open(path, 'a+')

    def run(self, client, response):
        if response:
            self.file.write(str(response))
            return response
        else:
            self.file.close()

class SinaPipelineDummy(Pipeline):
    def __init__(self, queue):
        router = Router()
        Pipeline.__init__(self, self.__class__.__name__,
                          router, queue)
        ptrweibo = TrendingWeiboProcessor()
        writer = FileWriter(SinaResponseType.TRENDING_WEIBO)
        ptrweibo.forward(writer)
        prelist = RepostListProcessor()
        writer = FileWriter(SinaResponseType.REPOST_LIST)
        prelist.forward(writer)
        puinfo = UserInfoProcessor()
        writer = FileWriter(SinaResponseType.USER_INFO)
        puinfo.forward(writer)
        puweibo = UserWeiboProcessor()
        writer = FileWriter(SinaResponseType.USER_WEIBO)
        puweibo.forward(writer)
        router.forward(ptrweibo)
        router.forward(prelist)
        router.forward(puinfo)
        router.forward(puweibo)

    
class TestSpiderPipeline:
    pipeline = None
    session = None
    loginer = None
    def setup_class(cls):
        cls.pipeline = SinaPipelineDummy(None)
        cls.pipeline.register_scheduler_client('test')
        cls.session = requests.Session()
        cls.loginer = sinaspider.sina_login.SinaSessionLoginer(cls.session)
        user_identity = sinaspider.services.ttypes.UserIdentity(
            'atropos_spider1@sina.com', 'passward')
        cls.loginer.login(user_identity)
    
    def test_router(self):
        links = ['https://d.weibo.com/',
                'https://weibo.com/p/1005052840177141/info?mod=pedit_more',
                'https://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=100505&is_hot=1&pagebar=0&pl_name=Pl_Official_MyProfileFeed__20&id=1005052840177141&script_uri=/zhenshimeinvtuijian&feed_type=0&page=2&pre_page=1&domain_op=100505&__rnd=1513303863282',
                'https://weibo.com/aj/v6/mblog/info/big?ajwvr=6&id=4185163515138322&max_id=4185211120618742&page=2&__rnd=1513315560511']
        for link in links:
            res = self.session.get(link)
            self.pipeline.start(res)

    def test_trending_weibo_processor(self):
        pass
    
    def test_repost_list_processor(self):
        pass

    def test_user_info_processor(self):
        pass
    
    def test_user_weibo_processor(self):
        pass
    
    def test_user_info_html_parser(self):
        res = self.session.get('https://weibo.com/2876831014/info')
        page = decode_response_text(res)
        user = SinaUser()
        user_info_html_parser(page, user)

    def teardown_class(cls):
        cls.pipeline = None