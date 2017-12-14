from sinaspider.config import *
from sinaspider.services.ttypes import *
from sinaspider.scheduler import SchedulerServiceHandler


class TestSchedulerServiceHandler:
    def setup_method(self, method):
        self.handler = SchedulerServiceHandler()
        self.handler.init()
        self.downloaders = ['downloader-'+str(i) for i in range(5)]
    
    def teardown_method(self, method):
        self.handler = None

    def test_downloader(self):
        for downloader in self.downloaders:
            assert self.handler.register_downloader(downloader) == RetStatus.SUCCESS
        for downloader in self.downloaders:
            assert self.handler.unregister_downloader(downloader) == RetStatus.SUCCESS
            assert self.handler.unregister_downloader(downloader) == RetStatus.FAILED

    def test_user_identities(self):
        user_identities = set()
        for ident in SCHEDULER_CONFIG['user_identity']:
            ident = UserIdentity(ident['name'], ident['pwd'])
            user_identities.add(ident)
        num_users = len(user_identities)
        num_downloaders = len(self.downloaders)
        for idx in range(num_downloaders, num_users):
            self.downloaders.append('downloader-'+str(idx))
        user_identities_copy = user_identities.copy()
        for idx in range(num_users):
            ident = user_identities_copy.pop()
            assert self.handler.request_user_identity(self.downloaders[idx]) == ident
        for idx in range(num_users, 2*num_users):
            self.downloaders.append('downloader-'+str(idx))
        user_identities_copy = user_identities.copy()
        for idx in range(num_users, int(1.3*num_users)):
            ident = user_identities_copy.pop()
            assert self.handler.request_user_identity(self.downloaders[idx]) == ident
        user_identities_copy = user_identities.copy()
        for idx in range(num_users, int(1.3*num_users)):
            ident = user_identities_copy.pop()
            assert self.handler.resign_user_identity(ident, self.downloaders[idx]) == RetStatus.SUCCESS
        user_identities_copy = user_identities.copy()
        for idx in range(num_users, int(1.3*num_users)):
            ident = user_identities_copy.pop()
            assert self.handler.request_user_identity(self.downloaders[idx]) == ident
            
    def test_links(self):
        linkss = list()
        num_batch = 20
        batch = 5
        for i in range(num_batch):
            links = list()
            for j in range(batch):
                links.append('http://%s' % j)
            linkss.append(links)
        for i in range(int(num_batch/2)):
            assert self.handler.submit_links(linkss[i]) == RetStatus.SUCCESS
        for i in range(int(num_batch/4)):
            links = self.handler.grab_links(batch)
            assert links == linkss[i]
        for i in range(int(num_batch/2), num_batch):
            assert self.handler.submit_links(linkss[i]) == RetStatus.SUCCESS
        for i in range(int(num_batch/4), num_batch):
            assert self.handler.grab_links(batch) == linkss[i]
        assert self.handler.grab_links(batch) == []
    
    def test_proxies(self):
        proxies = list()
        num = 20
        for i in range(num):
            proxy = ProxyAddress('10.10.18.%i' % i, i+8000)
            proxies.append(proxy)
        batch = 4
        for i in range(int(num/batch)):
            assert self.handler.submit_proxies(proxies[batch*i:batch*(i+1)]) == RetStatus.SUCCESS
        num_downloaders = len(self.downloaders)
        for idx in range(num_downloaders, num):
            self.downloaders.append('downloader-'+str(idx))
        _proxies = list()
        for idx in range(int(num/2)):
            proxy = self.handler.request_proxy(self.downloaders[idx])
            _proxies.append(proxy)
            assert proxy in proxies
        for idx in range(int(num/2)):
            proxy = self.handler.resign_proxy(_proxies[idx], self.downloaders[idx])
            assert proxy not in _proxies
            assert proxy in proxies
            

    @staticmethod
    def teardown_class(cls):
        pass