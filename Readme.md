# SinaSpider

A simple weibo spider which collects trending post messages.

## Design

## Python
### Testing
* [Pytest](https://docs.pytest.org/en/latest/example/simple.html): Testing framework

    ```
    @pytest.fixture(scope='session', autouse=True)
    def init():
        """
        Initialize for testing. Called before all test starts.

        See https://docs.pytest.org/en/latest/fixture.html
        """
        global initialization
        yield None
        # Tear down
    ```
* [Pytest introduction](http://pythontesting.net/framework/pytest/pytest-introduction/): Pytest tutorial
* [Unit testing](https://jeffknupp.com/blog/2013/12/09/improve-your-python-understanding-unit-testing/): Good python unit test blog
### Coding style
* [Google python style](https://google.github.io/styleguide/pyguide.html)
### Logging
* [Best logging practice](https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/)
* [Logging cookbook]()
### Configuration management
* [4 ways to manage configuration](https://hackernoon.com/4-ways-to-manage-the-configuration-in-python-4623049e841b)
### APIs
* [Requests](http://docs.python-requests.org/en/master/api/)
### Others
* How to export requirements.txt?

    1. using `pipreqs`: `pipreqs /home/project/location`
    2. using `pip freeze`
* Thrift 

    `thrift --out . --gen py:new_style services.thrift`

    Issues:

    1. class has no `__hash__`
    2. need monkey patch `xrange`

* Nice python libs:

    > multiprocessing, threading, concurrent, twisted

* Formatting:
    `autopep8 --in-place --recursive --aggressive`

* When running multiple threads, the whole system will stuck if one thread among them is connected to the thrift server using socket. What is wrong with this?
    
    ```python
    def main():
        client ...
        transport ...
        while True:
            transport.open()
            client.command...
            transport.close()
    
    for i in range(3):
        thread = threading.thread(target=main)
        theadd.start()
    ```
    When we want all 3 threads running parallel, we need to close that transport.

* I tested on a server with 16-core with a configuration of 10 downloaders and 6-process-engine. It can process 35 links/s.

* How to debug multiprocessing programs?
    
    Using log as the following:
    ```
    Thread-1:
        ...
        logger.info('Thread-1 1')
        ...
        logger.info('Thread-1 2')
        ...
        logger.info('Thread-1 3')
    ```
* Singleton:

    ```python
    class SingletonExample(object):
        _instance = None
        def __new__(cls):
            if not SingletonExample._instance:
                SingletonExample._instance = super(SingletonExample, cls).__new__(cls)
                super(SingletonExample, SingletonExample._instance).__init__(SingletonExample._instance)
                # Customization code goes here
            return SingletonExample._instance
        # Never use __init__ to init.
    ```
    Python use `__new__` to create an instance and use `__init__` to customize it.

* [Concurrent practice with Redis](https://eli.thegreenplace.net/2017/concurrent-servers-part-1-introduction/)
