"""
A simple html processing pipeline.
"""

import logging
import concurrent

from sinaspider.config import *

class PipelineNode(object):
    """
    A pipeline node.  
    
    Subclass should call PipelineNode.__init__() at first.
    """
    def __init__(self, name):
        self.name = name
        self.children = list()
        self.logger = logging.getLogger(self.name)
    
    def forward(self, node):
        """
        Add a child node.
        
        Input:
        - node: A PipelineNode to be added.
        """
        self.children.append(node)
    
    def run(self, *kws, **kwargs):
        """
        Implement this in subclass.
        """
        pass
    
    def start(self, *kws, **kwargs):
        """
        Runs current node.
        """
        (kws, kwargs) = self.run(*kws, **kwargs)
        for child in self.children:
            child.start(*kws, **kwargs)
    

class Pipeline(object):
    """
    A pipeline is composed of a series of pipeline node. Each downloaded page
    is feeded to a pipeline, then sumbit the pipeline to the engine to execute.
    """
    def __init__(self, name, head):
        """
        Input:
        - name: A string of pipeline node.abs
        - head: A PipelineNode executes firstly.
        """
        self.name = name
        self.head = head
        self.logger = logging.getLogger(self.name)
    
    def start(self):
        """
        Start the pipeline.
        """
        self.head.start()


class PipelineEngine(object):
    def __init__(self):
        self.executor = concurrent.futures.ProcessPoolExecutor(
                            max_workers=PIPELINE_CONFIG['engine_pool_size'])
    def submit(self, pipeline):
        """
        Input:
        - pipeline: A Pipeline which represents a processing pipeline
        """
        self.executor.submit(pipeline.start)

class SimplePipelineNode(PipelineNode):
    def __init__(self, response):
        PipelineNode.__init__(self, name=self.__class__.__name__)
        self.response = response
    def run(self):
        self.logger.info('Response: %s' % self.response[:20])
        return ((self.response,), dict())

class SimplePipeline(Pipeline):
    def __init__(self, response):
        head = SimplePipelineNode(response)
        Pipeline.__init__(self, self.__class__.__name__, head)
