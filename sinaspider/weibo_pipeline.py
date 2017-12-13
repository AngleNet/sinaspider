
from sinaspider.pipeline import Pipeline, PipelineNode


class Processor(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        return (response, )


class FileWriter(PipelineNode):
    def __init__(self):
        PipelineNode.__init__(self, self.__class__.__name__)

    def run(self, client, response):
        content = ''
        if response:
            content = "%s: %s" % (response.status_code, response.text[:20])
        content += '\n'
        open('/tmp/writer', 'a+').write(content)


class SpiderPipeline(Pipeline):
    def __init__(self, queue):
        head = Processor()
        Pipeline.__init__(self, self.__class__.__name__,
                          head, queue)
        writer = FileWriter()
        head.forward(writer)
