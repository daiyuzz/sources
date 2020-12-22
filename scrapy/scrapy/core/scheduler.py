import os
import json
import logging
from os.path import join, exists

from scrapy.utils.misc import load_object, create_instance
from scrapy.utils.job import job_dir

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Scrapy Scheduler. It allows to enqueue requests and then get
    a next request to download. Scheduler is also handling duplication
    filtering, via dupefilter.



    Prioritization and queueing is not performed by the Scheduler.
    User sets ``priority`` field for each Request, and a PriorityQueue
    (defined by :setting:`SCHEDULER_PRIORITY_QUEUE`) uses these priorities
    to dequeue requests in a desired order.

    Scheduler uses two PriorityQueue instances, configured to work in-memory
    and on-disk (optional). When on-disk queue is present, it is used by
    default, and an in-memory queue is used as a fallback for cases where
    a disk queue can't handle a request (can't serialize it).

    :setting:`SCHEDULER_MEMORY_QUEUE` and
    :setting:`SCHEDULER_DISK_QUEUE` allow to specify lower-level queue classes
    which PriorityQueue instances would be instantiated with, to keep requests
    on disk and in memory respectively.

    Overall, Scheduler is an object which holds several PriorityQueue instances
    (in-memory and on-disk) and implements fallback logic for them.
    Also, it handles dupefilters.
    """

    def __init__(self, dupefilter, jobdir=None, dqclass=None, mqclass=None,
                 logunser=False, stats=None, pqclass=None, crawler=None):

        self.df = dupefilter  # url 去重器
        self.dqdir = self._dqdir(jobdir)  # 磁盘队列的工作目录
        self.pqclass = pqclass  # 优先队列类名
        self.dqclass = dqclass  # 磁盘队列类名
        self.mqclass = mqclass  # 内存队列类名
        self.logunser = logunser  # 是否记录日志请求
        self.stats = stats  #
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        """
        类方法，按照配置文件 settings 中的配置项，生成调度器实例
        Args:
            crawler:爬虫类

        Returns:实例化调度器(调用__init__方法)

        """
        settings = crawler.settings
        # 链接去重器 scrapy.dupefilters.RFPDupeFilter
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])
        dupefilter = create_instance(dupefilter_cls, settings, crawler) # 创建一个url过滤器实例
        # 优先队列类 queuelib.priorityQueue 按照请求的优先级进行堆操作
        pqclass = load_object(settings['SCHEDULER_PRIORITY_QUEUE'])
        # 磁盘队列类 scrapy.squeues.PickeLifoDiskQueue
        dqclass = load_object(settings['SCHEDULER_DISK_QUEUE'])
        # 内存队列类 scrapy.squeues.LifoMemoryQueue
        mqclass = load_object(settings['SCHEDULER_MEMORY_QUEUE'])
        # 日志
        logunser = settings.getbool('SCHEDULER_DEBUG')
        return cls(dupefilter, jobdir=job_dir(settings), logunser=logunser,
                   stats=crawler.stats, pqclass=pqclass, dqclass=dqclass,
                   mqclass=mqclass, crawler=crawler)

    def has_pending_requests(self):
        """
        判断调度器中是否为空
        Returns:

        """
        return len(self) > 0

    def open(self, spider):
        """
        实例化内存队列、磁盘队列、url去重器
        Args:
            spider: 自定义爬虫

        Returns: url去重器是否开启成功

        """

        self.spider = spider
        # 优先内存队列实例
        self.mqs = self._mq()
        # 优先磁盘队列实例，如果在启动时候设置了JOBDIR，则激活磁盘队列实例，否则设置None
        self.dqs = self._dq() if self.dqdir else None
        return self.df.open()

    def close(self, reason):
        """
        关闭磁盘队列
        Args:
            reason:

        Returns: url去重器是否关闭成功

        """
        if self.dqs:
            state = self.dqs.close()
            self._write_dqs_state(self.dqdir, state)
        return self.df.close(reason)

    def enqueue_request(self, request):
        """
        入队列，pqclass优先队列，应该是按照请求的优先级别进行堆操作，而mqclass应该是内存的队列，当队列较多时，将请求数据同步到dqclass磁盘队列。
        Args:
            request: 请求

        Returns: True

        """
        # 如果请求需要去重，并且链接去重器中有该请求，那么不需要进行请求
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        # 磁盘优先队列去重
        dqok = self._dqpush(request)
        if dqok:
            # 记录日志
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            # 用内存优先队列去重
            self._mqpush(request)
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True


    def next_request(self):
        """
        出队列
        Returns:

        """
        request = self.mqs.pop()
        if request:
            self.stats.inc_value('scheduler/dequeued/memory', spider=self.spider)
        else:
            request = self._dqpop()
            if request:
                self.stats.inc_value('scheduler/dequeued/disk', spider=self.spider)
        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)
        return request

    def __len__(self):
        return len(self.dqs) + len(self.mqs) if self.dqs else len(self.mqs)

    def _dqpush(self, request):
        if self.dqs is None:
            return
        try:
            self.dqs.push(request)
        except ValueError as e:  # non serializable request
            if self.logunser:
                msg = ("Unable to serialize request: %(request)s - reason:"
                       " %(reason)s - no more unserializable requests will be"
                       " logged (stats being collected)")
                logger.warning(msg, {'request': request, 'reason': e},
                               exc_info=True, extra={'spider': self.spider})
                self.logunser = False
            self.stats.inc_value('scheduler/unserializable',
                                 spider=self.spider)
            return
        else:
            return True

    def _mqpush(self, request):
        self.mqs.push(request)

    def _dqpop(self):
        if self.dqs:
            return self.dqs.pop()

    def _mq(self):
        """ Create a new priority queue instance, with in-memory storage """
        return create_instance(self.pqclass,
                               settings=None,
                               crawler=self.crawler,
                               downstream_queue_cls=self.mqclass,
                               key='')

    def _dq(self):
        """ Create a new priority queue instance, with disk storage """
        state = self._read_dqs_state(self.dqdir)
        q = create_instance(self.pqclass,
                            settings=None,
                            crawler=self.crawler,
                            downstream_queue_cls=self.dqclass,
                            key=self.dqdir,
                            startprios=state)
        if q:
            logger.info("Resuming crawl (%(queuesize)d requests scheduled)",
                        {'queuesize': len(q)}, extra={'spider': self.spider})
        return q

    def _dqdir(self, jobdir):
        """ Return a folder name to keep disk queue state at """
        if jobdir:
            dqdir = join(jobdir, 'requests.queue')
            if not exists(dqdir):
                os.makedirs(dqdir)
            return dqdir

    def _read_dqs_state(self, dqdir):
        path = join(dqdir, 'active.json')
        if not exists(path):
            return ()
        with open(path) as f:
            return json.load(f)

    def _write_dqs_state(self, dqdir, state):
        with open(join(dqdir, 'active.json'), 'w') as f:
            json.dump(state, f)
