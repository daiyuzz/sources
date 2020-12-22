class PriorityQueue(object):
    """A priority queue implemented using multiple internal queues (typically,
    FIFO queues). The internal queue must implement the following methods:
    PriorityQueue的实现是由一个hash表组成，k是队列等级，v是队列，同时维护一个指针，指向
    最高的队列等级，在这里数值越小，代表优先级越大。
        * push(obj)
        * pop()
        * close()
        * __len__()

    PriorityQueue一共有四个方法，以及一个len的内部方法

    PriorityQueue的实现思想是用hash表存储不同优先级的队列：优先级的数值作为key，数字越小，优先级别越高；队列作为value
    也就是说每个value的队列中每个值优先级是一样的

    那么用hash表这样存储不同优先级的队列，那么如何保证出队列时，pop出对象的优先级最高呢？这里就要使用一个指针，时刻指向hash
    表中key最小的key，这里的指针在程序中就是类变量： self.curprio

    """

    def __init__(self, qfactory, startprios=()):
        """

        Args:
            qfactory: qfactory是队列的工厂类，主要包括queuelib.queue.py下面的FifoDiskQueue、LifoDiskQueue等六个类，分别是在文件、内存、SQLite三种存储介质上的队列和栈类。这个qfactory就是我们上面提到的hash表的value
            startprios: 初始已经存储的优先级。这里需要我们结合1来理解，这个类在初始化的时候，是可以在我们的储存中读取出初始优先级队列的，也就是上面六个不同的类，从文件、SQLite或内存中取出startprios的优先级数据
        """
        # 队列queue由散列表实现，k存储优先级，v储存这一优先级的Queue(根据qfactory的不同)的队列
        self.queues = {}
        # qfactory是队列的工厂类，主要包括queuelib.queue.py下面的FifoDiskQueue、LifoDiskQueue等六个类，分别是在文件、内存、SQLite三种存储介质上的队列和
        # 栈类。这个qfactory就是我们上面提到的hash表的value
        self.qfactory = qfactory
        for p in startprios:
            # 如果qfactory是FifoDiskQueue或LifoDiskSQLite这种磁盘型队列，则从文件夹+“队列”的形式读取，如果内存型队列则没有缓存
            self.queues[p] = self.qfactory(p)
        # curprio 可以看成是一个指针，指向最高优先级的队列桶
        self.curprio = min(startprios) if startprios else None

    def push(self, obj, priority=0):
        """
        入队列
        Args:
            obj: 要插入的对象
            priority: 要插入对象的优先级

        Returns:

        """
        # 如果hash表中没有则赋值
        if priority not in self.queues:
            self.queues[priority] = self.qfactory(priority)
        q = self.queues[priority]
        # 该优先级队列push一个obj
        q.push(obj)  # this may fail (eg. serialization error)
        # 保持指针指向最大优先级
        if self.curprio is None or priority < self.curprio:
            self.curprio = priority

    def pop(self):
        """
        返回最大优先级的对象
        Returns: 最大优先级的对象
        """
        # 如果指针为空，则返回
        if self.curprio is None:
            return
        # 获取指针指向的最大优先级的队列
        q = self.queues[self.curprio]
        # 返回队列数据
        m = q.pop()
        # 如果pop后队列为空，则删除该优先级的队列，同时重新计算指向最大优先级的指针
        if len(q) == 0:
            del self.queues[self.curprio]
            q.close()
            prios = [p for p, q in self.queues.items() if len(q) > 0]
            self.curprio = min(prios) if prios else None
        # 返回pop对象
        return m

    def close(self):
        """
        关闭队列并保存：如果队列长度大于0，返回该优先级
        Returns: 队列不为空的优先级列表

        """
        active = []
        for p, q in self.queues.items():
            if len(q):
                active.append(p)
            q.close()
        return active

    def __len__(self):
        """

        Returns: 每个优先级中队列的长度和

        """

        return sum(len(x) for x in self.queues.values()) if self.queues else 0
