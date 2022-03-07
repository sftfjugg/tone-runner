import heapq


class PriorityQueue(object):

    def __init__(self, size=10, reverse=False):
        self.queue = []
        self._index = 0
        self._size = size
        self._reverse = reverse  # 默认最小堆，reverse为True时为最大堆

    def push(self, item, priority):
        if self._reverse:
            priority = -priority
        while self.qsize() >= self._size:
            self.pop()
        else:
            heapq.heappush(self.queue, (priority, self._index, item))
        self._index += 1

    def pop(self):
        if self.empty():
            return None
        return heapq.heappop(self.queue)[-1]

    def qsize(self):
        return len(self.queue)

    def reset_size(self, _size):
        self._size = _size

    def empty(self):
        return True if not self.queue else False
