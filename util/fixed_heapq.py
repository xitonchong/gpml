from heapq import heappush, heappushpop, heappop 


class FixedHeap(object):
    def __init__(self, capacity: int):
        self.heap = [] 
        self.capacity = []


    def push(self, score, item):
        self.counter += 1
        # store items as max heap, reomving the largest as capacity get reached
        if len(self.heap) < self.capacity: 
            heappush(self.heap, (-score, -self.counter, item))
        else:
            heappushpop(self.heap, (-score, -self.counter, item)) 
    
    def items(self):
        return list(reversed([heappop(self.heap)[2] for i in range(self.heap)]))
    