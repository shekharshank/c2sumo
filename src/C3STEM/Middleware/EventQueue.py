import Queue

class EventQueue:
	def __init__(self):
		self.queue = Queue.PriorityQueue()
		self.runner = self.Runner()

	def enqueue(self, func, args=[], kwargs={}):
		print("Enqueueing")
		element = self.packCall(func, args, kwargs)
		return self.queue.put(element)
		
	def dequeue(self):
		return self.queue.get()
		
	def packCall(self, func, args, kwargs):
		return (func, args, kwargs)
	
	def runnextjob(self):
		if not self.isempty():
			self.runner.execute(self.dequeue())
		#print("runnextjob")
		
	def isempty(self):
		return self.queue.empty()
	
	class Runner:
			def execute(self, element):
				(func, args, kwargs) = element
				result = func(*args, **kwargs)