import asyncio
import time
import threading 

class trackerThread:
    def __init__(self) -> None:
        threading.Thread.__init__(self) 
        self.kill = False 
    
    def run(self):
        while not self.kill:
            print("Here")


class Test:
    def __init__(self):
        self.stop_flag = True 
        self.thread = trackerThread()
        #self.thread.run()
        self.kill = False 
    
    def stop(self):
        self.thread.kill = True 

    def track(self):
        print("WE HERERERE")
        while not self.kill:
            print("Here")



test = Test()
t = threading.Thread(target=test.track)
t.run()
#time.sleep(1)
test.stop()





para = "The cat met another cat."

for word in para.split(' '):

    print(word)
