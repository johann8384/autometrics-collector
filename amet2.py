import os
import Queue
import threading
import urllib2
from pygtail import Pygtail
 
 ## Initial Queue Example from :http://www.blog.pythonlibrary.org/2012/08/01/python-concurrency-an-example-of-a-queue/
########################################################################
class Downloader(threading.Thread):
    """Threaded File Downloader"""
 
    #----------------------------------------------------------------------
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
 
    #----------------------------------------------------------------------
    def run(self):
        while True:
            # gets the url from the queue
            url = self.queue.get()
 
            # download the file
            self.download_file(url)
 
            # send a signal to the queue that the job is done
            self.queue.task_done()
 
    #----------------------------------------------------------------------
    def download_file(self, url):
        """"""
        handle = urllib2.urlopen(url)
        fname = os.path.basename(url)
        with open(fname, "wb") as f:
            while True:
                chunk = handle.read(1024)
                if not chunk: break
                f.write(chunk)
 
#----------------------------------------------------------------------
class LineProcessingError(Exception):
    pass
#----------------------------------------------------------------------
class FetchLines(threading.Thread):
    #----------------------------------------------------------------------
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    #----------------------------------------------------------------------
    def run(self):
        while True:
            # gets the url from the queue
            url = self.queue.get()
 
            # download the file
            self.download_file(url)
 
            # send a signal to the queue that the job is done
            self.queue.task_done()
 
    #----------------------------------------------------------------------
    def download_file(self, url):
        try:
            # Check that the log file exists.
            assert os.path.isfile(LOG_FILE)

            # Check that the offset directory exists.
            assert os.path.isdir(OFFSET_DIR)

            # Check that the offset directory is writeable.
            assert os.access(OFFSET_DIR, os.W_OK)

            # Check that the log file is writeable.
            assert os.access(LOG_FILE, os.R_OK)
        except AssertionError:
            sys.stderr.write('Error: One or more preconditions failed.\n')
            # Exit 13, don't restart tcollector.
            sys.exit(13)

        # If the offset file exists, it had better not be empty.
        if os.path.isfile(OFFSET_FILE):
            try:
                assert os.path.getsize(OFFSET_FILE) > 0
            except AssertionError:
                os.remove(OFFSET_FILE)

        # We're not using paranoid mode in Pygtail for performance.
        maillog = Pygtail(LOG_FILE, offset_file=OFFSET_FILE)

        for line in maillog:
            try:
                process_line(line)
            except LineProcessingError:
                pass

            if STOP:
               # Force the offset file to update before we shutdown.
               # If pygtail is not paranoid=True, this is necessary to ensure
               # that the offset gets written after SIGTERM.
               maillog._update_offset_file()
               break

#----------------------------------------------------------------------
class Event(threading.Thred):
  def process_line(line):
      """ Process a line of the maillog file, writing out metrics. """

      # Grab the timestamp.
      try:
          timestamp = dateutil.parser.parse(line.split()[0]).strftime("%s")
      except IndexError:
          raise LineProcessingError
      except ValueError:
          raise LineProcessingError

      # Check for fields in the log line.
      event = dict(re.findall(r'(\S+)=([^, ]+)', line))
      event_tags = []

      if 'dsn' in event:
          event_tags.append('dsn=%s' % event['dsn'])

      if 'status' in event:
          event_tags.append('status=%s' % event['status'])

      if 'delay' in event:
          try:
              event['delay'] = float(event['delay'])
          except ValueError:
              raise LineProcessingError

      # Print an event metric, with tags.
      print 'postfix.events', timestamp, 1, ' '.join(event_tags)
#----------------------------------------------------------------------
def stop(signum, frame):
    """ Stops processing. """

    global STOP
    STOP = True

def main(urls):
    """
    Run the program
    """
    """ Go forth and write metrics. """

    # Register 'stop' to handle SIGTERM and SIGINT
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    queue = Queue.Queue()
 
    # create a thread pool and give them a queue
    for i in range(5):
        t = Downloader(queue)
        t.setDaemon(True)
        t.start()
 
    # give the queue some data
    for url in urls:
        queue.put(url)
 
    # wait for the queue to finish
    queue.join()
 
if __name__ == "__main__":
    urls = ["http://www.irs.gov/pub/irs-pdf/f1040.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040a.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040ez.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040es.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040sb.pdf"]
    main(urls)
