import os
import time
import bisect
import itertools
from datetime import datetime
import logging

default_data_dir = './data/'
default_write_buffer_size = 1024*10
default_read_buffer_size = 1024*10

def ensure_data_dir():
    if not os.path.exists(default_data_dir):
        os.makedirs(default_data_dir)

def init():
    ensure_data_dir()

class WawaIndex:
    def __init__(self, index_name):
        self.fp_index = open(os.path.join(default_data_dir, index_name + '.index'), 'a+')
        self.indexes = []
        self.offsets = [] 

    def __update_index(self, key, offset):
        self.indexes.append(key)
        self.offsets.append(offset)

    def load_index(self):
        self.fp_index.seek(0)
        for line in self.fp_index:
            key, offset  = line.split()
            offset = int(offset)
            self.__update_index(key, offset)

    def append_index(self, key, offset):
        self.__update_index(key, offset)
        index_data = '%s %s %s' % (key, offset, os.linesep)
        self.fp_index.write(index_data)

    def get_offsets(self, begin_key, end_key):
        index_len = len(self.indexes)
        if index_len == 0 : return 0, 0
        left = bisect.bisect_left(self.indexes, str(begin_key))
        right = bisect.bisect_left(self.indexes, str(end_key))
        logging.debug('get_index_range:%s %s %s %s %s %s', self.indexes[0], self.indexes[-1], begin_key, end_key, left, right)
        if left == index_len : left = 0
        if right == index_len : right = 0 
        return self.offsets[left], self.offsets[right] 


class WawaDB:
    def __init__(self, db_name):
        self.db_name = db_name
        self.fp_data_for_append = open(os.path.join(default_data_dir, db_name + '.db'), 'a', default_write_buffer_size)
        self.index1 = WawaIndex(db_name+'1')
        self.index1.load_index()

    def __get_data_by_offsets(self, begin_offset, end_offset):
        fp_data = open(os.path.join(default_data_dir, self.db_name + '.db'), 'r', default_read_buffer_size)
        fp_data.seek(int(begin_offset))
        
        will_read_len, read_len = end_offset - begin_offset, 0
        line = fp_data.readline()
        while line:
            read_len += len(line)
            if read_len >= will_read_len:break
            yield line.rstrip('\r\n')
            line = fp_data.readline()

    def append_data(self, data, record_time=datetime.now()):
        def check_args():
            if not data:
                raise ValueError('data is null') 
            if not isinstance(data, basestring):
                raise ValueError('data is not string') 
            if data.find('\r') != -1 or data.find('\n') != -1:
                raise ValueError('data contains linesep') 

        check_args()

        
        record_time = time.mktime(record_time.timetuple()) 
        data = '%s %s %s' % (record_time, data, os.linesep)
        offset = self.fp_data_for_append.tell()
        self.fp_data_for_append.write(data)
        self.index1.append_index(record_time, offset)

    def get_data(self, begin_time, end_time, data_filter=None):
        def check_args():
            if not (isinstance(begin_time, datetime) and isinstance(end_time, datetime)):
                raise ValueError('begin_time or end_time is not datetime') 

        check_args()

        begin_time, end_time = time.mktime(begin_time.timetuple()), time.mktime(end_time.timetuple()) 
        begin_offset, end_offset = self.index1.get_offsets(begin_time, end_time)

        for data in self.__get_data_by_offsets(begin_offset, end_offset):
            if data_filter:
                if data_filter(data):
                    yield data
            else:
                yield data

def test():
    from datetime import datetime, timedelta
    import uuid, random
    logging.getLogger().setLevel(logging.NOTSET) 

    def time_test(test_name):
        def inner(f):
            def inner2(*args, **kargs):
                start_time = datetime.now()
                result = f(*args, **kargs)
                print '%s take time:%s' % (test_name, (datetime.now() - start_time))
                return result
            return inner2
        return inner

    @time_test('gen_test_data')    
    def gen_test_data(db):
        now = datetime.now()
        begin_time = now - timedelta(hours=5)
        while begin_time < now:
            print begin_time
            for i in range(10000):
                db.append_data(str(random.randint(1,10000))+ ' ' +str(uuid.uuid1()), begin_time)
            begin_time += timedelta(minutes=1)
    
    @time_test('test_get_data')    
    def test_get_data(db):
        begin_time = datetime.now() - timedelta(hours=3) 
        end_time = begin_time + timedelta(minutes=120)
        results = list(db.get_data(begin_time, end_time, lambda x: x.find('1024') != -1))
        print 'test_get_data get %s results' % len(results)

    @time_test('get_db')    
    def get_db():
        return WawaDB('test')

    if not os.path.exists('./data/test.db'):
        db = get_db()
        gen_test_data(db)
  
    db = get_db() 
    test_get_data(db)

init()

if __name__ == '__main__':
    test()
