
import threading
import time

cache_data = {}
cache_version = {}

meta_data_table = {"1": 42}
version_table = {"1": 4}


def fill_cache_metadata(key):
    meta_data = meta_data_table[key]
    print("Filling cache meta data for", meta_data)
    cache_data[key] = meta_data
    
def fill_cache_version(key):
    time.sleep(2)
    version = version_table[key]
    print("Filling cache version data for", version)
    cache_version[key] = version


def invalidate_cache(key, metadata, version):
    try:
        cache_data = cache_data[key][value] ## To produce error
    except:
        drop_cache(key, version)
        

def drop_cache(key, version):
    cache_version_value = cache_version[key]
    if version > cache_version_value:
        cache_data.pop(key)
        cache_version.pop(key)
            


def write_in_databse_transactionally(key, data, version):
    meta_data_table[key] = data
    version_table[key] = version

def read_value_from_cache(key):
    if key in cache_data:
        return cache_data[key]
    else:
        fill_cache_thread = threading.Thread(target=fill_cache(key))
        fill_cache_thread.start()
        return None
        
def fill_cache(key):
    fill_cache_metadata(key)
    fill_cache_version(key)

def read_value(key):
    value = read_value_from_cache(key)
    if value is not None:
        return value
    else:
        return meta_data_table[key]



def write_value(key, value):
    version = 1
    if key in version_table:
        version = version_table[key]
        version = version + 1

    write_in_databse_transactionally(key, value, version)
    time.sleep(3) ## To reproduce the error
    invalidate_cache(key, value, version)

def print_values():
    time.sleep(5)
    print("meta data table:", meta_data_table)
    print("version table", version_table)
    print("cache_data_table", cache_data)
    print("version_cache", cache_version)    


read_thread = threading.Thread(target=read_value, args=("1"))
write_thread = threading.Thread(target=write_value, args=("1",43))
print_thread = threading.Thread(target=print_values)

start = time.time() 

read_thread.start()
write_thread.start()
print_thread.start()



end = time.time()


