import msgpack

class File:
    def __init__(self, name, size):
        self.name = name
        self.size = size

class Folder:
    def __init__(self, name, files=[], folders=[]):
        self.name = name
        self.files = {f.name: f for f in files}
        self.folders = {f.name: f for f in folders}

def msgpack_unpack(code, data):
    if code == 21:
        data = msgpack.unpackb(data, encoding='utf-8', ext_hook=msgpack_unpack)
        return Folder(data['name'], data['files'], data['folders'])
    elif code == 81:
        data = msgpack.unpackb(data, encoding='utf-8', ext_hook=msgpack_unpack)
        return File(data['name'], data['size'])
    raise RuntimeError('unknown msgpack extension type %i', code)

def load_data(filename = 'data.msgpack'):
    try:
        with open(filename, 'rb') as f:
            data = msgpack.unpack(f, encoding='utf-8', ext_hook=msgpack_unpack)
        return data['root']
    except Exception as e:
        print('error loading %s' % filename)
        print(e)
        return False

if __name__ == '__main__':
    root = load_data()
    if root != False:
        print('go')

