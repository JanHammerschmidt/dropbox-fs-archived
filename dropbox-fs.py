import msgpack, logging, os, stat, time, errno, unicodedata
import llfuse

mountpoint = '/Volumes/dropbox-fs'

log = logging.getLogger() # get root Logger

class DropboxFs(llfuse.Operations):
    def __init__(self, root):
        super().__init__()
        log.info('init')
        self.gid = os.getgid()
        self.uid = os.getuid()
        read_access = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        execute_access = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        self.file_mode = stat.S_IFREG | read_access
        self.dir_mode = stat.S_IFDIR | read_access | execute_access
        self.mount_time = int(time.time() * 1e9)
        self.root_entry = self.construct_entry(llfuse.ROOT_INODE, self.dir_mode, 1, self.mount_time)
        self.inodes = {llfuse.ROOT_INODE: root}
        self.inode = llfuse.ROOT_INODE

    def construct_entry(self, inode, mode, size, time):
        entry = llfuse.EntryAttributes()
        entry.st_ino = inode
        entry.st_mode = mode
        # entry.st_nlink = 1
        entry.st_size = size

        entry.st_uid = self.uid
        entry.st_gid = self.gid

        # entry.st_blocks = 1
        entry.st_atime_ns = time
        entry.st_mtime_ns = time
        entry.st_ctime_ns = time

        return entry

    def getattr(self, inode, ctx=None):
        log.info('getattr %i' % inode)
        if inode == llfuse.ROOT_INODE:
            return self.root_entry
        else:
            try:
                return self.inodes[inode].attr(self)
            except KeyError as e:
                log.warn('access to unknown inode %i during getattr' % inode)
                raise llfuse.FUSEError(errno.ENOENT)

    def lookup(self, parent_inode, name, ctx=None):
        log.info('lookup %i: %s' % (parent_inode, name))
        try:
            return self.inodes[parent_inode].lookup(self, name) # TODO: cache folder during opendir?
        except KeyError as e:
            log.warn('access to unknown inode %i during lookup' % parent_inode)
            raise llfuse.FUSEError(errno.ENOENT)

    def opendir(self, inode, ctx=None):
        log.info('opendir %i' % inode)
        if not inode in self.inodes:
            log.warn('access to unknown inode %i during opendir' % inode)
        # if inode != llfuse.ROOT_INODE:
        #     raise llfuse.FUSEError(errno.ENOENT)
        return inode

    def readdir(self, inode, off): #todo
        log.info('readdir %i/%i' % (inode, off))
        try:
            folder = self.inodes[inode]
        except KeyError as e:
            log.warn('access to unknown inode %i during lookup' % inode)
            raise llfuse.FUSEError(errno.ENOENT)
        items = list(folder.folders.values()) + list(folder.files.values()) # todo: switch to ordereddict?
        for i,f in enumerate(items[off:]):
            self.check_inode(f)
            yield (f.name.encode(), f.attr(self), off+i+1)

    def check_inode(self, f):
        if not 'inode' in f.__dict__:
            self.inode += 1
            f.inode = self.inode
            self.inodes[self.inode] = f

    def statfs(self, ctx): #todo
        sfs = llfuse.StatvfsData()

        sfs.f_bsize = 512
        sfs.f_frsize = 512

        size = 5
        sfs.f_blocks = size // sfs.f_frsize
        sfs.f_bfree = 0
        sfs.f_bavail = sfs.f_bfree

        inodes = len(self.inodes)
        sfs.f_files = inodes
        sfs.f_ffree = 0
        sfs.f_favail = sfs.f_ffree

        return sfs

class File:
    def __init__(self, name, size):
        self.name = name
        self.size = size

    def attr(self, fs):
        return fs.construct_entry(self.inode, fs.file_mode, self.size, fs.mount_time)

class Folder:
    def __init__(self, name, files=[], folders=[]):
        self.name = name
        self.files = {f.name: f for f in files}
        self.folders = {f.name: f for f in folders}

    def attr(self, fs):
        return fs.construct_entry(self.inode, fs.dir_mode, 0, fs.mount_time)

    def lookup(self, fs, name):
        named = unicodedata.normalize('NFC', name.decode())
        f = self.files.get(named) or self.folders.get(named)
        if f is None:
            if not named.startswith('.'):
                log.warn('unsuccessful lookup (%s => %s) in folder %s' % (name, named, self.name))
            raise llfuse.FUSEError(errno.ENOENT)
        fs.check_inode(f)
        return f.attr(fs)

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

def init_logging():
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.WARN)

if __name__ == '__main__':
    init_logging()
    root = load_data()
    if root != False:
        name = 'dropbox-fs'
        fuse_options = set(llfuse.default_options)
        fuse_options.discard('nonempty')  # necessary for osxfuse
        fuse_options.add('fsname=%s' % name)
        fuse_options.add('volname=%s' % name)
        # fuse_options.add('debug')
        # fuse_options.discard('default_permissions')
        # fuse_options.add('defer_permissions')

        try:
            llfuse.init(DropboxFs(root), mountpoint, fuse_options)
        except Exception as e:
            print(str(e))
        else:
            try:
                llfuse.main(workers=1)
            except:
                llfuse.close()
                raise
            llfuse.close()

