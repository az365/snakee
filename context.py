from datetime import datetime
import gc

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from utils import arguments as arg
    from loggers import logger_classes
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .streams import stream_classes as sm
    from .connectors import connector_classes as ct
    from .utils import arguments as arg
    from .loggers import logger_classes
    from .schema import schema_classes as sh


DEFAULT_STREAM_CONFIG = dict(
    max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
    tmp_files_template=sm.TMP_FILES_TEMPLATE,
    tmp_files_encoding=sm.TMP_FILES_ENCODING,
)


class SnakeeContext:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SnakeeContext, cls).__new__(cls)
        return cls.instance

    def __init__(
            self,
            stream_config=arg.DEFAULT,
            conn_config=arg.DEFAULT,
            logger=arg.DEFAULT
    ):
        self.logger = arg.undefault(logger, logger_classes.get_logger())
        self.stream_config = arg.undefault(stream_config, DEFAULT_STREAM_CONFIG)
        self.conn_config = arg.undefault(conn_config, dict())
        self.stream_instances = dict()
        self.conn_instances = dict()

        self.sm = sm
        self.ct = ct
        self.sh = sh

    @staticmethod
    def is_context():
        return True

    def get_context(self):
        return self

    def get_logger(self):
        if self.logger is not None:
            return self.logger
        else:
            return logger_classes.get_logger()

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    @staticmethod
    def get_default_instance_name():
        return datetime.now().isoformat()

    def conn(self, conn, name=arg.DEFAULT, check=True, redefine=True, **kwargs):
        name = arg.undefault(name, self.get_default_instance_name())
        conn_object = self.conn_instances.get(name)
        if conn_object:
            if redefine or ct.is_conn(conn):
                self.leave_conn(name, verbose=False)
            else:
                return conn_object
        if ct.is_conn(conn):
            conn_object = conn
        else:
            conn_class = ct.get_class(conn)
            conn_object = conn_class(context=self, **kwargs)
        self.conn_instances[name] = conn_object
        if check and hasattr(conn_object, 'check'):
            conn_object.check()
        return conn_object

    def stream(self, stream_type, name=arg.DEFAULT, check=True, **kwargs):
        name = arg.undefault(name, self.get_default_instance_name())
        if sm.is_stream(stream_type):
            stream_object = stream_type
        else:
            stream_object = sm.stream(stream_type, **kwargs)
        stream_object = stream_object.set_name(
            name,
            register=False,
        ).fill_meta(
            context=self,
            check=check,
            **self.stream_config
        )
        self.stream_instances[name] = stream_object
        return stream_object

    def get(self, name, deep=True):
        if name in self.stream_instances:
            return self.stream_instances[name]
        elif name in self.conn_instances:
            return self.conn_instances[name]
        elif deep:
            for c in self.conn_instances:
                if hasattr(c, 'get_items'):
                    if name in c.get_items():
                        return c.get_items()[name]

    def rename_stream(self, old_name, new_name):
        assert old_name in self.stream_instances, 'Stream must be defined (name {} is not registered)'.format(old_name)
        if new_name != old_name:
            self.stream_instances[new_name] = self.stream_instances.pop(old_name)

    def get_local_storage(self, name='filesystem'):
        local_storage = self.conn_instances.get('name')
        if not local_storage:
            local_storage = ct.LocalStorage(name, context=self)
        return local_storage

    def get_job_folder(self):
        job_folder_obj = self.conn_instances.get('job')
        if job_folder_obj:
            return job_folder_obj
        else:
            job_folder_path = self.stream_config.get('job_folder', '')
            job_folder_obj = ct.LocalFolder(job_folder_path, context=self)
            self.conn_instances['job'] = job_folder_obj
            return job_folder_obj

    def get_tmp_folder(self):
        tmp_folder = self.conn_instances.get('tmp')
        if tmp_folder:
            return tmp_folder
        else:
            tmp_files_template = self.stream_config.get('tmp_files_template')
            if tmp_files_template:
                tmp_folder = ct.LocalFolder(tmp_files_template, context=self)
                self.conn_instances['tmp'] = tmp_folder
                return tmp_folder

    def close_conn(self, name, recursively=False, verbose=True):
        closed_count = 0
        this_conn = self.conn_instances[name]
        closed_count += this_conn.close() or 0
        if recursively and hasattr(this_conn, 'get_links'):
            for link in this_conn.get_links():
                closed_count += link.close() or 0
        if verbose:
            self.log('{} connection(s) closed.'.format(closed_count))
        else:
            return closed_count

    def close_stream(self, name, recursively=False, verbose=True):
        this_stream = self.stream_instances[name]
        closed_stream, closed_links = this_stream.close() or 0
        if recursively and hasattr(this_stream, 'get_links'):
            for link in this_stream.get_links():
                closed_links += link.close() or 0
        if verbose:
            self.log('{} stream(es) and {} link(s) closed.'.format(closed_stream, closed_links))
        else:
            return closed_stream, closed_links

    def leave_conn(self, name, recursively=True, verbose=True):
        if name in self.conn_instances:
            self.close_conn(name, recursively=recursively, verbose=verbose)
            self.conn_instances.pop(name)
            gc.collect()
            if not verbose:
                return 1

    def leave_stream(self, name, recursively=True, verbose=True):
        if name in self.stream_instances:
            self.close_stream(name, recursively=recursively, verbose=verbose)
            self.stream_instances.pop(name)
            gc.collect()
            if not verbose:
                return 1

    def close_all_conns(self, recursively=False, verbose=True):
        closed_count = 0
        for name in self.conn_instances:
            closed_count += self.close_conn(name, recursively=recursively, verbose=False)
        if verbose:
            self.log('{} connection(s) closed.'.format(closed_count))
        else:
            return closed_count

    def close_all_streams(self, recursively=False, verbose=True):
        closed_streams, closed_links = 0, 0
        for name in self.stream_instances:
            closed_streams, closed_links = self.close_stream(name, recursively=recursively)
        if verbose:
            self.log('{} stream(es) and {} link(s) closed.'.format(closed_streams, closed_links))
        else:
            return closed_streams, closed_links

    def close_all(self, verbose=True):
        closed_conns = self.close_all_conns(recursively=True, verbose=False)
        closed_streams, closed_links = self.close_all_streams(recursively=True, verbose=False)
        if verbose:
            self.log('{} conn(s), {} stream(es), {} link(s) closed.'.format(closed_conns, closed_streams, closed_links))
        else:
            return closed_conns, closed_streams, closed_links

    def leave_all_conns(self, recursively=False):
        closed_count = self.close_all_conns(verbose=False)
        left_count = 0
        for name in self.conn_instances.copy():
            left_count += self.leave_conn(name, recursively=recursively, verbose=False)
        self.log('{} connection(s) closed, {} connection(s) left.'.format(closed_count, left_count))

    def leave_all_streams(self, recursively=False):
        closed_streams, closed_links = self.close_all_streams(verbose=False)
        left_count = 0
        for name in self.stream_instances.copy():
            left_count += self.leave_stream(name, recursively=recursively, verbose=False)
        message = '{} stream(es) and {} link(s) closed, {} stream(es) left'
        self.log(message.format(closed_streams, closed_links, left_count))

    def leave_all(self):
        self.close_all(verbose=True)
        self.leave_all_conns(recursively=True)
        self.leave_all_streams(recursively=True)
