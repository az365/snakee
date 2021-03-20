import gc
from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from base import base_classes as bs
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from utils import arguments as arg
    from loggers import logger_classes as lg
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .base import base_classes as bs
    from .streams import stream_classes as sm
    from .connectors import connector_classes as ct
    from .utils import arguments as arg
    from .loggers import logger_classes as lg
    from .schema import schema_classes as sh

Logger = Union[lg.LoggerInterface, lg.ExtendedLoggerInterface]
Connector = Optional[bs.TreeInterface]
Stream = Optional[sm.StreamInterface]
Child = Union[Logger, Connector, Stream]

NAME = 'cx'
DEFAULT_STREAM_CONFIG = dict(
    max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
    tmp_files_template=sm.TMP_FILES_TEMPLATE,
    tmp_files_encoding=sm.TMP_FILES_ENCODING,
)


class SnakeeContext(bs.AbstractNamed, bs.ContextInterface):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SnakeeContext, cls).__new__(cls)
        return cls.instance

    def __init__(
            self,
            name=arg.DEFAULT,
            stream_config=arg.DEFAULT,
            conn_config=arg.DEFAULT,
            logger=arg.DEFAULT
    ):
        self.logger = logger
        self.stream_config = arg.undefault(stream_config, DEFAULT_STREAM_CONFIG)
        self.conn_config = arg.undefault(conn_config, dict())
        self.stream_instances = dict()
        self.conn_instances = dict()

        name = arg.undefault(name, NAME)
        super().__init__(name)

        self.sm = sm
        self.sm.set_context(self)
        self.ct = ct
        self.ct.set_context(self)
        self.sh = sh

    def set_logger(self, logger: Logger):
        self.logger = logger
        if hasattr(logger, 'get_context'):
            if not logger.get_context():
                if hasattr(logger, 'set_context'):
                    logger.set_context(self)

    def get_logger(self, create_if_not_yet=True) -> Logger:
        if arg.is_defined(self.logger):
            if not self.logger.get_context():
                self.logger.set_context(self)
            return self.logger
        elif create_if_not_yet:
            return lg.get_logger(context=self)

    @staticmethod
    def get_new_selection_logger(name, **kwargs) -> lg.SelectionLoggerInterface:
        return lg.SelectionMessageCollector(name, **kwargs)

    def get_selection_logger(self, name=arg.DEFAULT, **kwargs) -> lg.SelectionLoggerInterface:
        logger = self.get_logger()
        if hasattr(logger, 'get_selection_logger'):
            selection_logger = logger.get_selection_logger(name, **kwargs)
        else:
            selection_logger = None
        if not selection_logger:
            selection_logger = self.get_new_selection_logger(name, **kwargs)
            if hasattr(logger, 'set_selection_logger'):
                logger.set_selection_logger(selection_logger)
        return selection_logger

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def set_parent(self, parent, reset=False, inplace=False):
        assert not reset, 'SnakeeContext is a root object'
        if inplace:
            return self

    def get_items(self) -> Iterable:
        yield from self.conn_instances.items()
        yield from self.stream_instances.items()

    def get_children(self) -> dict:
        return dict(self.get_items())

    def add_child(self, instance: bs.Contextual):
        name = instance.get_name()
        err_msg = 'instance with name {} already registered (got {})'
        if ct.is_conn(instance):
            assert name not in self.conn_instances, err_msg.format(name, instance)
            self.conn_instances[name] = instance
        elif sm.is_stream(instance):
            assert name not in self.stream_instances, err_msg.format(name, instance)
            self.stream_instances[name] = instance
        elif lg.is_logger(instance):
            assert isinstance(instance, lg.LoggerInterface)
            if hasattr(instance, 'is_common_logger'):
                if instance.is_common_logger():
                    self.set_logger(instance)
        elif hasattr(instance, 'is_progress'):
            pass
        else:
            raise TypeError("class {} isn't supported by context".format(instance.__class__.__name__))
        if not instance.get_context():
            instance.set_context(self)

    def conn(self, conn, name=arg.DEFAULT, check=True, redefine=True, **kwargs) -> Connector:
        name = arg.undefault(name, arg.get_generated_name('Connection'))
        conn_object = self.conn_instances.get(name)
        if conn_object:
            if redefine or ct.is_conn(conn):
                self.forget_conn(name, verbose=False)
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

    def stream(self, stream_type, name=arg.DEFAULT, check=True, **kwargs) -> Stream:
        name = arg.undefault(name, arg.get_generated_name('Stream'))
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

    def get_stream(self, name: str) -> Stream:
        return self.stream_instances.get(name)

    def get_connection(self, name: str) -> Connector:
        return self.stream_instances.get(name)

    def get_child(self, name, class_or_type=arg.DEFAULT, deep=True) -> Child:
        if 'Stream' in str(class_or_type):
            return self.get_stream(name)
        elif 'Conn' in str(class_or_type):
            return self.get_connection(name)
        elif 'Logger' in str(class_or_type):
            return self.get_logger()
        elif not arg.is_defined(class_or_type):
            if name in self.stream_instances:
                return self.stream_instances[name]
            elif name in self.conn_instances:
                return self.conn_instances[name]
            elif deep:
                for c in self.conn_instances:
                    if hasattr(c, 'get_children'):
                        return c.get_children().get(name)

    def rename_stream(self, old_name, new_name):
        assert old_name in self.stream_instances, 'Stream must be defined (name {} is not registered)'.format(old_name)
        if new_name != old_name:
            self.stream_instances[new_name] = self.stream_instances.pop(old_name)

    def get_local_storage(self, name='filesystem', create_if_not_yet=True) -> Connector:
        local_storage = self.conn_instances.get(name)
        if local_storage:
            assert isinstance(local_storage, ct.LocalStorage)
        elif create_if_not_yet:
            local_storage = ct.LocalStorage(name, context=self)
        if local_storage:
            self.conn_instances[name] = local_storage
        return local_storage

    def get_job_folder(self) -> Connector:
        job_folder_obj = self.conn_instances.get('job')
        if job_folder_obj:
            return job_folder_obj
        else:
            job_folder_path = self.stream_config.get('job_folder', '')
            job_folder_obj = ct.LocalFolder(job_folder_path, parent=self)
            self.conn_instances['job'] = job_folder_obj
            return job_folder_obj

    def get_tmp_folder(self) -> Connector:
        tmp_folder = self.conn_instances.get('tmp')
        if tmp_folder:
            return tmp_folder
        else:
            tmp_files_template = self.stream_config.get('tmp_files_template')
            if tmp_files_template:
                tmp_folder = ct.LocalFolder(tmp_files_template, parent=self)
                self.conn_instances['tmp'] = tmp_folder
                return tmp_folder

    def close_conn(self, name, recursively=False, verbose=True) -> int:
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

    def close_stream(self, name, recursively=False, verbose=True) -> tuple:
        this_stream = self.stream_instances[name]
        closed_stream, closed_links = this_stream.close() or 0
        if recursively and hasattr(this_stream, 'get_links'):
            for link in this_stream.get_links():
                closed_links += link.close() or 0
        if verbose:
            self.log('{} stream(es) and {} link(s) closed.'.format(closed_stream, closed_links))
        else:
            return closed_stream, closed_links

    def forget_conn(self, name, recursively=True, verbose=True) -> int:
        if name in self.conn_instances:
            self.close_conn(name, recursively=recursively, verbose=verbose)
            self.conn_instances.pop(name)
            gc.collect()
            if not verbose:
                return 1

    def forget_stream(self, name, recursively=True, verbose=True) -> int:
        if name in self.stream_instances:
            self.close_stream(name, recursively=recursively, verbose=verbose)
            self.stream_instances.pop(name)
            gc.collect()
            if not verbose:
                return 1

    def close_all_conns(self, recursively=False, verbose=True) -> int:
        closed_count = 0
        for name in self.conn_instances:
            closed_count += self.close_conn(name, recursively=recursively, verbose=False)
        if verbose:
            self.log('{} connection(s) closed.'.format(closed_count))
        else:
            return closed_count

    def close_all_streams(self, recursively=False, verbose=True) -> tuple:
        closed_streams, closed_links = 0, 0
        for name in self.stream_instances:
            closed_streams, closed_links = self.close_stream(name, recursively=recursively)
        if verbose:
            self.log('{} stream(es) and {} link(s) closed.'.format(closed_streams, closed_links))
        else:
            return closed_streams, closed_links

    def close(self, verbose=True) -> tuple:
        closed_conns = self.close_all_conns(recursively=True, verbose=False)
        closed_streams, closed_links = self.close_all_streams(recursively=True, verbose=False)
        if verbose:
            self.log('{} conn(s), {} stream(es), {} link(s) closed.'.format(closed_conns, closed_streams, closed_links))
        else:
            return closed_conns, closed_streams, closed_links

    def forget_child(
            self,
            name_or_child: Union[bs.Contextual, str],
            recursively=True,
            skip_errors=False,
    ) -> int:
        name, child = self.get_name_and_child(name_or_child)
        if name in self.get_children() or skip_errors:
            child = self.get_children().pop(name)
            if child:
                child.close()
                count = 1
                if recursively:
                    for c in child.get_children():
                        count += c.forget_all_children()
                return count
        else:
            raise TypeError('child {} with name {} not registered'.format(name, child))

    def forget_all_conns(self, recursively=False):
        closed_count = self.close_all_conns(verbose=False)
        left_count = 0
        for name in self.conn_instances.copy():
            left_count += self.forget_conn(name, recursively=recursively, verbose=False)
        self.log('{} connection(s) closed, {} connection(s) left.'.format(closed_count, left_count))

    def forget_all_streams(self, recursively=False):
        closed_streams, closed_links = self.close_all_streams(verbose=False)
        left_count = 0
        for name in self.stream_instances.copy():
            left_count += self.forget_stream(name, recursively=recursively, verbose=False)
        message = '{} stream(es) and {} link(s) closed, {} stream(es) left'
        self.log(message.format(closed_streams, closed_links, left_count))

    def forget_all_children(self):
        self.close(verbose=True)
        self.forget_all_conns(recursively=True)
        self.forget_all_streams(recursively=True)

    def __repr__(self):
        return NAME
