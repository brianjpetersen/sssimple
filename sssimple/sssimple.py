# standard libraries
import os
import queue
import posixpath
import contextlib
import abc
# third party libraries
import boto3
import botocore
# first party libraries
pass


__where__ = os.path.dirname(os.path.abspath(__file__))


class Bucket:
    
    def __init__(self, name, access_key_id=None, secret_access_key=None, 
                 prefix='/', region='us-east-1', connection=None):
        if connection is None:
            if access_key_id is None:
                raise ValueError
            if secret_access_key is None:
                raise ValueError
            connection = self.connect(access_key_id, secret_access_key, region)
        self.connection = connection
        self.name = name
        self._bucket = self.connection.Bucket(name)
        self.prefix = prefix
    
    @staticmethod
    def connect(access_key_id, secret_access_key, region):
        session = boto3.session.Session(
            aws_access_key_id=access_key_id, 
            aws_secret_access_key=secret_access_key,
            region_name=region,
        )
        resource = session.resource('s3')
        return resource
    
    @classmethod
    def create(cls, name, access_key_id, secret_access_key, acl='private', 
               prefix='/', region='us-east-1'):
        connection = cls.connect(access_key_id, secret_access_key, region)
        connection.create_bucket(Bucket=name, ACL=acl)
        return cls(name=name, prefix=prefix, connection=connection)
    
    def get_response(self, key):
        path = posixpath.join(self.prefix.lstrip('/'), key.lstrip('/'))
        try:
            response = self._bucket.Object(path).get()
        except botocore.exceptions.ClientError as exception:
            message, = exception.args
            if 'NoSuchKey' in message:
                raise KeyError
            else:
                raise
        return response
    
    def __repr__(self):
        return '{}.{}({}, prefix={})'.format(
            self.__module__,
            self.__class__.__name__,
            repr(self.name),
            repr(self.prefix)
        )
    
    def __iter__(self):
        for response in self._bucket.objects.all():
            yield response.key
    
    def items(self):
        for key in self:
            yield key, self[key]
    
    def values(self):
        for key in self:
            yield self[key]
    
    def keys(self):
        for key in self:
            yield key
    
    def get(self, key, value=None):
        try:
            value = self[key]
        except:
            pass
        return value
    
    def __delitem__(self, key):
        self._bucket.delete_object(key)
    
    def __getitem__(self, key):
        response = self.get_response(key)
        item = response['Body'].read()
        return item
    
    def set(self, key, value):
        path = posixpath.join(self.prefix.lstrip('/'), key.lstrip('/'))
        response = self._bucket.Object(path).put(
            Body=value, ServerSideEncryption='AES256', ACL='private', 
            StorageClass='STANDARD',
        )
        return response
    
    def __setitem__(self, key, value):
        self.set(key, value)


class BasePool(metaclass=abc.ABCMeta):
    
    def __init__(self, min_size=1, max_size=1024, timeout=20, **connection_kwargs):
        if min_size > max_size:
            raise ValueError
        self.min_size = min_size
        self.max_size = max_size
        self.connection_kwargs = connection_kwargs
        self.timeout = timeout
        self.queue = queue.Queue()
    
    @staticmethod
    @abc.abstractmethod
    def connect(self):
        pass
    
    def fill(self):
        while len(self) < self.min_size:
            connection = self.connect(**self.connection_kwargs)
            self.add(connection)
    
    def empty(self):
        self.queue = queue.Queue()
    
    def add(self, connection):
        if len(self) < self.max_size:
            self.queue.put(connection)
    
    @contextlib.contextmanager
    def swim(self):
        try:
            try:
                connection = self.queue.get(block=False)
            except:
                connection = self.connect(**self.connection_kwargs)
            finally:
                yield connection
        except:
            raise
        else:
            self.add(connection)
        finally:
            self.fill()
    
    def __len__(self):
        return self.queue.qsize()


class Pool(BasePool):
    
    @staticmethod
    def connect(name, access_key_id=None, secret_access_key=None, 
                prefix='/', region='us-east-1'):
        return Bucket(name, access_key_id, secret_access_key, prefix, region)
