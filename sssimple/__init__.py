# standard libraries
import os
# third party libraries
pass
# first party libraries
from . import (sssimple, )


__where__ = os.path.dirname(os.path.abspath(__file__))


with open(os.path.join(__where__, '..', 'VERSION'), 'rb') as f:
    __version__ = f.read()


S3Bucket = sssimple.Bucket
S3Pool = sssimple.Pool
