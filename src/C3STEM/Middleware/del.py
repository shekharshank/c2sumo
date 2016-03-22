from cloud.NovaCloudAccess import *
from cloud.CloudInterface import *

import sys

vm_name = sys.argv[1]
cloud = CloudInterface();
is_deleted = cloud.deleteVM(vm_name);
print is_deleted;

