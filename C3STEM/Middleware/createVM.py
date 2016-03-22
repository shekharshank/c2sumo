from cloud.NovaCloudAccess import *
from cloud.CloudInterface import *

#client = NovaCloudAccess();
#client.getServerList();
#client.getImageList();
#client.getFlavorList();
#client.createServer("229-snapshot", "m1.medium");

cloud = CloudInterface();
vm_name = cloud.createVM();
print vm_name;

