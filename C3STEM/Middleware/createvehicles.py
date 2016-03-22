#Create vehicles in TrafficData.xml
#Uncomment this part once you need to
#add/remove cars from simulation
import xml.dom.minidom

rouDoc = xml.dom.minidom.parse(r'dir\C3STEM\Middleware\westend_2.rou.xml')
f = open(r'dir\Middleware\TrafficData.xml', 'w')
f.write("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n")
f.write("<vehicles>\n")
for node in rouDoc.getElementsByTagName("vehicle"):
	veh_id = str(node.getAttribute("id"))
	f.write("\t<vehicle id=\"")
	f.write(str(veh_id))
	f.write("\" lat=\"0\" lng=\"0\" >")
	f.write("</vehicle>\n")
f.write("</vehicles>\n")
