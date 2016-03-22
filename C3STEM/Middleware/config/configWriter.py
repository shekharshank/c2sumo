import ConfigParser

cfgfile = open("ec2cloud.conf",'w')

# add the settings to the structure of the file, and lets write it out...
Config = ConfigParser.RawConfigParser();

Config.add_section('AuthDetails')
Config.set('AuthDetails', 'username','user')
Config.set('AuthDetails','password','password')
Config.set('AuthDetails','tenant','C3STEM')
Config.set('AuthDetails','authURL','https://server:5000/v2.0')
Config.write(cfgfile)
cfgfile.close()
