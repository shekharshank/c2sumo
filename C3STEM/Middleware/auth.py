import cherrypy
from bson.objectid import ObjectId
from DBUtil import *
from UserDataInterface import *
import bcrypt

SESSION_KEY = '_cp_username'
SESSION_AUTH_KEY = '_cp_auth'

def check_credentials(username, password):
    """Verifies credentials for username and password.
    Returns None on success or a string describing the error on failure"""
    # Adapt to your needs
    db = DBUtil().getDatabase()
	
    student = db.student.find_one({"_id":username})
    
    if student is not None:
	hashed = student['password'].strip()
	input_hash = bcrypt.hashpw(password.encode('utf-8'), hashed)
	if input_hash != hashed:
    		return u"Incorrect username or password."
        return None

    return u"Incorrect username or password."
    
    # An example implementation which uses an ORM could be:
    # u = User.get(username)
    # if u is None:
    #     return u"Username %s is unknown to me." % username
    # if u.password != md5.new(password).hexdigest():
    #     return u"Incorrect password"

def check_auth(*args, **kwargs):
    """A tool that looks in config for 'auth.require'. If found and it
    is not None, a login is required and the entry is evaluated as a list of
    conditions that the user must fulfill"""
    conditions = cherrypy.request.config.get('auth.require', None)
    if conditions is not None:
        username = cherrypy.session.get(SESSION_KEY)
        if username:
            cherrypy.request.login = username
            cherrypy.session[SESSION_AUTH_KEY] = "true"
            for condition in conditions:
                # A condition is just a callable that returns true or false
                if not condition():
                	cherrypy.session[SESSION_AUTH_KEY] = "false"
                	break
        else:
            cherrypy.session[SESSION_AUTH_KEY] = "false"
            raise cherrypy.HTTPRedirect("/auth/login")
    else:
    	cherrypy.session[SESSION_AUTH_KEY] = "true"
cherrypy.tools.auth = cherrypy.Tool('before_handler', check_auth)

def require(*conditions):
    """A decorator that appends conditions to the auth.require config
    variable."""
    def decorate(f):
        if not hasattr(f, '_cp_config'):
            f._cp_config = dict()
        if 'auth.require' not in f._cp_config:
            f._cp_config['auth.require'] = []
        f._cp_config['auth.require'].extend(conditions)
        return f
    return decorate


# Conditions are callables that return True
# if the user fulfills the conditions they define, False otherwise
#
# They can access the current username as cherrypy.request.login
#
# Define those at will however suits the application.

def member_of(groupname):
    def check():
        # replace with actual check if <username> is in <groupname>
        return cherrypy.request.login == 'joe' and groupname == 'admin'
    return check

def name_is(reqd_username):
    return lambda: reqd_username == cherrypy.request.login


def has_role(rqd_role):
    def role_check():

	    db = DBUtil().getDatabase()
	    student_rolerow = db.user_studentrole.find_one({"user_id":cherrypy.request.login})
	    roles = student_rolerow["role_ids"]
	    if (rqd_role in roles):
		return True
	    else:
		return False
    return role_check

def has_role_of(rqd_role):
	    db = DBUtil().getDatabase()
	    student_rolerow = db.user_studentrole.find_one({"user_id":cherrypy.request.login})
	    roles = student_rolerow["role_ids"]
	    if (rqd_role in roles):
		return True
	    else:
		return False

def has_privileges():
	mode = UserDataInterface().getUserMode(cherrypy.request.login)
	if(mode == 'COLAB'):
		return has_role_of('super')
	return True

# These might be handy

def any_of(*conditions):
    """Returns True if any of the conditions match"""
    def check():
        for c in conditions:
            if c():
                return True
        return False
    return check

# By default all conditions are required, but this might still be
# needed if you want to use it inside of an any_of(...) condition
def all_of(*conditions):
    """Returns True if all of the conditions match"""
    def check():
        for c in conditions:
            if not c():
                return False
        return True
    return check


# Controller to provide login and logout actions

class AuthController(object):
    
    def on_login(self, username):
        """Called on successful login"""
    
    def on_logout(self, username):
        """Called on logout"""
    
    def get_loginform(self, username, msg="Enter login information", from_page="/"):
        return """<html><body>
            <form method="post" action="/auth/login">
            <input type="hidden" name="from_page" value="%(from_page)s" />
            %(msg)s<br />
            Username: <input type="text" name="username" value="%(username)s" /><br />
            Password: <input type="password" name="password" /><br />
            <input type="submit" value="Log in" />
        </body></html>""" % locals()
    
    @cherrypy.expose
    def login(self, username=None, password=None, from_page="/"):
        if username is None or password is None:
            return self.get_loginform("", from_page=from_page)
        
        error_msg = check_credentials(username, password)
        if error_msg:
            return self.get_loginform(username, error_msg, from_page)
        else:
            cherrypy.session[SESSION_KEY] = cherrypy.request.login = username
            self.on_login(username)
            raise cherrypy.HTTPRedirect(from_page or "/")
    
    @cherrypy.expose
    def logout(self, from_page="/"):
        sess = cherrypy.session
        username = sess.get(SESSION_KEY, None)
        if username:
	    cherrypy.request.login = None
            self.on_logout(username)
        sess[SESSION_KEY] = None
	sess[SESSION_AUTH_KEY] = None
        
        raise cherrypy.HTTPRedirect(from_page or "/")
