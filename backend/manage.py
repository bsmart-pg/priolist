from flask_script import Server, Manager
import os
from main import app

manager = Manager(app)

manager.add_command("runserver", Server(
    use_debugger=True,
    use_reloader=True,
    host='localhost',
    port=5000
))

if __name__ == '__main__':
    manager.run()