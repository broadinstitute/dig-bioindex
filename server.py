import dotenv
import flask
import flask_cors

import api.bio
import api.portal


# load dot files and configuration
dotenv.load_dotenv()

# create flask app; this will load .env
app = flask.Flask(__name__, static_folder='web/static')
flask_cors.CORS(app)

# resource service routes
app.register_blueprint(api.bio.routes)
app.register_blueprint(api.portal.routes)


@app.route('/')
def index():
    """
    SPA page.
    """
    return flask.send_file('web/index.html', mimetype='text/html')
