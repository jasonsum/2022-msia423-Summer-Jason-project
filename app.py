import logging
import logging.config
import sqlite3
import traceback

import sqlalchemy.exc
from flask import Flask, render_template, request, redirect, url_for

# For setting up the Flask-SQLAlchemy database session
from src.models import Measures, Features
from src.run_pred import PredManager 

# Initialize the Flask application
app = Flask(__name__, template_folder="app/templates",
            static_folder="app/static")

# Configure flask app from flask_config.py
app.config.from_pyfile('config/flaskconfig.py')

# Define LOGGING_CONFIG in flask_config.py - path to config file for setting
# up the logger (e.g. config/logging/local.conf)
logging.config.fileConfig(app.config["LOGGING_CONFIG"])
logger = logging.getLogger(app.config["APP_NAME"])
logger.debug(
    'Web app should be viewable at %s:%s if docker run command maps local '
    'port to the same port as configured for the Docker container '
    'in config/flaskconfig.py (e.g. `-p 5000:5000`). Otherwise, go to the '
    'port defined on the left side of the port mapping '
    '(`i.e. -p THISPORT:5000`). If you are running from a Windows machine, '
    'go to 127.0.0.1 instead of 0.0.0.0.', app.config["HOST"]
    , app.config["PORT"])

# Initialize the database session
pred_manager = PredManager(app)


@app.route('/')
def index():
    """Main view that lists songs in the database.

    Create view into index page that uses data queried from Track database and
    inserts it into the app/templates/index.html template.

    Returns:
        Rendered html template

    """

    try:
        measures = pred_manager.session.query(Measures).limit(
            app.config["MAX_ROWS_SHOW"]).all()
        logger.debug("Index page accessed")
        return render_template('index.html', measures=measures)
    except sqlite3.OperationalError as e:
        logger.error(
            "Error page returned. Not able to query local sqlite database: %s."
            " Error: %s ",
            app.config['SQLALCHEMY_DATABASE_URI'], e)
        return render_template('error.html')
    except sqlalchemy.exc.OperationalError as e:
        logger.error(
            "Error page returned. Not able to query MySQL database: %s. "
            "Error: %s ",
            app.config['SQLALCHEMY_DATABASE_URI'], e)
        return render_template('error.html')
    except:
        traceback.print_exc()
        logger.error("Not able to display tracks, error page returned")
        return render_template('error.html')


@app.route('/add', methods=['POST'])
def add_entry():
    """View that process a POST with new song input

    Returns:
        redirect to index page
    """

    try:
        pred_manager.add_input(access2=request.form['access2'],
                               arthritis=request.form['arthritis'],
                               binge=request.form['binge'],
                               bphigh=request.form['bphigh'],
                               bpmed=request.form['bpmed'],
                               cancer=request.form['cancer'],
                               casthma=request.form['casthma'],
                               chd=request.form['chd'],
                               checkup=request.form['checkup'],
                               cholscreen=request.form['cholscreen'],
                               copd=request.form['copd'],
                               csmoking=request.form['csmoking'],
                               depression=request.form['depression'],
                               diabetes=request.form['diabetes'],
                               highcol=request.form['highcol'],
                               kidney=request.form['kidney'],
                               obesity=request.form['obesity'],
                               stroke=request.form['stroke'],
                               scaled_TotalPopulation=request.form['scaled_TotalPopulation'],
                               midwest=request.form['midwest'],
                               northeast=request.form['northeast'],
                               south=request.form['south'],
                               southwest=request.form['southwest'])
        logger.info("New recorded added for prediction.")
        return redirect(url_for('index'))
    except sqlite3.OperationalError as e:
        logger.error(
            "Error page returned. Not able to add song to local sqlite "
            "database: %s. Error: %s ",
            app.config['SQLALCHEMY_DATABASE_URI'], e)
        return render_template('error.html')
    except sqlalchemy.exc.OperationalError as e:
        logger.error(
            "Error page returned. Not able to add song to MySQL database: %s. "
            "Error: %s ",
            app.config['SQLALCHEMY_DATABASE_URI'], e)
        return render_template('error.html')


if __name__ == '__main__':
    app.run(debug=app.config["DEBUG"], port=app.config["PORT"],
            host=app.config["HOST"])
