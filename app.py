import logging
import logging.config
import sqlite3
import traceback

import sqlalchemy.exc
from flask import Flask, render_template, request

# For setting up the Flask-SQLAlchemy database session
from src.models import scalerRanges
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

# Load in scaling and model objects
try:
    scaler_range = pred_manager.session.query(scalerRanges).filter_by(valuename="TotalPopulation").first()
    min_value = scaler_range.min_value
    max_value = scaler_range.max_value
    logger.info("Min-max scaling values loaded.")
except sqlite3.OperationalError as e:
    logger.error(
        "Error page returned. Not able to query local sqlite database: %s."
        " Error: %s ",
        app.config['SQLALCHEMY_DATABASE_URI'], e)
    render_template('error.html')
except sqlalchemy.exc.OperationalError as e:
    logger.error(
        "Error page returned. Not able to query MySQL database: %s. "
        "Error: %s ",
        app.config['SQLALCHEMY_DATABASE_URI'], e)
    render_template('error.html')
except:
    traceback.print_exc()
    logger.error("Not able to display table results, error page returned")
    render_template('error.html')


@app.route('/')
def index():
    """Main view that lists measurements in the database.

    Create view into index page that uses data queried from Track database and
    inserts it into the app/templates/index.html template.

    Returns:
        Rendered html template

    """

    try:
        hlth_outcomes, hlth_behaviors, hlth_prevention = pred_manager.get_metrics(app.config["MAX_ROWS_SHOW"])
        logger.debug("Index page accessed")
        return render_template('index.html', 
                               hlth_outcomes=hlth_outcomes,
                               hlth_behaviors=hlth_behaviors,
                               hlth_prevention=hlth_prevention,
                               prediction="")
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
        logger.error("Not able to display table results, error page returned")
        return render_template('error.html')


@app.route('/add', methods=['POST'])
def add_entry():
    """View that process a POST to Features table

    Returns:
        redirect to index page
    """

    # parse region radio button
    # west is used as omitted dummy variable
    regions = {'midwest':0 ,
               'northeast':0,
               'south':0, 
               'southwest':0}
    for k in regions.keys():
        if k == request.form['region']:
            regions[k] = 1

    scaled_totalpopulation = (float(request.form['population']) - min_value)/(max_value-min_value)

    # Retrieve measurements again for display
    hlth_outcomes, hlth_behaviors, hlth_prevention = pred_manager.get_metrics(app.config["MAX_ROWS_SHOW"])
    
    try:
        prob = pred_manager.generate_pred(access2=float(request.form['access2'])/100,
                                            arthritis=float(request.form['arthritis'])/100,
                                            binge=float(request.form['binge'])/100,
                                            bphigh=float(request.form['bphigh'])/100,
                                            bpmed=float(request.form['bpmed'])/100,
                                            cancer=float(request.form['cancer'])/100,
                                            casthma=float(request.form['casthma'])/100,
                                            chd=float(request.form['chd'])/100,
                                            checkup=float(request.form['checkup'])/100,
                                            cholscreen=float(request.form['cholscreen'])/100,
                                            copd=float(request.form['copd'])/100,
                                            csmoking=float(request.form['csmoking'])/100,
                                            depression=float(request.form['depression'])/100,
                                            diabetes=float(request.form['diabetes'])/100,
                                            highchol=float(request.form['highcol'])/100,
                                            kidney=float(request.form['kidney'])/100,
                                            obesity=float(request.form['obesity'])/100,
                                            stroke=float(request.form['stroke'])/100,
                                            scaled_totalpopulation=scaled_totalpopulation,
                                            midwest=int(regions['midwest']),
                                            northeast=int(regions['northeast']),
                                            south=int(regions['south']),
                                            southwest=int(regions['southwest']))
        logger.info("New prediction recorded.")
        return render_template('index.html', 
                               hlth_outcomes=hlth_outcomes,
                               hlth_behaviors=hlth_behaviors,
                               hlth_prevention=hlth_prevention,
                               prediction=str(prob))
    except sqlite3.OperationalError as e:
        logger.error(
            "Error page returned. Not able to access database"
            "database: %s. Error: %s ",
            app.config['SQLALCHEMY_DATABASE_URI'], e)
        return render_template('error.html')
    except sqlalchemy.exc.OperationalError as e:
        logger.error(
            "Error page returned. Not able to access database: %s. "
            "Error: %s ",
            app.config['SQLALCHEMY_DATABASE_URI'], e)
        return render_template('error.html')


if __name__ == '__main__':
    app.run(debug=app.config["DEBUG"], port=app.config["PORT"],
            host=app.config["HOST"])
