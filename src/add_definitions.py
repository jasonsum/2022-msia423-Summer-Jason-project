"""
Adds definitions to Measures table for metric references.
"""

import logging

import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

from src.models import Measures

logger = logging.getLogger(__name__)

Base = declarative_base()

def create_references(engine : sql.engine.base.Engine):
    """
    Creates database table of PLACES metric reference information.

    Args:
        engine (sql.engine.base.Engine) : SQL Alchemy engine object.

    Returns:
        None

    """

    # create a db session
    Session : sqlalchemy.orm.session.sessionmaker = sqlalchemy.orm.sessionmaker(bind=engine)
    session : sqlalchemy.orm.session.Session = Session()

    # add measure references
    arthritis = Measures(category="health outcomes", measureid="arthritis", short_question_text="arthritis")
    cancer = Measures(category="health outcomes", measureid="cancer", short_question_text="cancer (except skin)")
    kidney = Measures(category="health outcomes", measureid="kidney", short_question_text="chronic kidney disease")
    copd = Measures(category="health outcomes", measureid="copd", short_question_text="copd")
    chd = Measures(category="health outcomes", measureid="chd", short_question_text="coronary heart disease")
    casthma = Measures(category="health outcomes", measureid="casthma", short_question_text="current asthma")
    depression = Measures(category="health outcomes", measureid="depression", short_question_text="depression")
    diabetes = Measures(category="health outcomes", measureid="diabetes", short_question_text="diabetes")
    bphigh = Measures(category="health outcomes", measureid="bphigh", short_question_text="high blood pressure")
    highchol = Measures(category="health outcomes", measureid="highcol", short_question_text="high cholesterol")
    obesity = Measures(category="health outcomes", measureid="obesity", short_question_text="obesity")
    stroke = Measures(category="health outcomes", measureid="stroke", short_question_text="stroke")
    binge = Measures(category="health risk behaviors", measureid="binge", short_question_text="binge drinking")
    csmoking = Measures(category="health risk behaviors", measureid="csmoking", short_question_text="current smoking")
    ghlth = Measures(category="health status", measureid="ghlth", short_question_text="general health")
    cholscreen = Measures(category="prevention", measureid="cholscreen", short_question_text="cholesterol screening")
    access2 = Measures(category="prevention", measureid="access2", short_question_text="health insurance")
    colon_screen = Measures(category="prevention", measureid="colon_screen", short_question_text="colorectal cancer screening")
    bpmed = Measures(category="prevention", measureid="bpmed", short_question_text="taking bp medication")
    checkup = Measures(category="prevention", measureid="checkup", short_question_text="annual checkup")
    
    session.query(Measures).delete()

    session.add_all([
                    arthritis,
                    cancer,
                    kidney,
                    copd,
                    chd,
                    casthma,
                    depression,
                    diabetes,
                    bphigh,
                    highchol,
                    obesity,
                    stroke,
                    binge,
                    csmoking,
                    ghlth,
                    cholscreen,
                    access2,
                    colon_screen,
                    bpmed,
                    checkup])
    session.commit()
    logger.info(
        "Measure references created."
    )

def add_references(engine_string : str) -> None:
    """
    Populates Measures table with measurement
    PLACES metric reference information.

    Args:
        engine_string (str) : SQL Alchemy database URI path.

    Returns:
        None

    """
    engine : sql.engine.base.Engine = sql.create_engine(engine_string)

    # test database connection
    try:
        engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        logger.error("Could not connect to database.")
        logger.debug("Database URI: %s", )
        raise e

    # add reference table
    create_references(engine)

