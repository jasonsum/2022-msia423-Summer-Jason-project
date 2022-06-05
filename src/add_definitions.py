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

def create_references(engine : sql.engine.base.Engine) -> None:
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
    arthritis = Measures(category="health outcomes", measureid="arthritis", short_question_text="Arthritis",
                         long_question_text="Arthritis among adults aged >=18 years")
    cancer = Measures(category="health outcomes", measureid="cancer", short_question_text="Cancer (except skin)",
                      long_question_text="Cancer (excluding skin cancer) among adults aged >=18 years")
    kidney = Measures(category="health outcomes", measureid="kidney", short_question_text="Chronic Kidney Disease",
                      long_question_text="Chronic kidney disease among adults aged >=18 years")
    copd = Measures(category="health outcomes", measureid="copd", short_question_text="COPD",
                    long_question_text="Chronic obstructive pulmonary disease among adults aged >=18 years")
    chd = Measures(category="health outcomes", measureid="chd", short_question_text="Coronary Heart Disease",
                   long_question_text="Coronary heart disease among adults aged >=18 years")
    casthma = Measures(category="health outcomes", measureid="casthma", short_question_text="Current Asthma",
                       long_question_text="Current asthma among adults aged >=18 years")
    depression = Measures(category="health outcomes", measureid="depression", short_question_text="Depression",
                          long_question_text="Depression among adults aged >=18 years")
    diabetes = Measures(category="health outcomes", measureid="diabetes", short_question_text="Diabetes",
                        long_question_text="Diagnosed diabetes among adults aged >=18 years")
    bphigh = Measures(category="health outcomes", measureid="bphigh", short_question_text="High Blood Pressure",
                      long_question_text="High blood pressure among adults aged >=18 years")
    highchol = Measures(category="health outcomes", measureid="highcol", short_question_text="High Cholesterol",
                        long_question_text="High cholesterol among adults aged >=18 years who have been\
                                            screened in the past 5 years")
    obesity = Measures(category="health outcomes", measureid="obesity", short_question_text="Obesity",
                       long_question_text="Obesity among adults aged >=18 years")
    stroke = Measures(category="health outcomes", measureid="stroke", short_question_text="Stroke",
                      long_question_text="Stroke among adults aged >=18 years")
    binge = Measures(category="health risk behaviors", measureid="binge", short_question_text="Binge Drinking",
                     long_question_text="Binge drinking among adults aged >=18 years")
    csmoking = Measures(category="health risk behaviors", measureid="csmoking", short_question_text="Current Smoking",
                        long_question_text="Current smoking among adults aged >=18 years")
    ghlth = Measures(category="health status", measureid="ghlth", short_question_text="General Health",
                     long_question_text="Fair or poor self-rated health status among adults aged >=18 years")
    cholscreen = Measures(category="prevention", measureid="cholscreen", short_question_text="Cholesterol Screening",
                          long_question_text="Cholesterol screening among adults aged >=18 years")
    access2 = Measures(category="prevention", measureid="access2", short_question_text="Health Insurance",
                       long_question_text="Current lack of health insurance among adults aged 18-64 years")
    colon_screen = Measures(category="prevention", measureid="colon_screen",
                            short_question_text="Colorectal Cancer Screening",
                            long_question_text="Fecal occult blood test, sigmoidoscopy, or colonoscopy\
                                                among adults aged 50-75 years")
    bpmed = Measures(category="prevention", measureid="bpmed", short_question_text="Taking BP Medication",
                     long_question_text="Taking medicine for high blood pressure control among adults\
                                         aged >=18 years with high blood pressure")
    checkup = Measures(category="prevention", measureid="checkup", short_question_text="Annual Checkup",
                       long_question_text="Visits to doctor for routine checkup within the\
                                           past year among adults aged >=18 years")

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
