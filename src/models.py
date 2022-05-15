"""Creates table structure for features, model parameters, and refences for PLACES project."""
import os
import logging
import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, MetaData, Float

# set up looging config
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
logger = logging.getLogger(__file__)

Base = declarative_base()

class Features(Base):
    """Creates a table of feature value for generating predictions."""

    __tablename__ = "features"

    id = sql.Column(sql.Float, primary_key=True)
    access2 = sql.Column(sql.Float, unique=False, nullable=False)
    arthritis = sql.Column(sql.Float, unique=False, nullable=False)
    binge = sql.Column(sql.Float, unique=False, nullable=False)
    bphigh = sql.Column(sql.Float, unique=False, nullable=False)
    bpmed = sql.Column(sql.Float, unique=False, nullable=False)
    cancer = sql.Column(sql.Float, unique=False, nullable=False)
    casthma = sql.Column(sql.Float, unique=False, nullable=False)
    chd = sql.Column(sql.Float, unique=False, nullable=False)
    checkup = sql.Column(sql.Float, unique=False, nullable=False)
    cholscreen = sql.Column(sql.Float, unique=False, nullable=False)
    copd = sql.Column(sql.Float, unique=False, nullable=False)
    csmoking = sql.Column(sql.Float, unique=False, nullable=False)
    depression = sql.Column(sql.Float, unique=False, nullable=False)
    diabetes = sql.Column(sql.Float, unique=False, nullable=False)
    highcol = sql.Column(sql.Float, unique=False, nullable=False)
    kidney = sql.Column(sql.Float, unique=False, nullable=False)
    lpa = sql.Column(sql.Float, unique=False, nullable=False)
    mhlth = sql.Column(sql.Float, unique=False, nullable=False)
    obesity = sql.Column(sql.Float, unique=False, nullable=False)
    phlth = sql.Column(sql.Float, unique=False, nullable=False)
    stroke = sql.Column(sql.Float, unique=False, nullable=False)
    scaled_TotalPopulation = sql.Column(sql.Float, unique=False, nullable=False)
    midwest = sql.Column(sql.Float, unique=False, nullable=False)
    northeast = sql.Column(sql.Float, unique=False, nullable=False)
    south = sql.Column(sql.Float, unique=False, nullable=False)
    southwest = sql.Column(sql.Float, unique=False, nullable=False)
    logit_ghlth = sql.Column(sql.Float, unique=False, nullable=True)
    ghlth = sql.Column(sql.Float, unique=False, nullable=True)

    def __repr__(self):
        return f"<Features: access2: {self.access2}, arthritis: {self.arthritis}, binge: {self.binge}, bphigh: {self.bphigh}, bpmed: {self.bpmed}, cancer: {self.cancer}, \
                casthma: {self.casthma}, chd: {self.chd}, checkup: {self.checkup}, cholscreen: {self.cholscreen}, copd: {self.copd}, csmoking: {self.csmoking}, depression: {self.depression}, \
                diabetes: {self.diabetes}, highcol: {self.highcol}, kidney: {self.kidney}, lpa: {self.lpa}, mhlth: {self.mhlth}, obesity: {self.obesity}, phlth: {self.phlth}, \
                stroke: {self.stroke}, scaled_TotalPopulation: {self.scaled_TotalPopulation}, midwest: {self.midwest}, northeast: {self.northeast}, south: {self.south}, southwest: {self.southwest}>"

class Parameters(Base):
    """Creates a table of regression parameters for predicting log-odds of GHLTH."""

    __tablename__ = "parameters"

    id = sql.Column(sql.Integer, primary_key=True)
    access2 = sql.Column(sql.Float, unique=False, nullable=False)
    arthritis = sql.Column(sql.Float, unique=False, nullable=False)
    binge = sql.Column(sql.Float, unique=False, nullable=False)
    bphigh = sql.Column(sql.Float, unique=False, nullable=False)
    bpmed = sql.Column(sql.Float, unique=False, nullable=False)
    cancer = sql.Column(sql.Float, unique=False, nullable=False)
    casthma = sql.Column(sql.Float, unique=False, nullable=False)
    chd = sql.Column(sql.Float, unique=False, nullable=False)
    checkup = sql.Column(sql.Float, unique=False, nullable=False)
    cholscreen = sql.Column(sql.Float, unique=False, nullable=False)
    copd = sql.Column(sql.Float, unique=False, nullable=False)
    csmoking = sql.Column(sql.Float, unique=False, nullable=False)
    depression = sql.Column(sql.Float, unique=False, nullable=False)
    diabetes = sql.Column(sql.Float, unique=False, nullable=False)
    highcol = sql.Column(sql.Float, unique=False, nullable=False)
    kidney = sql.Column(sql.Float, unique=False, nullable=False)
    lpa = sql.Column(sql.Float, unique=False, nullable=False)
    mhlth = sql.Column(sql.Float, unique=False, nullable=False)
    obesity = sql.Column(sql.Float, unique=False, nullable=False)
    phlth = sql.Column(sql.Float, unique=False, nullable=False)
    stroke = sql.Column(sql.Float, unique=False, nullable=False)
    scaled_TotalPopulation = sql.Column(sql.Float, unique=False, nullable=False)
    midwest = sql.Column(sql.Float, unique=False, nullable=False)
    northeast = sql.Column(sql.Float, unique=False, nullable=False)
    south = sql.Column(sql.Float, unique=False, nullable=False)
    southwest = sql.Column(sql.Float, unique=False, nullable=False)
    intercept = sql.Column(sql.Float, unique=False, nullable=False)

    def __repr__(self):
        return f"<Parameters: access2: {self.access2}, arthritis: {self.arthritis}, binge: {self.binge}, bphigh: {self.bphigh}, bpmed: {self.bpmed}, cancer: {self.cancer}, \
                casthma: {self.casthma}, chd: {self.chd}, checkup: {self.checkup}, cholscreen: {self.cholscreen}, copd: {self.copd}, csmoking: {self.csmoking}, depression: {self.depression}, \
                diabetes: {self.diabetes}, highcol: {self.highcol}, kidney: {self.kidney}, lpa: {self.lpa}, mhlth: {self.mhlth}, obesity: {self.obesity}, phlth: {self.phlth}, \
                stroke: {self.stroke}, scaled_TotalPopulation: {self.scaled_TotalPopulation}, midwest: {self.midwest}, northeast: {self.northeast}, south: {self.south}, southwest: {self.southwest}, \
                intercept: {self.intercept}>"

class Measures(Base):
    """Creates a table of references for PLACES measures."""

    __tablename__ = "measures"

    id = sql.Column(sql.Integer, primary_key=True)
    category = sql.Column(sql.String(100), unique=False, nullable=False)
    measureid = sql.Column(sql.String(100), unique=False, nullable=False)
    short_question_text = sql.Column(sql.String(100), unique=False, nullable=False)

    def __repr__(self):
        return f"<Measures: category: {self.category}, measureid: {self.measureid}, short_question_text: {self.short_question_text}>"

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
    teethlost = Measures(category="health outcomes", measureid="teethlost", short_question_text="all teeth lost")
    arthritis = Measures(category="health outcomes", measureid="arthritis", short_question_text="arthritis")
    cancer = Measures(category="health outcomes", measureid="cancer", short_question_text="cancer (except skin)")
    kidney = Measures(category="health outcomes", measureid="kidney", short_question_text="chronic kidney disease")
    copd = Measures(category="health outcomes", measureid="copd", short_question_text="copd")
    chd = Measures(category="health outcomes", measureid="chd", short_question_text="coronary heart disease")
    casthma = Measures(category="health outcomes", measureid="casthma", short_question_text="current asthma")
    depression = Measures(category="health outcomes", measureid="depression", short_question_text="depression")
    diabetes = Measures(category="health outcomes", measureid="diabetes", short_question_text="diabetes")
    bphigh = Measures(category="health outcomes", measureid="bphigh", short_question_text="high blood pressure")
    highchol = Measures(category="health outcomes", measureid="highchol", short_question_text="high cholesterol")
    obesity = Measures(category="health outcomes", measureid="obesity", short_question_text="obesity")
    stroke = Measures(category="health outcomes", measureid="stroke", short_question_text="stroke")
    binge = Measures(category="health risk behaviors", measureid="binge", short_question_text="binge drinking")
    csmoking = Measures(category="health risk behaviors", measureid="csmoking", short_question_text="current smoking")
    lpa = Measures(category="health risk behaviors", measureid="lpa", short_question_text="physical inactivity")
    sleep = Measures(category="health risk behaviors", measureid="sleep", short_question_text="sleep <7 hours")
    ghlth = Measures(category="health status", measureid="ghlth", short_question_text="general health")
    mhlth = Measures(category="health status", measureid="mhlth", short_question_text="mental health")
    phlth = Measures(category="health status", measureid="phlth", short_question_text="physical health")
    cervical = Measures(category="prevention", measureid="cervical", short_question_text="cervical cancer screening")
    cholscreen = Measures(category="prevention", measureid="cholscreen", short_question_text="cholesterol screening")
    access2 = Measures(category="prevention", measureid="access2", short_question_text="health insurance")
    colon_screen = Measures(category="prevention", measureid="colon_screen", short_question_text="colorectal cancer screening")
    mammouse = Measures(category="prevention", measureid="mammouse", short_question_text="mammography")
    corem = Measures(category="prevention", measureid="corem", short_question_text="core preventive services for older men")
    corew = Measures(category="prevention", measureid="corew", short_question_text="core preventive services for older women")
    bpmed = Measures(category="prevention", measureid="bpmed", short_question_text="taking bp medication")
    dental = Measures(category="prevention", measureid="dental", short_question_text="dental visit")
    checkup = Measures(category="prevention", measureid="checkup", short_question_text="annual checkup")
    session.add_all([
                    teethlost,
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
                    lpa,
                    sleep,
                    ghlth,
                    mhlth,
                    phlth,
                    cervical,
                    cholscreen,
                    access2,
                    colon_screen,
                    mammouse,
                    corem,
                    corew,
                    bpmed,
                    dental,
                    checkup])
    session.commit()
    logger.info(
        "Measure references created."
    )

def create_db(engine_string : str) -> None:
    """
    Creates database table schema and populates one table with measurement
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
        logger.error("Could not connect to database!")
        logger.debug("Database URI: %s", )
        raise e

    # create tables
    Base.metadata.create_all(engine)
    logger.info("Database created.")

    # add reference table
    create_references(engine)

