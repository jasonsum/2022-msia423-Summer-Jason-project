"""Creates and ingests data into a table of songs for the PennyLane."""
import os
import logging
import sqlalchemy as sql
import sqlalchemy.exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, MetaData, Float

engine_string = os.getenv("SQLALCHEMY_DATABASE_URI")
if engine_string is None:
    raise RuntimeError("SQLALCHEMY_DATABASE_URI environment variable not set; exiting")
# engine_string = "mysql+pymysql://user:password@host:3306/msia423_db"

# set up looging config
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
logger = logging.getLogger(__file__)

Base = declarative_base()


class Features(Base):
    """Creates a table of feature value for generating predictions."""

    __tablename__ = "features"

    id = sqlalchemy.Column(sqlalchemy.Float, primary_key=True)
    access2 = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    arthritis = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    binge = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    bphigh = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    bpmed = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    cancer = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    casthma = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    chd = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    checkup = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    cholscreen = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    copd = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    csmoking = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    depression = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    diabetes = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    highcol = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    kidney = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    lpa = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    mhlth = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    obesity = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    phlth = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    stroke = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    scaled_TotalPopulation = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    midwest = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    northeast = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    south = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    southwest = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    logit_ghlth = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=True)
    ghlth = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=True)

    def __repr__(self):
        return f"<Features: access2: {self.access2}, arthritis: {self.arthritis}, binge: {self.binge}, bphigh: {self.bphigh}, bpmed: {self.bpmed}, cancer: {self.cancer}, \
                casthma: {self.casthma}, chd: {self.chd}, checkup: {self.checkup}, cholscreen: {self.cholscreen}, copd: {self.copd}, csmoking: {self.csmoking}, depression: {self.depression}, \
                diabetes: {self.diabetes}, highcol: {self.highcol}, kidney: {self.kidney}, lpa: {self.lpa}, mhlth: {self.mhlth}, obesity: {self.obesity}, phlth: {self.phlth}, \
                stroke: {self.stroke}, scaled_TotalPopulation: {self.scaled_TotalPopulation}, midwest: {self.midwest}, northeast: {self.northeast}, south: {self.south}, southwest: {self.southwest}>"

class Parameters(Base):
    """Creates a table of regression parameters for predicting log-transformed GHLTH."""

    __tablename__ = "parameters"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    access2 = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    arthritis = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    binge = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    bphigh = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    bpmed = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    cancer = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    casthma = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    chd = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    checkup = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    cholscreen = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    copd = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    csmoking = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    depression = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    diabetes = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    highcol = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    kidney = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    lpa = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    mhlth = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    obesity = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    phlth = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    stroke = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    scaled_TotalPopulation = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    midwest = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    northeast = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    south = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    southwest = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)
    intercept = sqlalchemy.Column(sqlalchemy.Float, unique=False, nullable=False)

    def __repr__(self):
        return f"<Parameters: access2: {self.access2}, arthritis: {self.arthritis}, binge: {self.binge}, bphigh: {self.bphigh}, bpmed: {self.bpmed}, cancer: {self.cancer}, \
                casthma: {self.casthma}, chd: {self.chd}, checkup: {self.checkup}, cholscreen: {self.cholscreen}, copd: {self.copd}, csmoking: {self.csmoking}, depression: {self.depression}, \
                diabetes: {self.diabetes}, highcol: {self.highcol}, kidney: {self.kidney}, lpa: {self.lpa}, mhlth: {self.mhlth}, obesity: {self.obesity}, phlth: {self.phlth}, \
                stroke: {self.stroke}, scaled_TotalPopulation: {self.scaled_TotalPopulation}, midwest: {self.midwest}, northeast: {self.northeast}, south: {self.south}, southwest: {self.southwest}, \
                intercept: {self.intercept}>"

class Measures(Base):
    """Creates a table of references for PLACES measures."""

    __tablename__ = "measures"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    category = sqlalchemy.Column(sqlalchemy.String(100), unique=False, nullable=False)
    measureid = sqlalchemy.Column(sqlalchemy.String(100), unique=False, nullable=False)
    short_question_text = sqlalchemy.Column(sqlalchemy.String(100), unique=False, nullable=False)

    def __repr__(self):
        return f"<Measures: category: {self.category}, measureid: {self.measureid}, short_question_text: {self.short_question_text}>"


if __name__ == "__main__":
    # set up mysql connection
    engine = sql.create_engine(engine_string)

    # test database connection
    try:
        engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        logger.error("Could not connect to database!")
        logger.debug("Database URI: %s", )
        raise e

    # create tableS
    Base.metadata.create_all(engine)
    logger.info("Database created.")

    # create a db session
    Session = sessionmaker(bind=engine)
    session = Session()

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