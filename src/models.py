"""Creates table structure for features, model parameters, and references for PLACES project."""
import os
import logging
import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, MetaData, Float

# set up looging config
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
logger = logging.getLogger(__name__)

Base = declarative_base()

class Features(Base):
    """Creates a table of feature value for generating predictions."""

    __tablename__ = "features"

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
    highchol = sql.Column(sql.Float, unique=False, nullable=False)
    kidney = sql.Column(sql.Float, unique=False, nullable=False)
    obesity = sql.Column(sql.Float, unique=False, nullable=False)
    stroke = sql.Column(sql.Float, unique=False, nullable=False)
    scaled_totalpopulation = sql.Column(sql.Float, unique=False, nullable=False)
    midwest = sql.Column(sql.Integer, unique=False, nullable=False)
    northeast = sql.Column(sql.Integer, unique=False, nullable=False)
    south = sql.Column(sql.Integer, unique=False, nullable=False)
    southwest = sql.Column(sql.Integer, unique=False, nullable=False)
    prediction = sql.Column(sql.Float, unique=False, nullable=True)
    record_time = sql.Column(sql.DateTime, unique=False, nullable=True, default=func.now())

    def __repr__(self):
        return f"<Features: access2: {self.access2}, arthritis: {self.arthritis}, binge: {self.binge}, bphigh: {self.bphigh}, bpmed: {self.bpmed}, cancer: {self.cancer}, \
                casthma: {self.casthma}, chd: {self.chd}, checkup: {self.checkup}, cholscreen: {self.cholscreen}, copd: {self.copd}, csmoking: {self.csmoking}, depression: {self.depression}, \
                diabetes: {self.diabetes}, highchol: {self.highchol}, kidney: {self.kidney}, obesity: {self.obesity}, stroke: {self.stroke}, scaled_TotalPopulation: {self.scaled_totalpopulation}, \
                midwest: {self.midwest}, northeast: {self.northeast}, south: {self.south}, southwest: {self.southwest}>"

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
    highchol = sql.Column(sql.Float, unique=False, nullable=False)
    kidney = sql.Column(sql.Float, unique=False, nullable=False)
    obesity = sql.Column(sql.Float, unique=False, nullable=False)
    stroke = sql.Column(sql.Float, unique=False, nullable=False)
    scaled_totalpopulation = sql.Column(sql.Float, unique=False, nullable=False)
    midwest = sql.Column(sql.Float, unique=False, nullable=False)
    northeast = sql.Column(sql.Float, unique=False, nullable=False)
    south = sql.Column(sql.Float, unique=False, nullable=False)
    southwest = sql.Column(sql.Float, unique=False, nullable=False)
    intercept = sql.Column(sql.Float, unique=False, nullable=False)

    def __repr__(self):
        return f"<Parameters: access2: {self.access2}, arthritis: {self.arthritis}, binge: {self.binge}, bphigh: {self.bphigh}, bpmed: {self.bpmed}, cancer: {self.cancer}, \
                casthma: {self.casthma}, chd: {self.chd}, checkup: {self.checkup}, cholscreen: {self.cholscreen}, copd: {self.copd}, csmoking: {self.csmoking}, depression: {self.depression}, \
                diabetes: {self.diabetes}, highchol: {self.highchol}, kidney: {self.kidney}, obesity: {self.obesity}, stroke: {self.stroke}, scaled_TotalPopulation: {self.scaled_totalpopulation}, \
                midwest: {self.midwest}, northeast: {self.northeast}, south: {self.south}, southwest: {self.southwest}, \
                intercept: {self.intercept}>"

class scalerRanges(Base):
    """Creates a table of min-max values of scaled features."""

    __tablename__ = "scaler_ranges"

    id = sql.Column(sql.Integer, primary_key=True)
    valuename = sql.Column(sql.String(100), unique=False, nullable=True)
    max_value = sql.Column(sql.Float, unique=False, nullable=True)
    min_value = sql.Column(sql.Float, unique=False, nullable=True)

    def __repr__(self):
        return f"<Column: access2: {self.valuename}, max: {self.max_value}, min: {self.min_value}>"

class Measures(Base):
    """Creates a table of references for PLACES measures."""

    __tablename__ = "measures"

    id = sql.Column(sql.Integer, primary_key=True)
    category = sql.Column(sql.String(100), unique=False, nullable=False)
    measureid = sql.Column(sql.String(100), unique=False, nullable=False)
    short_question_text = sql.Column(sql.String(100), unique=False, nullable=False)

    def __repr__(self):
        return f"<Measures: category: {self.category}, measureid: {self.measureid}, short_question_text: {self.short_question_text}>"


def create_db(engine_string : str) -> None:
    """
    Creates database table schema.

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

    # drop tables if exists
    Base.metadata.drop_all(engine)
    # create tables
    Base.metadata.create_all(engine)
    logger.info("Database created.")

