"""Ingests user input as features into table, enables querying of feature, parameter,
and reference tables and renders query results and prediction for PLACES app."""
# mypy: plugins = sqlmypy, plugins = flasksqlamypy
import argparse
import logging
import logging.config
import sqlite3
import typing

import flask
import sqlalchemy
import sqlalchemy.orm
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import desc

from src.models import Features, Parameters, Measures, scalerRanges

logger = logging.getLogger(__name__)

Base: typing.Any = declarative_base()


class PredManager:
    """Creates a SQLAlchemy connection to the Features and Parameter tables.

    Args:
        app (:obj:`flask.app.Flask`): Flask app object for when connecting from
            within a Flask app. Optional.
        engine_string (str): SQLAlchemy engine string specifying which database
            to write to. Follows the format
    """
    def __init__(self, app: typing.Optional[flask.app.Flask] = None,
                 engine_string: typing.Optional[str] = None):
        if app:
            self.database = SQLAlchemy(app)
            self.session = self.database.session
        elif engine_string:
            engine = sqlalchemy.create_engine(engine_string)
            session_maker = sqlalchemy.orm.sessionmaker(bind=engine)
            self.session = session_maker()
        else:
            raise ValueError(
                "Need either an engine string or a Flask app to initialize")

    def close(self) -> None:
        """Closes SQLAlchemy session

        Returns: None

        """
        self.session.close()
          

    def add_input(self, 
                  access2: float,
                  arthritis: float,
                  binge: float,
                  bphigh: float,
                  bpmed: float,
                  cancer: float,
                  casthma: float,
                  chd: float,
                  checkup: float,
                  cholscreen: float,
                  copd: float,
                  csmoking: float,
                  depression: float,
                  diabetes: float,
                  highchol: float,
                  kidney: float,
                  obesity: float,
                  stroke: float,
                  scaled_TotalPopulation: float,
                  midwest: int,
                  northeast: int,
                  south: int,
                  southwest: int
                  ) -> None:
        """Seeds an existing database with new user feature input values.

        See Measures table or reference information for more information about each measure.

        Args:
            access2 (float) : Proportion of county population recorded with indicated measure.
            arthritis (float) : Proportion of county population recorded with indicated measure.
            binge (float) : Proportion of county population recorded with indicated measure.
            bphigh (float) : Proportion of county population recorded with indicated measure.
            bpmed (float) : Proportion of county population recorded with indicated measure.
            cancer (float) : Proportion of county population recorded with indicated measure.
            casthma (float) : Proportion of county population recorded with indicated measure.
            chd (float) : Proportion of county population recorded with indicated measure.
            checkup (float) : Proportion of county population recorded with indicated measure.
            cholscreen (float) : Proportion of county population recorded with indicated measure.
            copd (float) : Proportion of county population recorded with indicated measure.
            csmoking (float) : Proportion of county population recorded with indicated measure.
            depression (float) : Proportion of county population recorded with indicated measure.
            diabetes (float) : Proportion of county population recorded with indicated measure.
            highchol (float) : Proportion of county population recorded with indicated measure.
            kidney (float) : Proportion of county population recorded with indicated measure.
            obesity (float) : Proportion of county population recorded with indicated measure.
            stroke (float) : Proportion of county population recorded with indicated measure.
            scaled_TotalPopulation (float) : Scaled value between 0 and 1 representing county population.
            midwest: Binary indicator of county in corresponding region. 1 indicates True.
            northeast: Binary indicator of county in corresponding region. 1 indicates True.
            south: Binary indicator of county in corresponding region. 1 indicates True.
            southwest: Binary indicator of county in corresponding region. 1 indicates True.


        Returns:
            None
        """

        session = self.session
        input = Features(access2=access2,
                         arthritis=arthritis,
                         binge=binge,
                         bphigh=bphigh,
                         bpmed=bpmed,
                         cancer=cancer,
                         casthma=casthma,
                         chd=chd,
                         checkup=checkup,
                         cholscreen=cholscreen,
                         copd=copd,
                         csmoking=csmoking,
                         depression=depression,
                         diabetes=diabetes,
                         highchol=highchol,
                         kidney=kidney,
                         obesity=obesity,
                         stroke=stroke,
                         scaled_TotalPopulation=scaled_TotalPopulation,
                         midwest=midwest,
                         northeast=northeast,
                         south=south,
                         southwest=southwest)
        
        session.add(input)
        session.commit()
        input_arr = [access2, arthritis, binge,	bphigh,	bpmed, cancer, casthma, chd, 
                     checkup, cholscreen, copd, csmoking, depression, diabetes, highchol, 
                     kidney, obesity, stroke, scaled_TotalPopulation]
        logger.info("New observation entered, min value: %.2f, max_value: %.2f", min(input_arr), max(input_arr))