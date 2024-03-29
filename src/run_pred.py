"""
Ingests user input as features into table, enables querying of feature, parameter,
and reference tables and renders query results and prediction for PLACES app.
"""

import logging
import logging.config
import typing
import math

import flask
import sqlalchemy
import sqlalchemy.orm
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base

from src.models import Features, Parameters, Measures

logger = logging.getLogger(__name__)

Base: typing.Any = declarative_base()


class PredManager:
    """
    Creates a SQLAlchemy connection to the Features and Parameter tables.

    Args:
        app (obj:flask.app.Flask) : Flask app object for when connecting from
                                    within a Flask app. Optional.
        engine_string (str) : SQLAlchemy engine string specifying which database
                              to write to. Follows the format.
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
        """
        Closes SQLAlchemy session

        Returns:
            None

        """
        self.session.close()

    def get_metrics(self,
                    row_limit : int) -> typing.Tuple:

        """
        Queries database for homepage measurements of app.

        Args:
            row_limit (int): Number of rows to return across all queries
        Returns:
            Tuple of query results

        """

        session = self.session
        hlth_outcomes = session.query(Measures).filter_by(category="health outcomes").limit(
            row_limit).all()
        hlth_behaviors = session.query(Measures).filter_by(category="health risk behaviors").limit(
            row_limit).all()
        hlth_prevention = session.query(Measures).filter_by(category="prevention").limit(
            row_limit).all()

        return  hlth_outcomes, hlth_behaviors, hlth_prevention

    def generate_pred(self,
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
                  scaled_totalpopulation: float,
                  midwest: int,
                  northeast: int,
                  south: int,
                  southwest: int
                  ) -> float:
        """
        Seeds an existing database with new user feature input values.

        Returns prediction as float in percentage format.
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
            midwest (int) : Binary indicator of county in corresponding region. 1 indicates True.
            northeast (int) : Binary indicator of county in corresponding region. 1 indicates True.
            south (int) : Binary indicator of county in corresponding region. 1 indicates True.
            southwest (int) : Binary indicator of county in corresponding region. 1 indicates True.

        Returns:
            prediction result
        """

        session = self.session
        coeffs = session.query(Parameters).first()
        # Generate prediction
        pred = (coeffs.access2 * access2 +
                coeffs.arthritis * arthritis +
                coeffs.binge * binge +
                coeffs.bphigh * bphigh +
                coeffs.bpmed * bpmed +
                coeffs.cancer * cancer +
                coeffs.casthma * casthma +
                coeffs.chd * chd +
                coeffs.checkup * checkup +
                coeffs.cholscreen * cholscreen +
                coeffs.copd * copd +
                coeffs.csmoking * csmoking +
                coeffs.depression * depression +
                coeffs.diabetes * diabetes +
                coeffs.highchol * highchol +
                coeffs.kidney * kidney +
                coeffs.obesity * obesity +
                coeffs.stroke * stroke +
                coeffs.scaled_totalpopulation * scaled_totalpopulation +
                coeffs.midwest * midwest +
                coeffs.northeast * northeast +
                coeffs.south * south +
                coeffs.southwest * southwest +
                coeffs.intercept)

        # Capture probability from log-odds
        # prob = odds / (1 + odds)
        prob = math.exp(pred) / (1 + math.exp(pred))

        # Record new prediction
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
                         scaled_totalpopulation=scaled_totalpopulation,
                         midwest=midwest,
                         northeast=northeast,
                         south=south,
                         southwest=southwest,
                         prediction=prob)

        session.add(input)
        logger.info("New prediction generated: %.2f", prob)
        session.commit()
        return round(prob*100, 2)

    def remove_inputs(self) -> None:
        """
        Deletes all rows from Features table.

        Args:
            None

        Returns:
            None
        """

        session = self.session
        session.query(Features).delete()
        session.commit()
