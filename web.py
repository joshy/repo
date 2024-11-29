import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, g, jsonify, make_response, render_template, request
from flask_assets import Bundle, Environment

from src.repo.database.connection import (
    open_connection,
    secta_cdwh_connection,
    usb_cdwh_engine,
)
from src.repo.database.contrast_medium import query_contrast_medium
from src.repo.database.fall import (
    query_acc,
    query_for_acc_given_fall_id,
    query_for_fall_id_given_acc,
)
from src.repo.database.sectra_cdwh import get_as_txt_from_sectra
from src.repo.database.usb_cdwh import get_patho_report
from src.repo.distiller import process
from src.repo.report import get_as_rtf, get_as_txt, get_with_file, parse_report, q

REPORTS_FOLDER = "reports"
if not os.path.exists(REPORTS_FOLDER):
    os.makedirs(REPORTS_FOLDER, exist_ok=True)


def create_app():
    load_dotenv()
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object("src.repo.default_config")
    app.config.from_pyfile("config.cfg")
    app.jinja_env.add_extension("jinja2.ext.loopcontrols")
    app.jinja_env.add_extension("jinja2.ext.do")
    version = app.config["VERSION"] = "4.0.1"

    assets = Environment(app)
    js = Bundle(
        "js/plugins/jquery-3.1.0.min.js",
        "js/plugins/moment.min.js",
        "js/plugins/pikaday.js",
        "js/plugins/pikaday.jquery.js",
        "js/dashboard/writerDashboard.js",
        "js/dashboard/reviewerDashboard.js",
        "js/handlers/diffHandling.js",
        "js/handlers/checkBoxHandling.js",
        "js/handlers/datePickerHandling.js",
        "js/handlers/clearHandling.js",
        "js/handlers/infoHandling.js",
        "js/handlers/buttonHandling.js",
        "js/handlers/floatTheadHandling.js",
        "js/graphs/graph.js",
        "js/graphs/pieChart.js",
        "js/graphs/barChart.js",
        filters="jsmin",
        output="gen/packed.js",
    )
    assets.register("js_all", js)

    @app.route("/")
    def main():
        return render_template("index.html", version=version)

    @app.route("/q")
    def query():
        day = request.args.get("day", "")
        dd = datetime.strptime(day, "%Y-%m-%d")
        parse_text = request.args.get("parse", False)
        if not day:
            logging.debug("No day given, returning to main view")
            return main()
        engine = get_ris_db()
        rows = q(engine, dd, parse_text)
        return jsonify(rows)

    def remove_NaT_format(df):
        return df.fillna("None")

    @app.route("/cm")
    def cm():
        "Queries for contrast medium for a accession number"
        accession_number = request.args.get("accession_number", "")
        if not accession_number:
            print("No accession number found in request, use accession_number=XXX")
            return main()
        con = get_ris_db()
        result = query_contrast_medium(con.cursor(), accession_number)
        return jsonify(result)

    @app.route("/acc2fall")
    def acc2fall():
        "Queries for fallid for a accession number"
        accession_number = request.args.get("accession_number", "")
        if not accession_number:
            print("No accession number found in request, use accession_number=XXX")
            return main()
        con = get_ris_db()
        result = query_for_fall_id_given_acc(con.cursor(), accession_number)
        return jsonify(result)

    @app.route("/fall2acc")
    def fall2acc():
        "Queries for accession number for a fall id"
        fall_id = request.args.get("fall_id", "")
        if not fall_id:
            print("No fall id found in request, use fall_id=XXX")
            return main()
        con = get_ris_db()
        result = query_for_acc_given_fall_id(con.cursor(), fall_id)
        return jsonify(result)

    @app.route("/acc")
    def acc():
        "Queries for accession for a given befund id"
        befund_id = request.args.get("befund_id", "")
        if not befund_id:
            print("No befund_id found in request, use befund_id=XXX")
            return main()
        con = get_ris_db()
        result = query_acc(con.cursor(), befund_id)
        return jsonify(result)

    @app.route("/sectra")
    def sectra():
        """Get the rtf report from sectra and return txt"""
        accession_number = request.args.get("accession_number", "")
        if not accession_number:
            return "No accession number found in request, use accession_number=XXX"
        
        # ok we have request for pathology
        if not accession_number[0].isdigit():
            engine = get_usb_cdwh_db()
            report = get_patho_report(engine, accession_number)
            return report
        else:
            engine = get_sectra_db()
            report_as_text = get_as_txt_from_sectra(engine, accession_number)
            return report_as_text

    @app.route("/show")
    def show():
        """Renders RIS Report as HTML."""
        accession_number = request.args.get("accession_number", "")
        output = request.args.get("output", "html")
        # if no accession number is given -> render main page
        if not accession_number:
            print("No accession number found in request, use accession_number=XXX")
            return main()

        if not accession_number.isdigit():
            logging.error(
                f'Accession number "{accession_number}" can\'t be converted to a number'
            )
            return f"No report found for accession number: {accession_number}"

        engine = get_ris_db()
        if output == "text":
            report_as_text, meta_data = get_as_txt(engine, accession_number)
            if report_as_text:
                return report_as_text
            else:
                # don't throw an error, no report found -> return empty response
                # because not all accession numbers have a valid report
                return ""
        else:
            report_as_html, meta_data = get_with_file(engine, accession_number)
            return render_template(
                "report.html",
                version=app.config["VERSION"],
                accession_number=accession_number,
                meta_data=meta_data,
                report=report_as_html,
            )

    @app.route("/distill")
    def distill():
        """Renders RIS Report as HTML."""
        accession_number = request.args.get("accession_number", "")
        output = request.args.get("output", "html")
        # if no accession number is given -> render main page
        if not accession_number:
            logging.warn(
                "No accession number found in request, use accession_number=XXX"
            )
            return main()

        if not accession_number.isdigit():
            logging.error(
                'Accession number "{}" can\'t be converted to a number'.format(
                    accession_number
                )
            )
            return render_template(
                "report.html",
                version=app.config["VERSION"],
                accession_number=accession_number,
                meta_data={},
                report=None,
            )

        engine = get_ris_db()
        report_as_text, meta_data = get_as_txt(engine, accession_number)
        result = process(report_as_text, meta_data)
        output = request.args.get("output", "html")
        if output == "json":
            j = {}
            j["report"] = report_as_text
            j["meta_data"] = meta_data
            j["distill"] = result
            j["report_parts"] = parse_report(report_as_text)
            return jsonify(j)
        elif output == "text":
            return report_as_text or ""
        else:
            report_as_html, meta_data = get_with_file(engine, accession_number)
            return render_template(
                "distill.html",
                version=app.config["VERSION"],
                accession_number=accession_number,
                meta_data=meta_data,
                nlp=result,
                report=report_as_html,
            )

    @app.route("/download")
    def download():
        """Downloads the original RTF report."""
        accession_number = request.args.get("accession_number", "")
        if not accession_number:
            return ""
        engine = get_ris_db()
        report = get_as_rtf(engine, accession_number)
        response = make_response(report)
        cd = "attachment; filename={}.rtf".format(accession_number)
        response.headers["Content-Disposition"] = cd
        return response

    def get_ris_db():
        """Returns a connection to the Oracle db."""
        db = getattr(g, "_ris_database", None)
        if db is None:
            db = g._ris_database = open_connection()
        return g._ris_database

    def get_sectra_db():
        """Returns a connection to the Sectra CDWH"""
        db = getattr(g, "_sectra_cdwh_database", None)
        if db is None:
            db = g._sectra_cdwh_database = secta_cdwh_connection()
        return g._sectra_cdwh_database

    def get_usb_cdwh_db():
        """ Returns a connection to the USB CDWH"""
        db = getattr(g, "_usb_cdwh_database", None)
        if db is None:
            db = g._usb_cdwh_database = usb_cdwh_engine()
        return g._usb_cdwh_database

    return app
