import unittest
from unittest.mock import MagicMock, patch

from src.repo.database.usb_cdwh import get_patho_report


class TestPathologyReport(unittest.TestCase):
    def setUp(self):
        self.engine = MagicMock()
        self.connection = self.engine.connect.return_value.__enter__.return_value
        self.rows = self.connection.execute.return_value.mappings.return_value
        self.row = {
            "summary": "Example summary",
            "diagnose": "Example diagnosis",
            "comment": "Example comment",
            "clinical_information": "Example clinical information",
            "report_art": "Hauptbericht",
        }
        self.expected = (
            "Berichtsart: Hauptbericht\n\n"
            "Zusammenfassung\nExample summary\n\n"
            "Bericht\nExample diagnosis\n\n"
            "Kommentar\nExample comment\n\n"
            "Klinische Angaben\nExample clinical information"
        )

    def test_sections_and_bound_query(self):
        self.rows.first.return_value = self.row
        self.assertEqual(get_patho_report(self.engine, "TEST-001"), self.expected)
        query, parameters = self.connection.execute.call_args.args
        self.assertEqual(parameters, {"accession_number": "TEST-001"})
        self.assertIn(":accession_number", str(query))
        self.assertIn(
            "order by r.LRP_SIGNOFF_DATE_TS desc nulls last, r.lrp_bk desc", str(query)
        )
        self.assertIn("limit 1", str(query))
        self.assertIn("o.dwh_is_current = 1", str(query))
        self.assertIn("s.dwh_is_current = 1", str(query))
        self.assertIn("r.lrp_status = 'Signed'", str(query))
        # Samples only select the order; a direct join repeats each report per sample.
        self.assertNotIn("join CDWH.V_IL_DIM_LABOR_SAMPLE_PATHOLOGY", str(query))
        self.assertIn("o.lop_bk in (", str(query))

    def test_only_latest_report_is_formatted(self):
        self.rows.first.return_value = self.row
        report = get_patho_report(self.engine, "TEST-001")
        self.assertEqual(report.count("Zusammenfassung"), 1)
        self.rows.all.assert_not_called()

    def test_null_fields(self):
        self.rows.first.return_value = dict.fromkeys(self.row)
        self.assertEqual(
            get_patho_report(self.engine, "TEST-001"),
            "Berichtsart: \n\nZusammenfassung\n\n\nBericht\n\n\nKommentar\n\n\nKlinische Angaben\n",
        )

    def test_no_reports(self):
        self.rows.first.return_value = None
        self.assertEqual(get_patho_report(self.engine, "TEST-001"), "No patho report found")

    def test_database_errors_propagate(self):
        self.connection.execute.side_effect = RuntimeError("Database unavailable")
        with self.assertRaises(RuntimeError):
            get_patho_report(self.engine, "TEST-001")

    def test_sectra_route_returns_pathology_report(self):
        import web

        # Do not read deployment secrets or connect to clinical systems.
        with patch("web.load_dotenv"), patch("flask.Config.from_pyfile"), \
                patch("web.usb_cdwh_engine", return_value=self.engine), \
                patch("web.get_patho_report", return_value=self.expected) as fetch:
            app = web.create_app()
            response = app.test_client().get("/sectra?accession_number=TEST-001")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(as_text=True), self.expected)
        fetch.assert_called_once_with(self.engine, "TEST-001")
