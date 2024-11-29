import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)


def get_patho_report(engine, accession_number):
    sql = """
        SELECT
            lrp_diagnosis as report
        FROM
            CDWH.v_il_fct_labor_report_pathology AS rp
        JOIN CDWH.V_IL_DIM_LABOR_SAMPLE_PATHOLOGY vidlsp ON
            rp.LOP_BK = VIDLSP.LOP_BK
        WHERE
            vidlsp.LSP_SAMPLE_CODE = :accession_number
            AND rp.LRP_STATUS = 'Signed'
            AND vidlsp.DWH_IS_CURRENT = 1

        """
    with engine.connect() as con:
        result = con.execute(text(sql), {"accession_number": accession_number})
        results = result.mappings().all()
        if len(results) > 0:
            logger.warning(
                f"Warning to many results for accession nr: {accession_number}, returning from first result"
            )
            return results[0].get("report")
        return results[0].get("report")
