from sqlalchemy import text


def get_patho_report(engine, accession_number):
    # Use the sample table only to find the order: joining it directly would
    # repeat every report once per sample row of the same B-number.
    # Only the most recently signed report (latest Freigabe) is returned.
    sql = """
        select
            r.lrp_bk,
            r.LRP_SEQUENCE as report_art,
            o.LOP_ENTRY_DATE_TS as zp01_einsendung,
            r.LRP_SIGNOFF_DATE_TS as zp15_freigabe,
            r.LRP_DIAGNOSIS AS diagnose,
            r.LRP_COMMENT AS comment,
            r.LRP_SUMMARY AS summary,
            o.LOP_CLINICAL_INFORMATION AS clinical_information
        from CDWH.V_IL_DIM_LABOR_ORDER_PATHOLOGY o
        join CDWH.V_IL_FCT_LABOR_REPORT_PATHOLOGY r on r.lop_bk = o.lop_bk
        where o.dwh_is_current = 1
            and r.lrp_status = 'Signed'
            and o.lop_bk in (
                select s.lop_bk
                from CDWH.V_IL_DIM_LABOR_SAMPLE_PATHOLOGY s
                where s.dwh_is_current = 1
                    and s.lsp_sample_code = :accession_number
            )
        order by r.LRP_SIGNOFF_DATE_TS desc nulls last, r.lrp_bk desc
        limit 1
        """
    with engine.connect() as con:
        result = con.execute(text(sql), {"accession_number": accession_number})
        row = result.mappings().first()
        if row is None:
            return "No patho report found"
        return format_patho_report(row)


def format_patho_report(row):
    """Format a signed report: Berichtsart line, then the sections in order."""
    sections = [f"Berichtsart: {row.get('report_art') or ''}"]
    sections += [
        f"{heading}\n{row.get(field) or ''}"
        for heading, field in (
            ("Zusammenfassung", "summary"),
            ("Diagnose", "diagnose"),
            ("Kommentar", "comment"),
            ("Klinische Angaben", "clinical_information"),
        )
    ]
    return "\n\n".join(sections)
