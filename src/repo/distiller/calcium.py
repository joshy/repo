"""
Extract calcium score from report
"""

RESULT_KEY_CALCIUM_SCORE = 'calcium_score'

def extract_score(report, meta_data):
    result = {}
    if report is None:
        return result
    lines = [s.strip() for s in report.splitlines()]
    for l in lines:
        r = None
        if l.startswith('Coronararterie|Calcium-Score'):
            r = l
            result[RESULT_KEY_CALCIUM_SCORE] = _values(r)
    return result


def _values(line):
    """
    A example line could like this:
      Coronararterie|Calcium-Score|RCA| 0|LM| 0|LAD/RIVA| 0|CX| 0|Gesamt Calcium-Score| 0|||
    """
    if line is None:
        return {}
    values = line.split('|')
    result = {
        values[k]: values[k + 1]
        for k in range(2, len(values) - 2, 2) if values[k]
    }
    result = {k.strip() : v.strip() for k, v in result.items() if v is not None}
    return result