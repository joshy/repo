"""
Extract aorta score from report
"""

RESULT_AORTA = 'aorta'

def extract_table(report, meta_data):
    result = {}
    if report is None:
        return result
    lines = [s.strip() for s in report.splitlines()]
    for l in lines:
        if l.startswith('Anulus') or l.startswith('Annulus'):
            result['Anulus'] = l.split('|')[1]
        if l.startswith('Sinusportion'):
            result['Sinusportion'] = l.split('|')[1]
        if l.startswith('Sinutubulärer'):
            result['Sinutubulärer'] = l.split('|')[1]
        if l.startswith('Aszendens'):
            result['Aszendens'] = l.split('|')[1]
        if l.startswith('Asz.'):
            result['Asz'] = l.split('|')[1]
        if l.startswith('Bogen zw'):
            result['Bogen zw'] = l.split('|')[1]
        if l.startswith('Bogen distal'):
            result['Bogen distal'] = l.split('|')[1]
        if l.startswith('Ao Aszendens Ø'):
            result['Ao Aszendens Ø'] = l.split('|')[1]
    return {'aorta': result}
