from collections import OrderedDict
import re
LVEF = 'Linksventrikuläre Auswurffraktion (LVEF)'
RVEF = 'Rechtsventrikuläre Auswurffraktion (RVEF)'
EDV = 'Enddiastolisches Volumen (EDV)'
EDVI = 'Enddiastolisches Volumen indexiert (EDVI)'
ESV = 'Endsystolisches Volumen (ESV)'
ESVI = 'Endsystolisches Volumen indexiert (ESVI)'
SV = 'Schlagvolumen (SV)'
SVI = 'Schlagvolumen indexiert (SVI)'
ED = 'Myokardmasse (ED)'
EDI = 'Myokardmasse indexiert (ED)'

RESULT_KEY_VENCTRICLE_FUNCTION = 'ventricle_function'

LEFT_INDEX_START_V1 = 'Linksventrikuläre.*Funktion.*Norm.*Mann.*emessen'
RIGHT_INDEX_START_V1 = 'Rechtsventrikuläre.*Funktion.*Norm.*Mann.*emessen'

LEFT_INDEX_START_V2 = 'Linksventrikuläre.*Funktion.*gemessen.*Norm.*Mann'
RIGHT_INDEX_START_V2 = 'Rechtsventrikuläre.*Funktion.*gemessen.*Norm.*Mann'


def extract_ventricle_function(report, meta_data):

    lines = [s.strip() for s in report.splitlines()]
    left_index_v1 = [i for i,s in enumerate(lines) if re.match(LEFT_INDEX_START_V1, s)]
    right_index_v1 = [i for i,s in enumerate(lines) if re.match(RIGHT_INDEX_START_V1, s)]

    left_index_v2 = [i for i,s in enumerate(lines) if re.match(LEFT_INDEX_START_V2, s)]
    right_index_v2 = [i for i,s in enumerate(lines) if re.match(RIGHT_INDEX_START_V2, s)]

    left = OrderedDict()
    right = OrderedDict()

    left_index = left_index_v1 or left_index_v2
    right_index = right_index_v1 or right_index_v2
    variant = 1 if left_index_v1 else 2
    if left_index:
        for l in lines[left_index[0] : (left_index[0] + 10)]:
            left = _extract(l, LVEF, left, variant)
            left = _extract(l, EDV, left, variant)
            left = _extract(l, EDVI, left, variant)
            left = _extract(l, ESV, left, variant)
            left = _extract(l, ESVI, left, variant)
            left = _extract(l, SV, left, variant)
            left = _extract(l, SVI, left, variant)
            left = _extract(l, ED, left, variant)
            left = _extract(l, EDI, left, variant)

    if right_index:
        for l in lines[right_index[0] : right_index[0] + 9]:
            right = _extract(l, RVEF, right, variant)
            right = _extract(l, EDV, right, variant)
            right = _extract(l, EDVI, right, variant)
            right = _extract(l, ESV, right, variant)
            right = _extract(l, ESVI, right, variant)
            right = _extract(l, SV, right, variant)
            right = _extract(l, SVI, right, variant)

    return { RESULT_KEY_VENCTRICLE_FUNCTION : {'left': left, 'right': right}}


def _extract(line, prefix, result, variant):
    parts = line.split('|')
    if line.startswith(prefix) and len(parts) > 2:
        if variant == 1:
            result[parts[0].strip()] = OrderedDict({
                'norm': parts[1].strip(),
                'gemessen': parts[2].strip()
            })
        else:
            result[parts[0].strip()] = OrderedDict({
                'norm': parts[2].strip(),
                'gemessen': parts[1].strip()
            })
        return result
    else:
        return result