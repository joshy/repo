from .calcium import extract_score
from .aorta import extract_table
from .ventricle_function import extract_ventricle_function


def process(report, meta_data):
    if report or meta_data is None:
        return {}
    elif contains_aorta(meta_data.get("Untersuchung", "")):
        x = extract_score(report, meta_data)
        y = extract_table(report, meta_data)
        return {**x, **y}
    elif meta_data.get("Untersuchung", "") == "MRI Herz":
        return extract_ventricle_function(report, meta_data)
    else:
        return {}


def contains_aorta(study_description):
    return study_description.startswith(
        (
            "CT Herz",
            "CT Aorten",
            "CT Angio",
            "CT Thorax",
            "CT Linker",
            "CT Coronar",
            "CT Abdomen",
        )
    )
