import json
import os
from typing import Dict, Optional, Tuple

FILE_PREFIX = 'ris-report-'


def write(accession_number: str, report: str,
          meta_data: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Writes the report to the file system and gives back the full path of the
    filename.
    """
    if report is not None and meta_data is not None:
        file_dir = "reports"
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        report_file_name = FILE_PREFIX + str(accession_number) + ".rtf"
        report_meta_name = FILE_PREFIX + str(accession_number) + ".json"
        report_dest = os.path.join(file_dir, report_file_name)
        meta_dest = os.path.join(file_dir, report_meta_name)

        with open(report_dest, 'w') as report_file, open(meta_dest,
                                                         'w') as meta_file:
            report_file.write(report)
            json.dump(meta_data, meta_file)
        return report_dest, meta_dest
    else:
        return None, None
