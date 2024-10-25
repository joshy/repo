import os
import sys

from typing import Dict, List

contexts = ('Anamnese', 'Technik', 'Fragestellung', 'Befund', 'Beurteilung')


def parse(text: List[str]) -> Dict[str, str]:
    """ Parses a ris report into sections.
    A few assumptions are made:
     * A new line ends any section, which holds to be true for most of the
       reports
    """
    section = ''
    values = [] # type: List[str]
    data = {}
    # remove empty lines
    text = [x for x in text if x]

    for line in text:
        if line.startswith(contexts):
            # Since we know filter(line.startswith, contexts) will get exactly
            # one hit, we just pull the hit using next
            current_section = next(filter(line.startswith, contexts))
            # if there is a new section save the current context to data
            if section and section != current_section:
                data[section.lower()] = ''.join(values)
                values = []
            section = current_section
        else:
            values.append(line)
    # add last element
    if not section:
        section = 'Datenimport'
    data[section.lower()] = ''.join(values)
    return data