# Changelog

## v3.2.9 - 05.06.2020
 * Added 'Aufnahmeart' in json output 

## v3.2.8 - 30.03.2020
 * Fixed bug in json api and added patientid to meta data

## v3.2.7 - 07.06.2019
 * Diff for accession number by befund id

## v3.2.6 - 07.05.2019
 * Diff query now possible by accession number

## v3.2.5 - 02.03.2019
 * Fixed wrong table name, was accidently commited

## v3.2.4 - 13.03.2019
 * Added more metadata to report view

## v3.2.3 - 15.01.2019
 * More aorta data extraction added

## v3.2.2 - 31.10.2018
 * Changed from unters_date to befund_date which is never empty

## v3.2.1 - 03.09.2018
 * Heart ventricle functions tested with over 1'000 reports

## v3.2.0 - 31.07.2018
 * Extraction of tables added to master.

## v3.1.0 - 25.07.2018
 * Integrated Kevin Streiters Bachelor thesis work. Thanks for the awesome work!

## v3.0.4 - 26.02.2018
 * Dashboard was always showing old data (side-effect of showing last exams)

## v3.0.3 - 09.02.2018
 * Download RTF report

## v3.0.2 - 05.02.2018
 * Dashboard query changed from days to number of exams

## v3.0.1 - 22.01.2018
 * Fixed output=text for invalid acc nr

## v3.0.0 - 22.01.2018
 * Dashboard for personal use added

## v2.0.4 - 01.11.2017
 * Ris reports per day working again, default is not parsing the text

## v2.0.3 - 13.10.2017
 * Diff with different report status

## v2.0.2 - 11.10.2017
 * Download report as text link added

## v2.0.1 - 27.09.2017
 * Request param output=text does not store the file anymore

## v2.0.0 - 04.09.2017
 * First version of report review functionality

## v1.1.0 - 16.08.2017
 * Add contrast medium query

## v1.0.0 - 14.07.2017
 * Can now query all reports per a specific day and outputs json
 * RIS Reports query per day splits the report into sections, see parse.py

## v0.0.5 - 15.06.2017
 * &output=text outputs now only plain text without html

## v0.0.4 - 13.03.2017
 * Removed unrtf dependency because it crashed sometimes

## v0.0.3 - 07.02.2017
 * Fixed wrong/not existing Accession Number entered

## v0.0.2 - 31.01.2017
 * Added error handling for
   - wrong accession number -> error is show
   - empty accession number -> renders main page
 * File open with 'with' statement for autmatic closing

## v0.0.1 - 09.01.2017
 * Initial version
