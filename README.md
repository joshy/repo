# Report Server

## RIS Connection
A connection to the RIS database is established and the reports are downloaded
locally. This is GE Centricity specific. Also the view is very specific.

*BEWARE: This is not a general solution*

## Sectra CDWH
This can also show the reports from the Sectra CDWH.

## Pathology CDWH
The `/sectra?accession_number=<accession_number>` endpoint routes accessions
starting with a non-digit to the existing USB CDWH connection. It selects signed
reports for current orders and samples and returns only the most recently signed
one (latest Freigabe, `LRP_SIGNOFF_DATE_TS`). The output starts with a
`Berichtsart: <LRP_SEQUENCE>` line, followed by these sections: Zusammenfassung
(summary), Bericht (diagnosis), Kommentar, and Klinische Angaben. Null fields
have empty bodies.
Reindex affected studies in the crawler to update previously indexed reports.

## System dependencies
 * Oracle driver
 * MS SQL Server driver


## URL
 * load specific report use URL
      `show?accession_number=<accession_number>`
 * load specific report, formatted as text
      `show?accession_number=<accession_number>&output=text`
 * load report from the CDWH (Sectra for numeric accessions, pathology otherwise)
      as plain text: `sectra?accession_number=<accession_number>`,
      as HTML page: `sectra?accession_number=<accession_number>&output=html`.
      The search field in the web UI (Search button / Enter) uses this.
