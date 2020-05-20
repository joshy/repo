# RIS/Review Report Server

## RIS Connection
A connection to the RIS database is established and the reports are downloaded
locally. This is GE Centricity specific. Also the view is very specific.

*BEWARE: This is not a general solution*

## System dependencies
 * Oracle driver


## URL
 * load specific report use URL
      `show?accession_number=<accession_number>`
 * load specific report, formatted as text
      `show?accession_number=<accession_number>&output=text`
