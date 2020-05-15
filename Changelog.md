# Changelog

## Version 1.0.0
### Added
- Created jdbc folder containing DB Connection and DML queries.
- Added more transformations to SparkFunctions.java

## Version 0.1.2
### Changed
- Hello.java to MovieWebSer.java
- Webservlet 'webser' name to 'movie'
### Added

## Version 0.1.1
### Changed
- Refactored code - removed parsing from Hello.java to new Class ParseMovie.java 
### Added
- ParseMovie.java

## Version 0.1.0
### Changed
- MovieFromCSV.java 
### Added
- movies_test.csv
    - Simple version for test purposes
- MovieFromCSV.java
    - RDD parsed from CSV is now of type MovieClass
    
## Version 0.0.2
### Changed
- Refactored code - removed Spark Functions from Hello.java to new Class SparkFunctions
### Added
- SparkFunctions.java
    - Parsing simple CSV File with 1 column

## Version 0.0.1
### Added
- Maven
- Hello.java
    - Webservlet called 'webser'
    - Spark Function
        - parallelize from Array