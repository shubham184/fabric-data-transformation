# 360 Layered Configuration - Walking Skelleton

## CTAS as a core principle for defining transformations
For transforming data, CTAS mechanism is used (Create Table As Select), similar to the core concept of DBT (Data Build Tool)
this approach leverages
- SQL as a universal language
- capabilities of relational databases, SparkSQL (and others)
- expertise and best practices from DBT community

## Layering and merge logic
The available layers for defining the configuration are 0_Product, 1_Industry and 2_Customer
Configuration files are processed in this order allowing for the latter to overwrite the previous.

With the current setup, properties defined at a higher level will overwrite the properties at the lower level. Future extensions could include the conditional nature of an override, eg based on a feature flag or a version condition.

### Lists
Lists (columns, ctes, ...) can be layered on top uysing the "operator" property:
- "+" (=default value) -> element is added to the list
- "-" -> element is removed from the list
- "U" -> element is updated in the list

### Partial configuration files
the configuration for a single element can be cut in smaller pieces, allowing for easier maintenance and version control:
MY_ELEMENT.partx.json
MY_ELEMENT.party.json

Each of these pieces will be merged independently

## building the configuration
* * Work In Progress * *
The build.py script will generate the build artefacts in the .build folder:
- json file for each element, containing the merged result of all layers and partials
- sql file (currently only for b2s) that is generated as source for the data transformations

The current version:
- only looks at BronzeToSilver (B2S)
- supports layering and partial configfiles, but conditional merges, eg based on version or feature flags hasn't been added
- does not yet include json schema validation
- has a simplified output rendering

## running the script locally
make use of the uv tool to install dependencies, create a virtual environment and run the script

''' sh
uv run buildtools/build.py
''' sh