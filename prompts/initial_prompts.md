## Prompt: Column Added
- **Drift:** New column ```age``` (IntegerType) was added
- **Prompt:** Given a schema drift where a new column ```age``` of type IntegerType was added, generate PySpark code to update the existing DataFrame to include this new column, assuming a default value of ```null```.

## Prompt: Column Removed
- **Drift:** Column ```name``` (StringType) was removed
- **Prompt:** Given that the column ```name``` was removed from the schema, generate PySpark code to drop that column from an existing DataFrame.

## Prompt: Type Changed
- **Drift:** Column ```id``` changed from IntegerType to LongType
- **Prompt:** Column ```id``` changed from IntegerType to LongType. Update the DataFrame transformation logic to cast ```id``` appropriately before writing.