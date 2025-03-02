# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

"""
-- Tabla que almacena los chunks 
CREATE OR REPLACE TABLE snowflake_coder.app.snowflake_documentation (
    category STRING NOT NULL,
    name STRING NOT NULL,
    part INT,
    definition STRING,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Vista para obtener los docuemntos completos

CREATE OR REPLACE VIEW snowflake_coder.app.full_document AS
SELECT 
    category,
    name,
    STRING_AGG(definition, ' ') WITHIN GROUP (ORDER BY part) AS full_documentation,
    MAX(updated_at) AS last_updated
FROM snowflake_coder.app.snowflake_documentation
GROUP BY category, name;

"""

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, flatten, current_timestamp


def main(session: snowpark.Session,
         doc_string: str,
         category: str,
         name: str) -> str:

    chunk_transformation = session.sql(
        '''
            SELECT SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
        ?,
        'none',
        1500,
        200
        ) AS definition
        ''',
        [doc_string]
    )

    chunk_transformation = chunk_transformation\
        .select(flatten(col("definition"), recursive=True))\
        .select("INDEX", "VALUE")\
        .with_column_renamed(new='part', existing='INDEX')\
        .with_column("category", lit(category))\
        .with_column("name", lit(name))\
        .with_column("updated_at", current_timestamp())

    chunk_transformation = chunk_transformation.write.mode("append")\
        .table("snowflake_coder.app.snowflake_documentation")

    return "Chunks stored in snowflake_coder.app.snowflake_documentation"
