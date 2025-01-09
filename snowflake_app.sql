CREATE DATABASE BASIC_KNOWLEDGE_GRAPH;
CREATE SCHEMA DATA;

CREATE OR REPLACE FUNCTION text_chunker(pdf_text STRING)
RETURNS TABLE (chunk VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'text_chunker'
PACKAGES = ('snowflake-snowpark-python', 'langchain')
AS
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1512, 
            chunk_overlap=256, 
            length_function=len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;

CREATE OR REPLACE STAGE docs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE (
    RELATIVE_PATH VARCHAR, 
    SIZE NUMBER, 
    FILE_URL VARCHAR, 
    SCOPED_FILE_URL VARCHAR, 
    CHUNK VARCHAR, 
    CATEGORY VARCHAR
);

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- More SQL operations here...

