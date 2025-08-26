import pandas as pd
from pandas import DataFrame
from typing import Any
from langchain_openai import AzureChatOpenAI,AzureOpenAIEmbeddings
import os
from dotenv import load_dotenv
from langchain_neo4j import GraphCypherQAChain,Neo4jGraph

load_dotenv()

llm = AzureChatOpenAI(  
    azure_deployment="gpt-4.1",  # or your deployment
    api_version="2024-12-01-preview",  # or your api version
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

embedding = AzureOpenAIEmbeddings(
    azure_deployment="text-embedding-3-small",
    api_version="2024-12-01-preview"
)


df = pd.read_csv("../../docs/talent.csv")

# modification is required
# data lose while returning the final output
def convert_df(input: Any, col:str, splitBy = None)->DataFrame|None: 
    df1 = pd.DataFrame(input)
    df1 = df1[[col]]
    col_trans = 'transformed'
    if col in df1.columns:
        if splitBy is not None:
            df1[col_trans] =  df1[col].str.split(splitBy)
            df1[col_trans] = df1[col_trans].explode(col_trans).drop_duplicates().reset_index(drop=True)  
        else:
            df1[col_trans] = df1[col].drop_duplicates().reset_index(drop=True)
            df1 = df1.dropna()
    else:
        return None
    return df1


def embed_df(input:Any)->DataFrame|None:
    if input is not None:
        df = pd.DataFrame(input)
        df["embedding"] = df['transformed'].apply(lambda item: embedding.embed_query(item[0]))
        return df 
    else:
        return None

