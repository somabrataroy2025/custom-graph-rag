import os
from dotenv import load_dotenv
from langchain_neo4j import Neo4jGraph
import pandas as pd
from pandas import DataFrame

load_dotenv()

#neo4j Driver

def db()->Neo4jGraph|None:
    return Neo4jGraph(
    url=os.getenv('NEO4J_URI'),
    username=os.getenv('NEO4J_USERNAME'),
    password=os.getenv('NEO4J_PASSWORD'),
    database='neo4j'
    )


def createSkills(input: DataFrame):
    df_person = pd.DataFrame(input)
    df_person['skills'] = df_person['skills'].str.split(',')
    driver = db()
    with driver:
        for index,item in df_person.iterrows():
            properties={
                "rows":{
                    'email':item['email'],
                    'name' : item['name'],
                    'skills' : item['skills']
                }
            }
            driver.query(
                """
                    UNWIND $rows as row
                    MERGE (p:Person {email:row.email})
                    set p.name = row.name
                    with p, row
                    foreach (item in row.skills |  
                            MERGE(s:Skill {name: rtrim(ltrim(item))}) 
                            MERGE (p)-[:KNOWS]->(s))
                """, properties
            )   
  
    
def createProjects(input: DataFrame):
    #add project node to db and map to person
    driver = db()
    with driver:
        for row,item in input.iterrows():
            properties = {
                'rows':{
                    'email': item['email'],
                    'project':item['project'],
                    'project_details':item['project_details']
                }
            }
            driver.query(
                """
                UNWIND $rows as row
                MERGE(p:Project{name:row.project})
                set p.project_details = row.project_details
                with row, p
                    MATCH(p1:Person{email: row.email})
                        MERGE(p1)-[:ASSIGNED_TO]->(p)

                """, properties
            )

def mapProjectSkills(input:DataFrame):
    df_projects = pd.DataFrame(input)
    df_projects['project_skills'] = df_projects['project_skills'].str.split(',')
    driver = db()
    with driver:
        for index,item in df_projects.iterrows():
            properties={
                "rows":{
                    'project_name' : item['project'],
                    'project_skills' : item['project_skills']
                }
            }
            driver.query(
                """
                    UNWIND $rows as row
                    MATCH (p:Project {name:row.project_name})
                    with p, row.project_skills as skills
                        unwind skills as skill
                        MATCH(s:Skill {name:skill})
                        MERGE (p)-[:IMPLEMENTS]->(s)
                """, properties
            )   
    
    