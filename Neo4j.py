# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 19:15:54 2022

@author: onais
"""

from neo4j import GraphDatabase

uri = "neo4j+s://14bca3e3.databases.neo4j.io"
driver = GraphDatabase.driver(uri, auth=("neo4j", "5SkGCc9Vyc3HUUhrjSqZ_xxBCX1N3x7gBgBA32JIy_I"))

def create_cluster_nodes(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)

def create_Record_nodes(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)




def create_relationship(tx, name, friend):
    tx.run("MATCH (a:Person) WHERE a.name = $name "
           "CREATE (a)-[:KNOWS]->(:Person {name: $friend})",
           name=name, friend=friend)

with driver.session() as session:
    session.execute_write(create_nodes, "Alivia Hogan")
    session.execute_write(create_nodes,"")
    session.execute_write(create_relationship, "Alice", "Bob")
    session.execute_write(create_relationship, "Alice", "Carl")
driver.close()