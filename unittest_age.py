import psycopg2
from psycopg2 import sql
import networkx as nx
import pytest
from pytest_postgresql import factories
from age import *



postgresql_my_proc = factories.postgresql_proc()

postgresql_my = factories.postgresql('postgresql_my_proc')

def compare_age(postgresql, graph1, graph2):
    g_nodes, g_edges = None, None

    
    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM cypher('{graph1}', $$ MATCH (v) RETURN v $$) as (v agtype);""" 
        
        cursor.execute(query)

        g_nodes = cursor.fetchall()
        for row in g_nodes:
            print(row, type(row))

    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM cypher('{graph1}', $$ MATCH ()-[r]->() RETURN r $$) as (edges agtype);""" 
        
        cursor.execute(query)
        g_edges = cursor.fetchall()

        for row in g_edges:
            print(row)

    h_nodes, h_edges = None, None

    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM cypher('{graph2}', $$ MATCH (v) RETURN v $$) as (v agtype);""" 
        
        cursor.execute(query)
        h_nodes = cursor.fetchall()

        for row in h_nodes:
            print(row)


    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM cypher('{graph2}', $$ MATCH ()-[r]->() RETURN r $$) as (edges agtype);""" 
        
        cursor.execute(query)
        h_edges = cursor.fetchall()

        for row in h_edges:
            print(row)
    
    if len(g_nodes)!=len(h_nodes) or len(g_edges)!=len(h_edges):
        return False

    return True

def test_age1(postgresql):

    graph1, graph2 = "test_graph1", "test_graph2"

    # Expected Graph
    with postgresql.cursor() as cursor:
        query = f"""CREATE EXTENSION age; LOAD 'age'; SET search_path = ag_catalog, "$user", public;
                    SELECT * FROM ag_catalog.create_graph('{graph1}');"""
        cursor.execute(query)

    # NetworkX graph of expected
    G = nx.DiGraph()
    

    # Convert NetworkX to Apache AGE 
    # Not Done yet
    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM ag_catalog.create_graph('{graph2}');"""
        cursor.execute(query)
    assert compare_age(postgresql, graph1, graph2)


def test_age2(postgresql):

    graph1, graph2 = "test_graph1", "test_graph2"

    # Expected Graph
    with postgresql.cursor() as cursor:
        query = f"""CREATE EXTENSION age; LOAD 'age'; SET search_path = ag_catalog, "$user", public;
                    SELECT * FROM ag_catalog.create_graph('{graph1}');"""
        cursor.execute(query)
   
    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM cypher('{graph1}', $$ CREATE (:l1 {{name: 'n1', weight: '5'}}) $$) as (n agtype);
                    SELECT * FROM cypher('{graph1}', $$ CREATE (:l1 {{name: 'n2', weight: '4'}}) $$) as (n agtype);
                    SELECT * FROM cypher('{graph1}', $$ CREATE (:l1 {{name: 'n3', weight: '9'}}) $$) as (n agtype);"""
        cursor.execute(query)

    with postgresql.cursor() as cursor:
        query = f"""SELECT * 
                        FROM cypher('{graph1}', $$
                            MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n1' AND b.name = 'n2'
                            CREATE (a)-[e:e1 {{property:'graph'}}]->(b)
                            RETURN e
                        $$) as (e agtype);
                    SELECT * 
                        FROM cypher('{graph1}', $$
                            MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n2' AND b.name = 'n3'
                            CREATE (a)-[e:e2 {{property:'node'}}]->(b)
                            RETURN e
                        $$) as (e agtype);"""
        cursor.execute(query)

    # NetworkX graph of expected
    G = nx.DiGraph()
    

    # Convert NetworkX to Apache AGE 
    # Not Done yet
    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM ag_catalog.create_graph('{graph2}');"""
        cursor.execute(query)
   
    with postgresql.cursor() as cursor:
        query = f"""SELECT * FROM cypher('{graph2}', $$ CREATE (:l1 {{name: 'n1', weight: '5'}}) $$) as (n agtype);
                    SELECT * FROM cypher('{graph2}', $$ CREATE (:l1 {{name: 'n2', weight: '4'}}) $$) as (n agtype);
                    SELECT * FROM cypher('{graph2}', $$ CREATE (:l1 {{name: 'n3', weight: '9'}}) $$) as (n agtype);"""
        cursor.execute(query)

    with postgresql.cursor() as cursor:
        query = f"""SELECT * 
                        FROM cypher('{graph2}', $$
                            MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n1' AND b.name = 'n2'
                            CREATE (a)-[e:e1 {{property:'graph'}}]->(b)
                            RETURN e
                        $$) as (e agtype);
                    SELECT * 
                        FROM cypher('{graph2}', $$
                            MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n2' AND b.name = 'n3'
                            CREATE (a)-[e:e2 {{property:'node'}}]->(b)
                            RETURN e
                        $$) as (e agtype);"""
        cursor.execute(query)
    assert compare_age(postgresql, graph1, graph2)