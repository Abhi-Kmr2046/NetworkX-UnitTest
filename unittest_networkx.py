import psycopg2
from psycopg2 import sql
import networkx as nx
import pytest
from pytest_postgresql import factories
from age import *

postgresql_my_proc = factories.postgresql_proc()

postgresql_my = factories.postgresql('postgresql_my_proc')


def compare_networkX(G, H):
    if G.number_of_nodes() != H.number_of_nodes():
        return False
    if G.number_of_edges() != H.number_of_edges():
        return False
    
    # test nodes
    nodes_G, nodes_H = G.number_of_nodes(), H.number_of_nodes()
    markG, markH = [0]*nodes_G, [0]*nodes_H
    nodes_list_G, nodes_list_H = list(G.nodes), list(H.nodes)

    for i in range(0, nodes_G):
        for j in range(0, nodes_H):
            if markG[i]==0 and markH[j]==0:
                node_id_G = nodes_list_G[i]
                property_G = G.nodes[node_id_G]

                node_id_H = nodes_list_H[i]
                property_H = H.nodes[node_id_H]

                if property_G == property_H:
                    markG[i] = 1
                    markH[j] = 1

    if any(elem == 0 for elem in markG):
        return False
    if any(elem == 0 for elem in markH):
        return False
    
    # test edges
    edges_G, edges_H = G.number_of_edges(), H.number_of_edges()
    markG, markH = [0]*edges_G, [0]*edges_H
    edges_list_G, edges_list_H = list(G.edges), list(H.edges)

    for i in range(0, edges_G):
        for j in range(0, edges_H):
            if markG[i]==0 and markH[j]==0:
                source_G, target_G = edges_list_G[i]
                property_G = G.edges[source_G, target_G]

                source_H, target_H = edges_list_H[i]
                property_H = H.edges[source_G, target_G]

                if property_G == property_H:
                    markG[i] = 1
                    markH[j] = 1

    if any(elem == 0 for elem in markG):
        return False
    if any(elem == 0 for elem in markH):
        return False

    return True


def test_networkX1(postgresql):
    # Expected Graph
    G = nx.DiGraph()

    # Apache AGE of expected
    with postgresql.cursor() as cursor:
        query = """CREATE EXTENSION age; LOAD 'age'; SET search_path = ag_catalog, "$user", public;
                    SELECT * FROM ag_catalog.create_graph('test_graph');"""
        cursor.execute(query)

    # Convert Apache AGE to NetworkX 
    # Not Done yet
    H = nx.DiGraph()
    assert compare_networkX(G,H)

def test_networkX2(postgresql):

    # Expected Graph
    G = nx.DiGraph()

    G.add_node('1', 
           label='l1',
           properties={'name' : 'n1',
                       'weight' : '5'})
    G.add_node('2', 
           label='l1', 
           properties={'name': 'n2' ,
                       'weight' : '4'})
    G.add_node('3', 
           label='l1', 
           properties={'name': 'n3' ,
                       'weight' : '9'})
    G.add_edge('1', '2', label='e1', properties={'property' : 'graph'} )
    G.add_edge('2', '3', label='e2', properties={'property' : 'node'} )

    # Apache AGE of expected
    with postgresql.cursor() as cursor:
        query = """CREATE EXTENSION age; LOAD 'age'; SET search_path = ag_catalog, "$user", public;
                    SELECT * FROM ag_catalog.create_graph('test_graph');"""
        cursor.execute(query)
   
    with postgresql.cursor() as cursor:
        query = """SELECT * FROM cypher('test_graph', $$ CREATE (:l1 {name: 'n1', weight: '5'}) $$) as (n agtype);
                    SELECT * FROM cypher('test_graph', $$ CREATE (:l1 {name: 'n2', weight: '4'}) $$) as (n agtype);
                    SELECT * FROM cypher('test_graph', $$ CREATE (:l1 {name: 'n3', weight: '9'}) $$) as (n agtype);"""
        cursor.execute(query)

    with postgresql.cursor() as cursor:
        query = """SELECT * 
                        FROM cypher('test_graph', $$
                            MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n1' AND b.name = 'n2'
                            CREATE (a)-[e:e1 {property:'graph'}]->(b)
                            RETURN e
                        $$) as (e agtype);
                    SELECT * 
                        FROM cypher('test_graph', $$
                            MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n2' AND b.name = 'n3'
                            CREATE (a)-[e:e2 {property:'node'}]->(b)
                            RETURN e
                        $$) as (e agtype);"""
        cursor.execute(query)
       

    with postgresql.cursor() as cursor:
        query = """SELECT * FROM cypher('test_graph', $$ MATCH ()-[r]->() RETURN r $$) as (edges agtype);"""
        cursor.execute(query)
        ret = cursor.fetchall()
        print(ret)

    # Convert Apache AGE to NetworkX 
    # Not Done yet
    H = nx.DiGraph()

    H.add_node('1', 
           label='l1',
           properties={'name' : 'n1',
                       'weight' : '5'})
    H.add_node('2', 
           label='l1', 
           properties={'name': 'n2' ,
                       'weight' : '4'})
    H.add_node('3', 
           label='l1', 
           properties={'name': 'n3' ,
                       'weight' : '9'})
    H.add_edge('1', '2', label='e1', properties={'property' : 'graph'} )
    H.add_edge('2', '3', label='e2', properties={'property' : 'node'} )

    assert compare_networkX(G,H)