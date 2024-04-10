import pprint
from neo4j import GraphDatabase

def create_session():
  username = 'neo4j'
  password = 'Test@123'

  print('Creating a connection with neo4j...')
  driver = GraphDatabase.driver("bolt://localhost:7687", auth=(username, password))

  session = driver.session()
  print('Session Initiated....')

  return session

session = create_session()

def print_results(records, summary):
    pp = pprint.PrettyPrinter(indent = 4)
    
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query = summary.query, records_count = len(records),
        time = summary.result_available_after,
    ))

    for record in records:
        pp.pprint(record.data())
        print()

def page_rank(session):
    
    session.run(
        """
        CALL gds.graph.drop('papersGraph',false);
        """)

    session.run(
        """
        CALL gds.graph.project(
            'papersGraph',
            'Paper',
            'cites',
            {
                relationshipProperties: 'year'
            }
        );
        """)
    
    print('Please wait.....')
    
    result = session.run(
        """
        CALL gds.pageRank.stream('papersGraph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS paper, score
        ORDER BY score DESC
        LIMIT 10;
        """
    )
    
    records = list(result)
    summary = result.consume()
    return records, summary

def node_similarity(session):
    
    session.run(
        """
        CALL gds.graph.drop('papersKeywordGraph',false);
        """)

    session.run(
        """
        CALL gds.graph.project(
            'papersKeywordGraph',
            'Paper',
            {
                HAS_SIMILAR_KEYWORD: {
                    type: 'HAS_SIMILAR_KEYWORD',
                    orientation: 'UNDIRECTED'
                }
            }
        );
        """)
    
    print('Please wait.....')
    
    result = session.run(
        """
        CALL gds.nodeSimilarity.stream('papersKeywordGraph')
        YIELD node1, node2, similarity
        RETURN gds.util.asNode(node1).title AS Paper1, 
            gds.util.asNode(node2).title AS Paper2, 
            similarity
        ORDER BY similarity DESC, Paper1, Paper2
        LIMIT 10;
        """
    )
    
    records = list(result)
    summary = result.consume()
    return records, summary

print('Page Rank Algorithm')
records, summary = session.execute_read(page_rank)
print_results(records, summary)

print('Node Similarity Algorithm')
records, summary = session.execute_read(node_similarity)
print_results(records, summary)

session.close()