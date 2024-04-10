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

# def query1(session):
    
#     session.run(
#         """
#         MERGE (dbCommunity:Community {name: 'Database'})
#         SET dbCommunity.keywords = ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
#         """)
    
def query1(session):
    
    session.run(
        """
        CREATE (dbCommunity:Community {name: 'Database Community'})
        WITH dbCommunity
        UNWIND ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] AS keyword
        CREATE (k:Keyword {name: keyword})
        CREATE (k)-[:DEFINES]->(dbCommunity)
        """)
    
# def query2(session):
    
#     session.run(
#         """
#         //Conferences/Workshops related to the Database community

#         MATCH (c:Conference)-[:part_of]->(p:Proceedings)<-[:published_in]-(paper:Paper)
#         WITH c, p, COLLECT(paper) AS papers
#         UNWIND papers as paper
#         WITH c, p, COUNT(paper) AS totalPapers, 
#             SUM(CASE WHEN any(keyword IN paper.keyword WHERE keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']) THEN 1 ELSE 0 END) AS relevantPapers
#         WITH c, p, relevantPapers, totalPapers
#         WHERE relevantPapers >= 0.9 * totalPapers
#         MERGE (c)-[:RELATED_TO]->(:Community {name: 'Database'})

#         //Journals related to the Database community

#         MATCH (j:Journal)<-[:published_in]-(paper:Paper)
#         WITH j, COLLECT(paper) AS papers
#         UNWIND papers as paper
#         WITH j, COUNT(paper) AS totalPapers, 
#             SUM(CASE WHEN any(keyword IN paper.keyword WHERE keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']) THEN 1 ELSE 0 END) AS relevantPapers
#         WITH j, relevantPapers, totalPapers
#         WHERE relevantPapers >= 0.9 * totalPapers
#         MERGE (j)-[:RELATED_TO]->(:Community {name: 'Database'})

#         """)

def query2(session):
    
    session.run(
        """
        MATCH (p:Paper)-[:published_in]->(j:Journal)
        WHERE ANY(kw IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
                WHERE p.title CONTAINS kw OR p.abstract CONTAINS kw)
        WITH j, COUNT(p) AS relatedPapers
        MATCH (p2:Paper)-[:published_in]->(j:Journal)
        WITH j, relatedPapers, COUNT(p2) AS totalPapers
        WHERE relatedPapers >= 0.9 * totalPapers
        MATCH (dbCommunity:Community {name: 'Database Community'})
        CREATE (j)-[:RELATED_TO]->(dbCommunity)
        """)

# def query3(session):
    
#     session.run(
#         """
#         MATCH (paper:Paper)-[:cited]->(cited_paper:Paper),
#             (paper)-[:published_in]->(proceedings:Proceedings),
#             (conference:Conference)-[:part_of]->(proceedings),
#             (conference)-[:RELATED_TO]->(dbCommunity:Community {name: 'Database'})
#         WITH cited_paper, COUNT(paper) AS citations
#         ORDER BY citations DESC
#         LIMIT 100
#         SET cited_paper.topPaper = true

#         // Find top-cited papers within the database-related journals
#         MATCH (paper:Paper)-[:cited]->(cited_paper:Paper),
#             (paper)-[:published_in]->(journal:Journal),
#             (journal)-[:RELATED_TO]->(dbCommunity:Community {name: 'Database'})
#         WITH cited_paper, COUNT(paper) AS citations
#         ORDER BY citations DESC
#         LIMIT 100
#         SET cited_paper.topPaper = true

#         """)

def query3(session):
    
    session.run(
        """
        MATCH (p:Paper)-[:cites]->(cited:Paper), (p)-[:published_in]->(j:Journal)-[:RELATED_TO]->(dbCommunity:Community)
        WITH cited, COUNT(p) AS citations
        ORDER BY citations DESC
        LIMIT 100
        MATCH (dbCommunity)
        CREATE (cited)-[:TOP_PAPER]->(dbCommunity)

        """)
    
# def query4(session):
    
#     session.run(
#         """
#         MATCH (author:Author)-[:writes]->(paper:Paper)
#         WHERE paper.topPaper = true
#         WITH author, COUNT(paper) AS papersCount
#         SET author.potentialReviewer = CASE WHEN papersCount > 0 THEN true ELSE null END

#         // Identify gurus who are authors of at least two top-100 papers
#         WITH author, papersCount
#         WHERE papersCount >= 2
#         SET author.guru = true
#         """)

def query4(session):
    
    session.run(
        """
        MATCH (author:Author)-[:writes]->(p:Paper)-[:TOP_PAPER]->(dbCommunity:Community)
        WITH author, COUNT(p) AS papersCount
        WHERE papersCount > 0
        CREATE (author)-[:POTENTIAL_REVIEWER_FOR]->(dbCommunity)
        WITH author, papersCount
        WHERE papersCount >= 2
        CREATE (author)-[:GURU_FOR]->(dbCommunity)
        """)


print('1. Define the database community based on keywords')
session.execute_write(query1)

print('2. Find conferences and journals where 90% of papers contain keywords related to the database community')
session.execute_write(query2)

print('3. Find top-cited papers within the database-related conferences/workshops')
session.execute_write(query3)

print('4. Identify potential reviewers who are authors of the top-100 papers')
session.execute_write(query4)

print('All queries have been executed')

session.close()