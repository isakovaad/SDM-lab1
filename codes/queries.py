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

def print_query_results(records, summary):
    pp = pprint.PrettyPrinter(indent = 4)
    
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query = summary.query, records_count = len(records),
        time = summary.result_available_after,
    ))

    for record in records:
        pp.pprint(record.data())
        print()

def top_cited_papers(session):
    result = session.run(
        """
        MATCH (p:Paper)-[:cites]->(p2:Paper)<-[:presented_in]-(c:Conference)
        WITH c, p2, COUNT(p) as citations
        ORDER BY citations DESC
        RETURN c.name as Conference, p2.title as Paper, citations as Citations
        LIMIT 3
        """
        )
    records = list(result)
    summary = result.consume()
    return records, summary

def conference_community(session):
    result = session.run(
        """
        MATCH (c:Conference)<-[:presented_in]-(p:Paper)<-[:writes]-(a:Author)
        WITH c, a, COUNT(DISTINCT c.edition) AS editions
        WHERE editions >= 4
        RETURN c.name AS Conference, COLLECT(a.name) AS Community LIMIT 2;

        """
    )
    
    records = list(result)
    summary = result.consume()
    return records, summary

def impact_factor(session):
    result = session.run(
        """
        MATCH (p:Paper)-[r1:cites]->(citedP:Paper)-[r2:published_in]->(j:Journal)
        WITH j, r2.year AS pubYear, COUNT(DISTINCT p) AS totalCitations
        MATCH (p2:Paper)-[r3:published_in]->(j)
        WHERE r3.year = pubYear - 1 OR r3.year = pubYear - 2
        WITH j, pubYear, totalCitations, COLLECT(DISTINCT p2) AS publicationsInYears
        WITH j, pubYear, totalCitations, SIZE(publicationsInYears) AS totalPublications
        RETURN j.name AS journalName,
        pubYear AS yearOfPublication,
        totalCitations,
        totalPublications,
        CASE WHEN totalPublications > 0
        THEN toFloat(totalCitations) / totalPublications
        ELSE 0 END AS impactFactor
        ORDER BY impactFactor DESC
        LIMIT 5;

        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

def hindex(session):
    result = session.run(
        """
        MATCH (author:Author)-[:writes]->(paper:Paper)
        WITH author, paper
        OPTIONAL MATCH (paper)-[:cites]->(citedPaper)
        WITH author, paper, COUNT(citedPaper) AS citations
        ORDER BY citations DESC
        WITH author, COLLECT(citations) AS citationList
        WITH author, citationList, RANGE(0, SIZE(citationList)-1) AS indices
        UNWIND indices AS index
        WITH author, citationList, index
        WHERE citationList[index] >= index + 1
        WITH author, COLLECT(index + 1) AS possibleHIndices
        RETURN author.name as AuthorName, REDUCE(s = 0, i IN possibleHIndices | CASE WHEN i > s THEN i ELSE s END) AS hIndex
        ORDER BY hIndex DESC
        LIMIT 5;
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

print('Find the top 3 most cited papers of each conference.')
records, summary = session.execute_read(top_cited_papers)
print_query_results(records, summary)

print('For each conference find its community')
records, summary = session.execute_read(conference_community)
print_query_results(records, summary)

print('Find the impact factors of the journals in your graph ')
records, summary = session.execute_read(impact_factor)
print_query_results(records, summary)

print('Find the h-indexes of the authors in your graph')
records, summary = session.execute_read(hindex)
print_query_results(records, summary)

session.close()