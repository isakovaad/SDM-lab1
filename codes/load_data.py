import os
from neo4j import GraphDatabase

def delete_and_detach_all_nodes(session):
  session.run(
      "MATCH (n) DETACH DELETE n"
  )
  
def create_session():
  username = 'neo4j'
  password = 'Test@123'

  print('Creating a connection with neo4j...')
  driver = GraphDatabase.driver("bolt://localhost:7687", auth=(username, password))

  session = driver.session()
  print('Session Initiated....')

  return session

def clean_session(session):
  print('Deleting and detaching all the previous nodes in the database.')
  session.execute_write(delete_and_detach_all_nodes)

  return session

def load_node_author_semantic(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/authors_node_semantic.csv' AS line
          CREATE (:Author {
              ID: line.ID,
              name: line.name,
              email: line.email,
              field: line.field
      })"""
  )

def load_node_conference_semantic(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/conference_semantic.csv' AS line
          CREATE (:Conference {
              ID: line.ID,
              name: line.name,
              year: toInteger(line.year),
              edition: toInteger(line.edition),
              city: line.city
      })"""
  )

def load_node_journal_semantic(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/journal_semantic.csv' AS line
          CREATE (:Journal {
              ID: line.ID,
              name: line.name
      })"""
  )

def load_node_proceeding_semantic(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/proceedings_semantic.csv' AS line
          CREATE (:Proceeding {
              ID: line.ID,
              name: line.name
      })"""
  )

def load_node_paper_semantic(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/papers_semantic.csv' AS line
          CREATE (:Paper {
              ID: line.ID,
              title: line.title,
              abstract: line.abstract,
              pages: line.pages,
              link: line.link,
              keyword: line.keyword,
              year: toInteger(line.year)
      })"""
  )

def load_relation_conference_ispart_proceeding(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/conference_part_of_proceedings.csv' AS line
          MATCH (conf:Conference {ID: line.CONFERENCE_ID})
          WITH conf, line
          MATCH (proc:Proceeding {ID: line.PROCEEDING_ID})
          CREATE (conf)-[:is_part]->(proc)"""
  )

def load_relation_author_writes_paper(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/author_writes_papers.csv' AS line
          MATCH (author:Author {ID: line.AUTHOR_ID})
          WITH author, line
          MATCH (paper:Paper {ID: line.PAPER_ID})
          CREATE (author)-[w:writes]->(paper)
          SET w.corresponding_author = toBoolean(line.corresponding_author)"""
  )

def load_relation_paper_presentedin_conference(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/paper_presented_in_conference.csv' AS line
          MATCH (paper:Paper {ID: line.PAPER_ID})
          WITH paper, line
          MATCH (conf:Conference {ID: line.CONFERENCE_ID})
          CREATE (paper) - [:presented_in] -> (conf)"""
  )

def load_relation_paper_publishedin_journal(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/paper_published_in_journal.csv' AS line
          MATCH (paper:Paper {ID: line.PAPER_ID})
          WITH paper, line
          MATCH (jour:Journal {ID: line.JOURNAL_ID})
          CREATE (paper) - [r:published_in] -> (jour)
          SET r.volume = toInteger(line.volume), r.year = toInteger(line.year)"""
  )

def load_relation_paper_cites_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///data/paper_cites_paper.csv' AS line
            MATCH (paper:Paper {ID: line.PAPER_ID})
            WITH paper, line
            MATCH (citedPaper:Paper {ID: line.CITED_PAPER_ID})
            CREATE (paper) - [:cites] -> (citedPaper)"""
    )

def load_relation_author_reviews_paper(session):
  session.run(
      """LOAD CSV WITH HEADERS FROM 'file:///data/author_review_papers.csv' AS line
          MATCH (author:Author {ID: line.AUTHOR_ID})
          WITH author, line
          MATCH (paper:Paper {ID: line.PAPER_ID})
          CREATE (author) - [:reviews] -> (paper)"""
  )

session = create_session()
session = clean_session(session)

print('Creating and loading the nodes and relations into the database...')
session.execute_write(load_node_author_semantic)
session.execute_write(load_node_conference_semantic)
session.execute_write(load_node_journal_semantic)
session.execute_write(load_node_proceeding_semantic)
session.execute_write(load_node_paper_semantic)
session.execute_write(load_relation_conference_ispart_proceeding)
session.execute_write(load_relation_author_writes_paper)
session.execute_write(load_relation_paper_presentedin_conference)
session.execute_write(load_relation_paper_publishedin_journal)
session.execute_write(load_relation_paper_cites_paper)
session.execute_write(load_relation_author_reviews_paper)
print('Creation and loading done for the database.')

print('Creating the indexes for the nodes and relations in the database...')

session.close()