from semanticscholar import SemanticScholar
from faker import Faker
from faker.providers import address, internet, lorem, sbn
from faker_education import SchoolProvider
from random import choice, randint, uniform
import pandas as pd
import numpy as np
import uuid


total_data = 100

sch = SemanticScholar()
big_data_paper = sch.search_paper('big data', limit=total_data)
data_science_paper = sch.search_paper('data science', limit=total_data)
data_engineering_paper = sch.search_paper('data engineering', limit=total_data)
database_paper = sch.search_paper('database', limit=total_data)
graph_theory_paper = sch.search_paper('graph theory', limit=total_data)

dataset_all = [big_data_paper, data_science_paper, data_engineering_paper, database_paper, graph_theory_paper]

for data_paper in dataset_all:
    for x in data_paper:
        print(x)
        break

dataset = []

for data_paper in dataset_all:
    i = 1
    for data in data_paper:
        if i > 500 :
            break
            
        dataset.append(data)
        i += 1

fake = Faker()
fake.add_provider(SchoolProvider)
fake.add_provider(internet)
fake.add_provider(sbn)
fake.add_provider(address)
fake.add_provider(lorem)

authors = {"ID": [], "name": [], "email": [], "field": []}
author_ids = set()

author_writes_papers = {"AUTHOR_ID" : [], "PAPER_ID": [], "corresponding_author": []}
author_papers = {}

for res in dataset:
    corresponding_author = True
    for author in res.authors:
        author_writes_papers["AUTHOR_ID"].append(author.authorId)
        author_writes_papers["PAPER_ID"].append(res.paperId)
        author_writes_papers["corresponding_author"].append(corresponding_author)
        
        corresponding_author = False
        
        p = author_papers.setdefault(author.authorId, set())
        p.add(res.paperId)
        author_papers[author.authorId] = p
        
        if author.authorId in author_ids:
            continue
            
        gender = np.random.choice(["M", "F"], p=[0.5, 0.5])
        first_name = fake.first_name_male() if gender =="M" else fake.first_name_female()
        last_name = fake.last_name()
        
        author_ids.add(author.authorId)
        
        authors["ID"].append(author.authorId)
        authors["name"].append(author.name)
        authors["email"].append(f'{first_name}.{last_name}@{fake.domain_name()}')
        authors["field"].append(fake.school_type())

authors_df = pd.DataFrame.from_dict(authors)
authors_df.to_csv('data/authors_node_semantic.csv')

author_wp_df =  pd.DataFrame.from_dict(author_writes_papers)
author_wp_df.to_csv('data/author_writes_papers.csv')

papers = {"ID": [], "title": [], "abstract": [], "pages": [], "year":[], "link":[], "keyword":[]}
papers_set = set()

for res in dataset:
    papers_set.add(res.paperId)
    papers["ID"].append(res.paperId)
    papers["title"].append(res.title)
    papers["abstract"].append(res.abstract)
    papers["year"].append(res.year)
    
    pages = f'{randint(15,100)}-{randint(101,150)}'
    if res.journal is not None:
        pages = res.journal.pages if res.journal.pages is not None else pages
    papers["pages"].append(pages)
    papers["link"].append(fake.uri())
    
    if res.fieldsOfStudy is None or len(res.fieldsOfStudy) == 0:
        papers["keyword"].append("Technology")
    else:
        for fs in res.fieldsOfStudy:
            papers["keyword"].append(fs)
            break

papers_df = pd.DataFrame.from_dict(papers)
papers_df.to_csv('data/papers_semantic.csv')

author_review_papers = {"AUTHOR_ID" : [], "PAPER_ID": [], "notes": []}

for paper in list(papers_set):
    # Pick 3 reviewers per paper
    for i in range(3):
        reviewed = False
        while not reviewed:
            
            # Make sure author not reviewing the same paper
            author = choice(list(author_papers.keys()))
            if paper in author_papers[author]:
                continue
            
            author_review_papers["AUTHOR_ID"].append(author)
            author_review_papers["PAPER_ID"].append(paper)
            author_review_papers["notes"].append(fake.paragraph(nb_sentences=2))
            reviewed = True

author_rp = pd.DataFrame.from_dict(author_review_papers)
author_rp.to_csv('data/author_review_papers.csv')

paper_cites_paper = {"PAPER_ID" : [], "CITED_PAPER_ID": []}

for cited_paper in list(papers_set):
    
    for i in range(randint(0,50)):
        cited = False
        while not cited:
            
            ### Cannot cite own paper
            paper = choice(list(papers_set))
            if cited_paper == paper:
                continue
            
            paper_cites_paper['PAPER_ID'].append(paper)
            paper_cites_paper['CITED_PAPER_ID'].append(cited_paper)
            cited = True

pcp = pd.DataFrame.from_dict(paper_cites_paper)
pcp.to_csv('data/paper_cites_paper.csv')

journals = {"ID": [], "name": []}
paper_published_in_journal = {"PAPER_ID" : [], "JOURNAL_ID": [], "volume": [], "year": []}

default_journal = {'id': str(uuid.uuid4()), 'name': 'Unknown', 'year': 2000, 'volume': 1}
journal_dict = {}

def get_journal_data(res):
    journal_name = default_journal['name']
    journal_id = default_journal['id']
    volume = default_journal['volume']
    year = default_journal['year']
    
    if (res.publicationVenue.name and res.publicationVenue.name != ''):
        journal_name = res.publicationVenue.name
        journal_id =  res.publicationVenue.id

    elif res.journal and res.journal.name and res.journal.name != '':
        journal_name = res.journal.name
        journal_id = str(uuid.uuid4())
    
    else:
        return journal_name, journal_id, volume, year
    
    if (res.publicationVenue and res.publicationVenue.type == 'journal' and res.year and res.year != ''):
        year = res.year
    if res.journal and res.journal.volume and res.journal.volume != '':
        volume = res.journal.volume

    return journal_name, journal_id, volume, year

for res in dataset:
    if not (res.publicationVenue and res.publicationVenue.type == 'journal'):
        continue
        
    journal_name, journal_id, volume, year = get_journal_data(res)
    
    paper_published_in_journal['PAPER_ID'].append(res.paperId)
    paper_published_in_journal['JOURNAL_ID'].append(journal_id)
    paper_published_in_journal['volume'].append(volume)
    paper_published_in_journal['year'].append(int(year))
    
    
    if journal_name in journal_dict:
        continue
    
    journal_dict.setdefault(journal_name, journal_id)
    journals['ID'].append(journal_id)
    journals['name'].append(journal_name)

jdf = pd.DataFrame.from_dict(journals)
jdf.to_csv('data/journal_semantic.csv')

ppij = pd.DataFrame.from_dict(paper_published_in_journal)
ppij.to_csv('data/paper_published_in_journal.csv')

conference = {"ID": [], "name": [], "city": [], "edition": [], "year":[]}
proceedings = {"ID": [], "name": []}

paper_presented_in_conference = {"PAPER_ID" : [], "CONFERENCE_ID": []}
conference_part_of_proceedings = {"CONFERENCE_ID" : [], "PROCEEDING_ID": []}

default_conference_proceeding = {'name': 'Unknown', 'proc_id': str(uuid.uuid4())}
conference_dict = {}

def get_conference_data(res):
    conference_name = default_conference_proceeding['name']
    conference_id = str(uuid.uuid4())
    
    if (res.publicationVenue.name and res.publicationVenue.name != ''):
        conference_name = res.publicationVenue.name
    
    if (res.publicationVenue.id and res.publicationVenue.id != ''):
        conference_id = res.publicationVenue.id
    
    return conference_name, conference_id

def get_proceeding_data(res):
    proc_name = default_conference_proceeding['name']
    proc_id = str(uuid.uuid4())
    
    if res.journal and res.journal.name and res.journal.name != '':
        proc_name = res.journal.name
    
    return proc_name, proc_id, fake.city()

for res in dataset:
    year = 2000
    
    if (res.publicationVenue and res.publicationVenue.type == 'journal' and res.year and res.year != ''):
        year = res.year
    
    if not (res.publicationVenue and res.publicationVenue.type == 'conference'):
        continue
    
    conference_name, conference_id = get_conference_data(res)
    if conference_id not in conference_dict:
        data = conference_dict.setdefault(conference_id, {})
        data['proceeding'] = {}
        proc_name, proc_id, city = get_proceeding_data(res)
        
        data['proceeding']['name'] = proc_name
        data['proceeding']['id'] = proc_id
        
        data['conference'] = {}
        data['conference']['name'] = conference_name
        data['conference']['id'] = conference_id
        data['conference']['city'] = city
        data['conference']['year'] = year
        
        conference_dict[conference_id] = data
    
        conference['ID'].append(conference_id)
        conference['name'].append(conference_name)
        conference['edition'].append(fake.pyint(min_value=1, max_value=10))
        conference['city'].append(city)
        conference['year'].append(year)
        
        proceedings['ID'].append(proc_id)
        proceedings['name'].append(proc_name)
        
        conference_part_of_proceedings['CONFERENCE_ID'].append(conference_id)
        conference_part_of_proceedings['PROCEEDING_ID'].append(proc_id)
    
    
    paper_presented_in_conference['PAPER_ID'].append(res.paperId)
    paper_presented_in_conference['CONFERENCE_ID'].append(conference_id)
    
cf = pd.DataFrame.from_dict(conference)
cf.to_csv('data/conference_semantic.csv')

pcdgs = pd.DataFrame.from_dict(proceedings)
pcdgs.to_csv('data/proceedings_semantic.csv')

ppic = pd.DataFrame.from_dict(paper_presented_in_conference)
ppic.to_csv('data/paper_presented_in_conference.csv')

cpop = pd.DataFrame.from_dict(conference_part_of_proceedings)
cpop.to_csv('data/conference_part_of_proceedings.csv')

cdf = pd.read_csv('data/conference_semantic.csv', index_col=False)
ppij = pd.read_csv('data/paper_published_in_journal.csv', index_col=False)
ppic = pd.read_csv('data/paper_presented_in_conference.csv', index_col=False)
papers = pd.read_csv('data/papers_semantic.csv', index_col=False)
awp = pd.read_csv('data/author_writes_papers.csv')

published_papers = pd.concat([ppij["PAPER_ID"], ppic["PAPER_ID"]], ignore_index=True).unique()
paper_set = set(papers["ID"])

unpublished_papers = paper_set.difference(set(published_papers))
unpublished_papers

conference_ids = ["7654260e-79f9-45c5-9663-d72027cf88f3",
"376732f4-ec63-4b76-bfc8-bbf77119d852",
"25eaf793-6674-4a6d-864f-6c8ae5428912",
"1123f25d-add0-4c9c-8f43-c877aab90a0b",
"b1ee6f13-7776-44aa-a2d5-b79deda2aecb",
"b83b14d5-4e97-4f22-85e2-0b30dfa042f4",
"c40f4908-60d1-42b4-8890-380119178833",
"5afb995f-87ba-455e-bd26-86ae67a10447",
"c2ff5df6-f2f4-4573-a884-8c53979d4c78",
"5042fe05-b1f6-41b6-8092-53294b52cbd6",
"bb718fdd-6d66-4f93-851b-08eeeefb28f5",
"0efa120a-36c7-45fa-b534-597651ae69d2",
"019d3f59-a115-42e3-bd7b-474dd4246499",
"bedd754b-5faf-4eff-8074-3c90be8ac9b0",
"0256ebd3-4f16-4fd0-91bc-b0e77fcd3c0d",
"f3dd946e-cb75-4502-b550-9dec04bda7f9",
"3ff00d27-28c7-4770-a1c7-855a072843fd",
"4c562775-121a-4c25-9f7a-823f54d0e93e",
"10ff739d-ef5f-48b7-9454-9cd1c6d2434d",
"3837ff2b-82e5-4165-8900-b069c31ef3d7",
"b55b50b1-aae7-47a7-b042-8aecc930073d",
"c85dfc25-bcef-4719-9997-f41ad334d998",
"d7907408-25bc-4816-a81d-4e0f2f6482c8"
]

journals = ["c6840156-ee10-4d78-8832-7f8909811576",
"d60da343-ab92-4310-b3d7-2c0860287a9d",
"27475f31-a1d2-401b-84ad-9b405c7609a8",
"961301b0-6f5a-44a6-9216-54b673cded78",
"bc30f894-9a9c-440e-8420-7bd3c5624384"]

unpublished_papers = list(unpublished_papers)
for_journals = unpublished_papers[len(unpublished_papers)//2+1:]
for_conferences = unpublished_papers[:len(unpublished_papers)//2+1]


paper_presented_in_conference = {"PAPER_ID" : [], "CONFERENCE_ID": []}
paper_published_in_journal = {"PAPER_ID" : [], "JOURNAL_ID": [], "volume": [], "year": []}

index = 0
for paper in for_conferences:
    paper_presented_in_conference["PAPER_ID"].append(paper)
    paper_presented_in_conference["CONFERENCE_ID"].append(conference_ids[index%len(conference_ids)])
    index += 1

avail_paper = len(for_journals)
while avail_paper > 0:
    for journal in journals:
        if avail_paper <= 0:
            break

        year = 2017
        volume = 1
        
        for i in range(5):
            if avail_paper <= 0:
                break
                
            paper_published_in_journal["PAPER_ID"].append(for_journals[avail_paper-1])
            paper_published_in_journal["JOURNAL_ID"].append(journal)
            paper_published_in_journal["volume"].append(volume)
            paper_published_in_journal["year"].append(year)
            
            year += 1
            volume += 1
            avail_paper -= 1

author_writes_papers = {"AUTHOR_ID" : [], "CONFERENCE_ID": [], "corresponding_author": []}
authors = ["144110054",
"147069795",
"4376295",
"2395456",
"8504175",
"145073266"

]

avail_paper = len(for_conferences)
while avail_paper > 0:
    for author in authors:
        if avail_paper <= 0:
            break

        
        for i in range(5):
            if avail_paper <= 0:
                break
                
            author_writes_papers["AUTHOR_ID"].append(author)
            author_writes_papers["CONFERENCE_ID"].append(for_conferences[avail_paper-1])
            author_writes_papers["corresponding_author"].append(False)
            
            avail_paper -= 1

ppij = pd.concat([ppij, pd.DataFrame.from_dict(paper_published_in_journal)], ignore_index=True)
ppic = pd.concat([ppic, pd.DataFrame.from_dict(paper_presented_in_conference)], ignore_index=True)

ppij.to_csv("data/paper_published_in_journal.csv", index=False)
ppic.to_csv("data/paper_presented_in_conference.csv", index=False)

