"""Mark hot articles which are extensively cited"""

import scholar


def overlap(s1, s2):
    s1 = replace(s1)
    s2 = replace(s2)
    s1 = set(s1.split())
    s2 = set(s2.split())
    intersec = s1 & s2
    return len(intersec)/len(s1)

def replace(s0):
    s0 = s0.replace('.', ' ')
    s0 = s0.replace(':', ' ')
    s0 = s0.lower()
    return s0

def get_citations(paper, verbose=1):
    def searchScholar(searchphrase, title):
        query = scholar.SearchScholarQuery()
        # query.set_words(searchphrase)
        query.set_words(title)
        querier.send_query(query)
        articles = querier.articles
        try:
            if overlap(articles[0].attrs['title'][0], title) < 0.9:
                return 0
        except:
            # set_new_proxy()
            return -1
        return articles[0].attrs['num_citations'][0]

    art = ["-c", "1", "--phrase", paper]
    querier = scholar.ScholarQuerier()
    settings = scholar.ScholarSettings()
    settings.set_citation_format(2)
    querier.apply_settings(settings)
    cites = searchScholar(art, paper)
    while cites == -1:
        searchScholar(art, paper)
    if verbose:
        print(f'{paper}: ', cites)
    return cites

test = get_citations('The Google File System')
print(test)