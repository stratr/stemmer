from nltk.stem.snowball import SnowballStemmer
from google.cloud import bigquery

stemmer = SnowballStemmer("finnish")
client = bigquery.Client()
table_id = "tanelis.tweets_stemmed.stem_words"

query = """
    SELECT * FROM `tanelis.tweets_stemmed.words_for_stemming`
"""
query_job = client.query(query)  # Make an API request.

insert_rows = []
for row in query_job:
    list_row = list(row)
    stem_words = [stemmer.stem(item) for item in list_row[2]]
    list_row.append(stem_words)

    insert_rows.append(list_row)

#https://cloud.google.com/bigquery/streaming-data-into-bigquery


table = client.get_table(table_id)

errors = client.insert_rows(table, insert_rows)  # Make an API request.
if errors == []:
    print("New rows have been added.")
else:
    print(errors)