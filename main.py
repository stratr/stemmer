from nltk.stem.snowball import SnowballStemmer
from google.cloud import bigquery
import base64

stemmer = SnowballStemmer("finnish")
client = bigquery.Client()
table_id = "tanelis.tweets_stemmed.stem_words"

# gcloud functions deploy stemmer --runtime python37 --trigger-topic stem_words --timeout 180s


def pubsub_stem(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    query = """
    SELECT * FROM `tanelis.tweets_stemmed.words_for_stemming`
    """
    query_job = client.query(query)  # Make an API request.
    table = client.get_table(table_id)

    insert_rows = []
    for row in query_job:
        list_row = list(row)
        stem_words = [stemmer.stem(item) for item in list_row[2]]
        list_row.append(stem_words)

        insert_rows.append(list_row)

    errors = client.insert_rows(table, insert_rows)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print(errors)

    # event data
    if 'data' in event:
        event_data = base64.b64decode(event['data']).decode('utf-8')
    else:
        event_data = 'no data in pub/sub event'
    print('Event data: {}!'.format(event_data))


def test():
    pubsub_stem('', '')


#test()