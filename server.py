import datetime
import falcon
import json
import psycopg2cffi.pool
import dj_database_url

database_config = dj_database_url.config()

pool = psycopg2cffi.pool.ThreadedConnectionPool(1, 10,
                                                host=database_config['HOST'],
                                                user=database_config['USER'],
                                                port=5432,
                                                password=database_config['PASSWORD'],
                                                database=database_config['NAME'])

class JSONTranslator(object):

    def process_request(self, req, resp):
        # req.stream corresponds to the WSGI wsgi.input environ variable,
        # and allows you to read bytes from the request body.
        #
        # See also: PEP 3333
        if req.content_length in (None, 0):
            # Nothing to do
            return

        try:
            req.context['doc'] = json.load(req.stream)

        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect or not encoded as '
                                   'UTF-8.')

    def process_response(self, req, resp, resource):
        if 'result' not in req.context:
            return

        resp.data = json.dumps(req.context['result'])

class MatchResource:
    def on_post(self, req, resp):
        doc = req.context['doc']
        today = datetime.date.today()
        with pool.getconn() as conn:
            conn.execute('WITH vars as (select ? as match)'
                         'INSERT INTO matches (date, success, failure) '
                         'VALUES (?, vars.match = true, vars.match = false) '
                         'ON CONFLICT (date) DO UPDATE SET '
                         'success = success + (vars.match = true)::INTEGER, '
                         'failure = failure + (vars.match = false)::INTEGER',
                         [today, doc, doc, doc, doc])


app = falcon.API(middleware=[
    JSONTranslator(),
])

match = MatchResource()

app.add_route('/match/', match)
