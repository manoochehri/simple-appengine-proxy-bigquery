# Copyright 2012 Google Inc. All Rights Reserved.

"""Handlers for Starting and Polling query jobs.
"""

__author__ = 'manoochehri@google.com (Michael Manoochehri)'

import httplib2

from apiclient.discovery import build
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials

# BigQuery API Settings
SCOPE = 'https://www.googleapis.com/auth/bigquery'

PROJECT_ID = 'XXXXXXXXXX' # REPLACE WITH YOUR Project ID

# Create a new API service for interacting with BigQuery
credentials = AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
bigquery_service = build('bigquery', 'v2', http=http)


class StartQueryHandler(webapp.RequestHandler):
  def post(self):
    query_string = self.request.get('query')
    jobCollection = bigquery_service.jobs()
    jobData = {
      'configuration': {
        'query': {
          'query': query_string,
        }
      }
    }
    try:
      insertResponse = jobCollection.insert(projectId=PROJECT_ID,
                                            body=jobData).execute()
      self.response.headers.add_header('content-type',
                                       'application/json',
                                       charset='utf-8')
      self.response.out.write(insertResponse)
    except:
      self.response.out.write('Error connecting to the BigQuery API')

class CheckQueryHandler(webapp.RequestHandler):
  def get(self, job_id):
    query_job = bigquery_service.jobs()
    try:
      queryReply = query_job.getQueryResults(projectId=PROJECT_ID,
                                             jobId=job_id).execute()
      self.response.headers.add_header('content-type',
                                       'application/json',
                                       charset='utf-8')
      self.response.out.write(queryReply)
    except:
      self.response.out.write('Error connecting to the BigQuery API')

application = webapp.WSGIApplication(
                                     [('/startquery(.*)', StartQueryHandler),
                                     ('/checkquery/(.*)', CheckQueryHandler)],
                                     debug=True)

def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
