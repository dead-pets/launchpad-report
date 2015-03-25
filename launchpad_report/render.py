import json
import os

from io import BytesIO
from jinja2 import Environment
from jinja2 import FileSystemLoader


import codecs
import cStringIO
import csv


class UnicodeWriter:
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class Renderer(object):
    def __init__(self, filename):
        self.filename = filename

    def render(self, data):
        if self.filename == '-':
            print(self._render(data))
        else:
            rep_file = open(self.filename, 'wb')
            rep_file.write(self._render(data))


class CSVRenderer(Renderer):
    def _render(self, data):
        csvfile = BytesIO()
        reporter = UnicodeWriter(csvfile)
        reporter.writerow([
            '', 'Link', 'Title', 'Created', 'Updated', 'Milestone',
            'Short status', 'Status', 'Priority', 'Tags', 'Team', 'Nick',
            'Name', 'Work items', 'Triage actions'
        ])
        for row in data['rows']:
            reporter.writerow([
                row['type'], row['link'], row['title'], row['date_created'],
                row['date_updated'], row['milestone'], row['short_status'],
                row['status'], row['priority'], row['tags'], row['team'],
                row['assignee'], row['name'], row['work_items'], row['triage']
            ])
        return csvfile.getvalue()


class JSONRenderer(Renderer):
    def _render(self, data):
        return json.dumps(data)


class HTMLRenderer(Renderer):
    def __init__(self, filename, template_filename):
        self.filename = filename
        self.template_filename = template_filename

    def _render(self, data):
        env = Environment(
            loader=FileSystemLoader(
                os.path.dirname(os.path.abspath(self.template_filename))
            )
        )
        template = env.get_template(
            os.path.basename(os.path.abspath(self.template_filename))
        )
        return template.render(data)
