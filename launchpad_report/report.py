from __future__ import print_function

import json

from launchpadlib.launchpad import Launchpad
import yaml

from launchpad_report.checks import Checks
from launchpad_report.render import CSVRenderer
from launchpad_report.render import HTMLRenderer
from launchpad_report.render import JSONRenderer
from launchpad_report.utils import all_bug_statuses
from launchpad_report.utils import get_name
from launchpad_report.utils import is_series
from launchpad_report.utils import open_bug_statuses
from launchpad_report.utils import printn
from launchpad_report.utils import short_status
from launchpad_report.utils import untriaged_bug_statuses
from launchpad_report.utils import work_items


class ConfigError(Exception):
    pass


class Report(object):
    def __init__(self, config_filename):
        with open(config_filename, "r") as f:
            self.config = yaml.load(f.read())

        self.teams = self.config['teams']
        self.trunc = self.config['trunc_report']

        cache_dir = self.config['cache_dir']

        if self.config['use_auth']:
            lp = Launchpad.login_with(
                'lp-report-bot', 'production',
                cache_dir, version='devel'
            )
        else:
            lp = Launchpad.login_anonymously(
                'lp-report-bot', 'production', version='devel'
            )
        self.project = lp.projects[self.config['project']]
        self.blueprint_series = {}

    def render2html(self, filename, template_filename):
        HTMLRenderer(filename, template_filename).render(self.data)

    def render2csv(self, filename):
        CSVRenderer(filename).render(self.data)

    def render2json(self, filename):
        JSONRenderer(filename).render(self.data)

    def load(self, filename):
        jsonfile = open(filename)
        self.data = json.load(jsonfile)

    def generate(self, all=False):
        if self.project is None:
            raise ConfigError("No such project '%s'" % self.config['project'])
        self.checks = Checks(self.iter_series())
        self.data = {'rows': []}
        self.data['config'] = self.config
        self.bug_issues = {}
        self.data['rows'] += self.bp_report(all=all)
        self.data['rows'] += self.bug_report(all=all)

    def iter_series(self):
        print("Collecting series data:")
        self.bps_series = {}
        self.milestones_series = {}
        for series in self.project.series:
            printn(" %s" % get_name(series))
            # Blueprints
            for (counter, bp) in enumerate(series.all_specifications):
                self.bps_series[get_name(bp)] = get_name(series)
            # Milestones
            for milestone in series.all_milestones:
                self.milestones_series[get_name(milestone)] = get_name(series)
        printn(" none")
        # Search for blueprints without series
        for (counter, bp) in enumerate(self.project.all_specifications):
            self.bps_series.setdefault(get_name(bp), None)
        print()
        return {
            'milestones': self.milestones_series,
        }

    def bp_report(self, all=False):
        report = []
        if all:
            blueprints = self.project.all_specifications
        else:
            blueprints = self.project.valid_specifications
        printn("Processing blueprints (%d):" % len(blueprints))
        for (counter, bp) in enumerate(blueprints, 1):
            if counter > self.trunc and self.trunc > 0:
                break
            if counter % 200 == 10:
                print()
            if counter % 10 == 0:
                printn("%4d" % counter)
            assignee = 'unassigned'
            assignee_name = 'unassigned'
            try:
                assignee = get_name(bp.assignee)
                assignee_name = bp.assignee.display_name
            except Exception:
                pass
            if bp.milestone:
                milestone = get_name(bp.milestone)
            else:
                milestone = 'None'
            team = 'unknown'
            for t in self.teams.keys():
                if assignee in self.teams[t]:
                    team = t
            triage = self.checks.run(bp, self.bps_series[get_name(bp)])
            sh_status = short_status(bp)
            w_items = work_items(bp)
            if sh_status in ['done', 'rejected'] and w_items != '':
                sh_status = 'unfinished'
            report.append({
                'type': 'bp',
                'link': bp.web_link.encode('utf-8'),
                'id': bp.web_link[
                    bp.web_link.rfind('/') + 1:
                ].encode('utf-8'),
                'title': bp.title.encode('utf-8'),
                'date_created': str(bp.date_created),
                'date_updated': '',
                'milestone': milestone,
                'series': self.bps_series[get_name(bp)],
                'status': bp.implementation_status,
                'short_status': sh_status,
                'priority': bp.priority,
                'team': team.encode('utf-8'),
                'assignee': assignee.encode('utf-8'),
                'name': assignee_name.encode('utf-8'),
                'work_items': w_items.encode('utf-8'),
                'tags': '',
                'triage': ', '.join(triage).encode('utf-8')
            })
        print()
        return report

    def bug_report(self, all=False):
        report = []
        if all:
            bugs = self.project.searchTasks(status=all_bug_statuses)
        else:
            bugs = self.project.searchTasks(status=(
                untriaged_bug_statuses + open_bug_statuses))
        printn("Processing bugs (%d):" % len(bugs))

        for (counter, bug) in enumerate(bugs, 1):
            if counter > self.trunc and self.trunc > 0:
                break
            if counter % 200 == 10:
                print()
            if counter % 10 == 0:
                printn("%4d" % counter)
            assignee = 'unassigned'
            assignee_name = 'unassigned'
            try:
                assignee = get_name(bug.assignee)
                assignee_name = bug.assignee.display_name
            except Exception:
                pass
            if bug.milestone:
                milestone = get_name(bug.milestone)
            else:
                milestone = 'None'
            team = 'unknown'
            self.bug_issues.setdefault(bug.bug.web_link, [])
            for t in self.teams.keys():
                if assignee in self.teams[t]:
                    team = t
            title = bug.bug.title
            triage = []
            for task in bug.bug.bug_tasks:
                series = task.target
                if is_series(series):
                    series = get_name(series)
                    if task.target.project != self.project:
                        continue
                else:
                    series = None
                    if task.target != self.project:
                        continue
                triage += self.checks.run(task, series)
            sh_status = short_status(bug)
            w_items = work_items(bug)
            if sh_status in ['done', 'rejected'] and w_items != '':
                sh_status = 'unfinished'
            report.append({
                'type': 'bug',
                'link': bug.web_link.encode('utf-8'),
                'id': bug.web_link[
                    bug.web_link.rfind('/') + 1:
                ].encode('utf-8'),
                'title': title.encode('utf-8'),
                'date_created': str(bug.bug.date_created),
                'date_updated': str(bug.bug.date_last_updated),
                'milestone': milestone,
                'status': bug.status,
                'short_status': sh_status,
                'priority': bug.importance,
                'team': team.encode('utf-8'),
                'assignee': assignee.encode('utf-8'),
                'name': assignee_name.encode('utf-8'),
                'work_items': w_items.encode('utf-8'),
                'tags': ' '.join(bug.bug.tags),
                'triage': ', '.join(triage).encode('utf-8'),
            })
        print()
        return report
