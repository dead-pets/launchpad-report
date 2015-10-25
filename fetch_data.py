#!/usr/bin/env python

import numpy as np
import pandas as pd
import sys

from launchpadlib.launchpad import Launchpad

lp = Launchpad.login_with('lp-report-bot', 'production', version='devel')
projects = ['fuel', 'mos', 'summary-reports', 'fuel-plugins']

untriaged_bug_statuses = [
    'New',
]

open_bug_statuses = [
    'Incomplete', 'Confirmed', 'Triaged', 'In Progress',
    'Incomplete (with response)', 'Incomplete (without response)',
]

rejected_bug_statuses = [
    'Opinion', 'Invalid', 'Won\'t Fix', 'Expired',
]

closed_bug_statuses = [
    'Fix Committed', 'Fix Released',
] + rejected_bug_statuses

all_bug_statuses = (
    untriaged_bug_statuses + open_bug_statuses + closed_bug_statuses
)

text_fields = [
    'title', 'heat', 'message_count', 'tags', 'private', 'security_related',
    'users_affected_count', 'number_of_duplicates',
    'users_unaffected_count', 'users_affected_count_with_dupes']
person_fields = ['owner']
date_fields = ['date_created', 'date_last_updated']
collection_size_fields = ['activity_collection', 'attachments_collection', 'bug_tasks_collection',
    'bug_watches_collection', 'cves_collection' ]

bt_text_fields = ['importance', 'status', 'is_complete']
bt_person_fields = ['assignee'] #, 'owner']
bt_date_fields = ['date_assigned', 'date_closed', 'date_confirmed', 'date_created', 'date_fix_committed',
    'date_fix_released', 'date_in_progress', 'date_incomplete', 'date_left_closed', 'date_left_new',
    'date_triaged']

df = pd.DataFrame(columns=text_fields + person_fields + date_fields + map(lambda x: x + '_size', collection_size_fields))
ms_df = {}

team_filters = ['fuel', 'mos']

def collect_bug(bug):
    id = bug.id
    if not (id in df.index):
        df.loc[id] = float('nan')
    for f in text_fields:
        df.loc[id][f] = getattr(bug, f)
    for f in date_fields:
        df.loc[id][f] = getattr(bug, f)
    for f in person_fields:
        if getattr(bug, f) is None:
            df.loc[id][f] = None
        else:
            df.loc[id][f] = getattr(bug, f).name
    for f in collection_size_fields:
        df.loc[id][f + '_size'] = len(getattr(bug, f))
    for bt in bug.bug_tasks:
        prj_name = 'unknown_project'
        if bt.target.resource_type_link.endswith('#project_series'):
            prj_name = bt.target.project.name
        if bt.target.resource_type_link.endswith('#project'):
            prj_name = bt.target.name
        if bt.milestone is None:
            ms_name = 'no_milestone'
        else:
            ms_name = bt.milestone.name
        col_prefix = '%s_%s_' % (prj_name, ms_name)
        try:
            dfx = ms_df[col_prefix]
        except KeyError:
            dfx = pd.DataFrame(columns=map(lambda x: col_prefix + x, bt_text_fields + bt_person_fields))
            ms_df[col_prefix] = dfx
        dfx.loc[id] = float('nan')
        for f in bt_text_fields:
            dfx.loc[id][col_prefix + f] = getattr(bt, f)
        for f in bt_person_fields:
            if getattr(bt, f) is None:
                dfx.loc[id][col_prefix + f] = None
            else:
                dfx.loc[id][col_prefix + f] = getattr(bt, f).name




if __name__ == "__main__":
    for prj_name in projects:
        prj = lp.projects[prj_name]

        skip_bugs = False

        # Download bugs
        if not skip_bugs:
            collection = prj.searchTasks(status=all_bug_statuses)
            s = len(collection)
            i = 0
            for bt in collection:
                i += 1
                print "%s: %d/%d %s" % (prj_name, i, s, bt.bug.id)
                collect_bug(bt.bug)

            print "Found %s bugs" % len(collection)

            df = pd.concat([df] + ms_df.values(), axis=1)

            df.to_csv('artifacts/bugs-%s.csv' % sys.argv[1], encoding='utf-8')

        teams_map = {}

        if not skip_bugs:
            cols_with_people = filter(lambda x: x.count('assignee'), df.columns) + ['owner']

            for id in pd.Series(df[cols_with_people].values.ravel()).unique():
                try:
                    if lp.people[id].is_team:
                        if not id in teams_map:
                            teams_map[id] = []
                except:
                    print "E: %s" % id

        for team_filter in team_filters:
            for team in lp.people.findTeam(text=team_filter):
                teams_map[team.name] = []

        for t in teams_map:
            for p in lp.people[t].members:
                try:
                    teams_map[t] += [p.name]
                except:
                    teams_map[t] = [p.name]

        df_teams = pd.DataFrame(columns=teams_map.keys())

        for t in teams_map.keys():
            for p in teams_map[t]:
                try:
                    df_teams.loc[p][t] = t
                except KeyError:
                    df_teams.loc[p] = float('nan')
                    df_teams.loc[p][t] = t

        df_teams.to_csv('artifacts/teams-%s.csv' % sys.argv[1], encoding='utf-8')
