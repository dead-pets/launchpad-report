#!/usr/bin/env python

import numpy as np
import pandas as pd
import sys

from launchpadlib.launchpad import Launchpad

lp = Launchpad.login_with('lp-report-bot', 'production', version='devel')
prj = lp.projects['fuel']

dev_focus = prj.development_focus
cur_ms = prj.getMilestone(name='8.0')

open_statuses = ['New', 'Confirmed', 'Triaged', 'In Progress']

area_tags = ['library', 'ui', 'fuel-python', 'system-tests', 'docs', 'devops', 'fuel-ci', 'fuel-build']

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

def collect_bug(bug):
    id = bug.id
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
        if bt.milestone is None:
            ms = 'Untargeted_'
        else:
            ms = bt.milestone.name + '_'
        try:
            dfx = ms_df[ms]
        except KeyError:
            dfx = pd.DataFrame(columns=map(lambda x: ms + x, bt_text_fields + bt_person_fields))
            ms_df[ms] = dfx
        dfx.loc[id] = float('nan')
        for f in bt_text_fields:
            dfx.loc[id][ms + f] = getattr(bt, f)
        for f in bt_person_fields:
            if getattr(bt, f) is None:
                dfx.loc[id][ms + f] = None
            else:
                dfx.loc[id][ms + f] = getattr(bt, f).name

# Download all open bugs
collection = prj.searchTasks(milestone=cur_ms, status=open_statuses)

df = pd.DataFrame(columns=text_fields + person_fields + date_fields + map(lambda x: x + '_size', collection_size_fields))
ms_df = {}

s = len(collection)
i = 0
for bt in collection:
    i += 1
    print "%d/%d %s" % (i, s, bt.bug.id)
    collect_bug(bt.bug)

df = pd.concat([df] + ms_df.values(), axis=1)

print "Found %s bugs" % len(collection)


#df[['owner', 'title', '8.0_status', 'tags', '8.0_importance', '8.0_assignee']]

# teams taken from here: https://review.fuel-infra.org/gitweb?p=tools/lp-reports.git;a=blob_plain;f=config/teams.yaml;hb=refs/heads/master
teams = """
mos-ceilometer
mos-cinder
mos-glance
mos-heat
mos-horizon
mos-ironic
mos-keystone
mos-kernel-virt
mos-kernel-networking
mos-kernel-storage
mos-murano
mos-neutron
mos-nova
mos-oslo
mos-packaging
mos-puppet
mos-sahara
mos-scale
mos-swift
fuel-python
fuel-ui
fuel-library
mos-maintenance
mos-linux
mos-ceph
fuel-plugin-calico
fuel-plugin-cisco-aci
fuel-plugin-external-glusterfs
fuel-plugin-cinder-netapp
fuel-plugin-zabbix
fuel-plugins-bugs
fuel-plugins-docs
mos-lma-toolchain
fuel-partner-engineering
fuel-plugin-contrail
fuel-plugin-vmware-dvs
fuel-docs
fuel-build
fuel-ci
fuel-devops
fuel-qa
mos-qa
mos-security
fuel-security
""".split()

cols_with_people = filter(lambda x: x.count('assignee'), df.columns) + ['owner']
for id in pd.Series(df[cols_with_people].values.ravel()).unique():
    try:
        if lp.people[id].is_team:
            if not id in teams:
                teams += [id]
                print id
    except:
        print "E: %s" % id

teams_map = {}
for t in teams:
    for p in lp.people[t].members:
        try:
            teams_map[t] += [p.name]
        except KeyError:
            teams_map[t] = [p.name]

df_teams = pd.DataFrame(columns=teams_map.keys())

for t in teams_map.keys():
    for p in teams_map[t]:
        try:
            df_teams.loc[p][t] = t
        except KeyError:
            df_teams.loc[p] = float('nan')
            df_teams.loc[p][t] = t

df.to_csv('artifacts/open_fuel_8_0-%s.csv' % sys.argv[1], encoding='utf-8')
df_teams.to_csv('artifacts/teams_fuel-%s.csv' % sys.argv[1], encoding='utf-8')
