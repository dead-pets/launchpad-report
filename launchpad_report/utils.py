import logging
import sys


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

untriaged_bp_statuses = [
    'Unknown',
]

untriaged_bp_def_statuses = [
    'New',
]

rejected_bp_def_statuses = ['Superseded', 'Obsolete']

closed_bp_statuses = ['Implemented']

valid_bp_priorities = [
    'Essential', 'High', 'Medium', 'Low'
]

valid_bug_priorities = [
    'Critical', 'High', 'Medium', 'Low', 'Wishlist'
]


logger = logging.getLogger(__name__)

cached_names = {
}


def is_bug(obj):
    return (
        obj.resource_type_link == u'https://api.launchpad.net/devel/#bug_task'
    )


def is_bp(obj):
    return (
        obj.resource_type_link ==
        u'https://api.launchpad.net/devel/#specification'
    )


def is_project(obj):
    return (
        obj.resource_type_link ==
        u'https://api.launchpad.net/devel/#project'
    )


def is_series(obj):
    return (
        obj.resource_type_link ==
        u'https://api.launchpad.net/devel/#project_series'
    )


def short_status(obj):
    if is_bp(obj):
        if obj.definition_status in rejected_bp_def_statuses:
            return 'rejected'
        if obj.implementation_status in closed_bp_statuses:
            return 'done'
        if (
            obj.definition_status in untriaged_bp_def_statuses or
            obj.assignee is None or
            obj.priority not in valid_bp_priorities or
            obj.implementation_status in untriaged_bp_statuses
        ):
            return 'untriaged'
        return 'open'
    if is_bug(obj):
        if obj.status in rejected_bug_statuses:
            return 'rejected'
        if obj.status in closed_bug_statuses:
            return 'done'
        if (
            obj.status in untriaged_bug_statuses or
            obj.assignee is None or
            obj.importance not in valid_bug_priorities
        ):
            return 'untriaged'
        return 'open'
    return 'unknown'


def get_milestone_name(obj):
    if obj.milestone is None:
        return "None"
    return obj.milestone.name


def work_items(obj):
    if is_bp(obj):
        tasks = obj.workitems_text.split("\n")[1:]
        tasks = filter(
            lambda x: not x.endswith(": DONE"),
            tasks
        )
        return ', '.join(tasks)

    if is_bug(obj):
        tasks = filter(
            lambda x: x.status in (open_bug_statuses + untriaged_bug_statuses),
            obj.bug.bug_tasks
        )
        return ', '.join(
            map(
                lambda x: ': '.join([get_milestone_name(x), x.status]),
                tasks
            )
        )


def get_name(obj):
    key = obj._wadl_resource._url
    if key not in cached_names:
        logger.debug("Miss name: " + key)
        cached_names[key] = obj.name
    else:
        logger.debug("Hit name: " + key)
    return cached_names[key]


def printn(text):
    sys.stdout.write(text)
    sys.stdout.flush()
