import inspect

from launchpad_report.utils import get_name
from launchpad_report.utils import is_bp
from launchpad_report.utils import is_bug
from launchpad_report.utils import is_project


class Checks(object):
    def __init__(self, mapping):
        self.mapping = mapping

    def run(self, obj, series):
        tests = inspect.getmembers(
            Checks,
            lambda x: inspect.ismethod(x) and
            x.__name__.startswith('is_')
        )
        actions = []
        for test in tests:
            actions.append(getattr(Checks, test[0])(self, obj, series))
        return filter(lambda x: x is not None, actions)

    def is_series_defined(self, obj, series):
        if is_bp(obj) and series is None:
            return "No series"

    def is_milestone_in_series(self, obj, series):
        if obj.milestone is None:
            return "No milestone for series (%s)" % series
        if series is None:
            return  # There is another check for missed series
        if (
            get_name(obj.milestone) in self.mapping['milestones'] and
            self.mapping['milestones'][get_name(obj.milestone)] != series
        ):
            return ("Wrong milestone (%s) for series (%s)" % (
                get_name(obj.milestone), series))

    def is_milestone_active(self, obj, series):
        if obj.milestone is None:
            return
        if not obj.milestone.is_active and not obj.is_complete:
            return (
                "Open and targeted to closed milestone (%s) on series (%s)" %
                (get_name(obj.milestone), series)
            )

    def is_bug_targeted_to_focus_series(self, obj, series):
        if (
            is_bug(obj) and
            is_project(obj.target) and
            get_name(obj.target.development_focus) == series
        ):
            return (
                "Targeted to the current development focus (%s)" %
                series)

    def is_priority_set(self, obj, series):
        valid_bp_priorities = [
            'Essential', 'High', 'Medium', 'Low'
        ]
        valid_bug_priorities = [
            'Critical', 'High', 'Medium', 'Low', 'Wishlist'
        ]
        if is_bp(obj) and obj.priority not in valid_bp_priorities:
            return "Priority (%s) is not valid for series (%s)" % (
                obj.priority, series)
        if is_bug(obj) and obj.importance not in valid_bug_priorities:
            return "Priority (%s) is not valid for series (%s)" % (
                obj.importance, series)

    def is_assignee_set(self, obj, series):
        if not obj.assignee:
            return "No assignee for series (%s)" % series

    def is_bug_confirmed(self, obj, series):
        if is_bug(obj) and obj.status == 'New':
            return "Not confirmed for series (%s)" % series
