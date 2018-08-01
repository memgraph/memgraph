# -*- coding: utf-8 -*-


class TestResults:
    """
    Class used to store test results.

    @attribute total:
        int, total number of scenarios.
    @attribute passed:
        int, number of passed scenarios.
    @attribute restarts:
        int, number of restarts of underlying tested system.
    """

    def __init__(self):
        self.total = 0
        self.passed = 0
        self.restarts = 0

    def num_passed(self):
        """
        Getter for param passed.
        """
        return self.passed

    def num_total(self):
        """
        Getter for param total.
        """
        return self.total

    def num_restarts(self):
        """
        Getter for param restarts.
        """
        return self.restarts

    def add_test(self, status, is_tested_system_restarted):
        """
        Method adds one scenario to current results. If
        scenario passed, number of passed scenarios increases.

        @param status:
            string in behave 1.2.5, 'passed' if scenario passed
        """
        if status == "passed":
            self.passed += 1
        self.total += 1
        if is_tested_system_restarted:
            self.restarts += 1
