class TestResults:
    """
    Clas used to store test results. It has parameters total 
    and passed.

    @attribute total:
        int, total number of scenarios.
    @attribute passed:
        int, number of passed scenarios.
    """
    def __init__(self):
        self.total = 0
        self.passed = 0

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

    def add_test(self, status):
        """
        Method adds one scenario to current results. If 
        scenario passed, number of passed scenarios increases.

        @param status:
            string in behave 1.2.5, 'passed' if scenario passed
        """
        if status == "passed":
            self.passed += 1
        self.total += 1
