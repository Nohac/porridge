import runpy
import unittest
from pathlib import Path

tool = runpy.run_path(str(Path(__file__).parents[1] / "measure-allocations"))


class AllocationBudgets(unittest.TestCase):
    def test_each_whole_iteration_not_normalized_average(self):
        samples = [dict(round=0, operations=1000, wall_ns=100)]
        values = [dict(round=0, operation=key, engine=value, kernel=0)
                  for key, value in [("alloc_calls", 3), ("realloc_calls", 2)]]
        record = dict(case="read", workers=1, slots=1000, counting=True, label="after",
                      iterations=tool["iteration_totals"](samples, values))
        self.assertEqual(record["iterations"][0]["engine"]["allocation_events"], 5)
        rules = [dict(case="read", workers=1, slots=1000,
                      maximum_per_iteration=dict(allocation_events=4))]
        self.assertEqual(len(tool["check_budget"](record, rules)), 1)
        rules[0]["maximum_per_iteration"]["allocation_events"] = 5
        self.assertFalse(tool["check_budget"](record, rules))
        self.assertTrue(tool["check_budget"](record, []))
        record["label"] = "before"
        self.assertFalse(tool["check_budget"](record, rules))

    def test_missing_metric_cannot_pass(self):
        record = dict(case="idle", workers=8, slots=1, counting=True, label="after",
                      iterations=[dict(round=0, engine={})])
        rule = dict(case="idle", workers=8, slots=1,
                    maximum_per_iteration=dict(allocation_events=0))
        self.assertTrue(tool["check_budget"](record, [rule]))


if __name__ == "__main__":
    unittest.main()
