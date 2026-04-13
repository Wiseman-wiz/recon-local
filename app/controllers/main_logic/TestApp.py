from helper_v2.Performance import timer_decorator,timer_decorator_get_data
import json
import pathlib
from typing import Callable, Dict, List, Tuple, Type, Union
from Crumble.Crumb import Crumb
import pickle

PATH_TO_FILE = pathlib.Path(__file__).parent.resolve()
CONFIG_FILE = "main_app.json"


class TestApp:
    def __init__(self) -> None:
        self.config: dict = {}
        with open(f"{PATH_TO_FILE}/{CONFIG_FILE}", "r") as f:
            json_data: str = f.read()
            print(json_data)
            self.config: dict = json.loads(json_data).get("config")

    @timer_decorator_get_data(PATH_TO_FILE)
    def test_crumble(self) -> None:
        class_: Callable = Crumb()
        results: Dict[str, str] = {}
        try:
            class_()
            pass
        except Exception as e:
            results["crumb"] = "pass"
            pass
        return results

    def test_helper(self) -> None:
        from helper_v2 import Performance

        class_ = Performance.timer_decorator()

    def test_cases(self) -> None:
        results = {}
        exclude_test = []
        test_cases = {1: self.test_crumble}

        for test_id, test in test_cases.items():
            results[test_id] = test
        

if __name__ == "__main__":
    ma = TestApp()
    ma.test_cases()
