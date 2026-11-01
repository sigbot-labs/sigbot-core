"""Python value objects for the versioned SDK; host commits state and Effects together."""

import json
import sys
from collections.abc import Mapping
from types import MappingProxyType


class ReadOnly(Mapping):
    __slots__ = ("_values",)

    def __init__(self, values):
        object.__setattr__(self, "_values", MappingProxyType({
            key: self.freeze(value) for key, value in values.items()
        }))

    @classmethod
    def freeze(cls, value):
        if isinstance(value, dict):
            return cls(value)
        if isinstance(value, list):
            return tuple(cls.freeze(item) for item in value)
        return value

    def __getitem__(self, key):
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __getattr__(self, key):
        try:
            return self._values[key]
        except KeyError as error:
            raise AttributeError(key) from error

    def __setattr__(self, key, value):
        raise TypeError("strategy input is read-only; persist changes in ctx.state")


class Effects:
    __slots__ = ("intents", "requests", "timers")

    def __init__(self, intents=(), requests=(), timers=()):
        self.intents = list(intents)
        self.requests = list(requests)
        self.timers = list(timers)

    def to_dict(self):
        return {"intents": self.intents, "requests": self.requests, "timers": self.timers}


class Market:
    __slots__ = ("_requests",)

    def __init__(self, requests):
        self._requests = requests

    def snapshot(self, query):
        if len(self._requests) >= 64:
            raise ValueError("strategy request limit reached")
        self._requests.append({"kind": "snapshot", "query": query})


class Compute:
    __slots__ = ("_requests",)

    def __init__(self, requests):
        self._requests = requests

    def submit(self, plan):
        if len(self._requests) >= 64:
            raise ValueError("strategy request limit reached")
        self._requests.append({"kind": "compute", "query": plan})


class Context:
    __slots__ = ("_input", "_requests", "state", "market", "compute")

    def __init__(self, serialized):
        values = json.loads(serialized)
        requests = []
        object.__setattr__(self, "state", values.pop("state"))
        object.__setattr__(self, "_input", ReadOnly(values))
        object.__setattr__(self, "_requests", requests)
        object.__setattr__(self, "market", Market(requests))
        object.__setattr__(self, "compute", Compute(requests))

    def __getattr__(self, key):
        return getattr(self._input, key)

    def __setattr__(self, key, value):
        raise TypeError("ctx fields are read-only; mutate the state draft in place")

    def finish(self, effects):
        if not isinstance(effects, Effects):
            raise TypeError("v2 callbacks must return Effects")
        result = effects.to_dict()
        result["requests"].extend(self._requests)
        return json.dumps({"state": self.state, "effects": result}, allow_nan=False)


class Input:
    @staticmethod
    def from_json(serialized):
        return ReadOnly.freeze(json.loads(serialized))


class BoundedOutput:
    __slots__ = ("value",)

    def __init__(self):
        self.value = ""

    def write(self, value):
        self.value += str(value)[: max(0, 65536 - len(self.value))]
        return len(value)

    def flush(self):
        pass


class OutputCapture:
    def __init__(self):
        self.stdout = BoundedOutput()
        self.stderr = BoundedOutput()

    def install(self):
        sys.stdout = self.stdout
        sys.stderr = self.stderr

    def finish(self, result):
        value = json.loads(result)
        value["stdout"] = self.stdout.value
        value["stderr"] = self.stderr.value
        return json.dumps(value, allow_nan=False)

    def fail(self, error):
        return json.dumps({"error": error, "stdout": self.stdout.value, "stderr": self.stderr.value}, allow_nan=False)
